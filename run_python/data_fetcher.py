# data_fetcher.py - Updated with comprehensive symbol search
import asyncio
import aiohttp
import re
import os
import time
import pandas as pd
from polygon import RESTClient
from config import (
    POLYGON_API_KEY, S3_BUCKET, MAX_CONCURRENT_REQUESTS, 
    REQUEST_TIMEOUT, BATCH_SIZE, MAX_RETRIES, RETRY_DELAY, BATCH_DELAY
)
from utils import update_stats, download_from_s3
import logging

logger = logging.getLogger(__name__)

class DataFetcher:
    """Handles all API calls and data fetching operations"""
    
    def __init__(self):
        self.api_key = POLYGON_API_KEY
        self.s3_bucket = S3_BUCKET
    
    async def fetch_with_retry(self, session, url, headers, symbol, max_retries=MAX_RETRIES):
        """Async fetch with retry logic"""
        for attempt in range(max_retries):
            try:
                async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == 'OK':
                            return data
                    elif response.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    elif response.status >= 500:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    return None
            except (asyncio.TimeoutError, Exception):
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        return None
    
    async def get_stock_data(self, session, symbol, stats, period_info):
        """Get stock data for specified period"""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        
        # Choose URL based on data period
        if period_info['data_period'] == "previous":
            data_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
        else:  # today
            today_date = period_info['date_str']
            data_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{today_date}/{today_date}"
        
        company_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
        
        # Fetch data and company info
        data_task = self.fetch_with_retry(session, data_url, headers, symbol)
        company_task = self.fetch_with_retry(session, company_url, headers, symbol)
        
        price_data, company_data = await asyncio.gather(data_task, company_task, return_exceptions=True)
        update_stats(stats, api_calls=2)
        
        return self._process_stock_response(symbol, price_data, company_data, stats, period_info)
    
    def _process_stock_response(self, symbol, price_data, company_data, stats, period_info):
        """Process API response into stock data structure"""
        stock_data = {'symbol': symbol, 'data_period': period_info['data_period']}
        
        # Process price data
        if isinstance(price_data, dict) and price_data.get('results') and len(price_data['results']) > 0:
            result = price_data['results'][0]
            
            open_price = result.get('o')
            high_price = result.get('h')
            low_price = result.get('l')
            close_price = result.get('c')
            volume = result.get('v')
            
            if all(x is not None and x > 0 for x in [open_price, high_price, low_price, close_price, volume]):
                stock_data.update({
                    'previous_open': open_price,
                    'previous_high': high_price,
                    'previous_low': low_price,
                    'previous_close': close_price,
                    'previous_volume': volume,
                    'previous_current_price': close_price,
                    'previous_avg_volume': volume,
                })
                
                # Calculate previous day percentage change
                if open_price > 0:
                    pct_change = ((close_price - open_price) / open_price) * 100
                    stock_data['previous_price_pct_change_from_open'] = pct_change
                else:
                    stock_data['previous_price_pct_change_from_open'] = 0.0
                    
            else:
                update_stats(stats, incomplete_records=1)
                return None
        else:
            update_stats(stats, incomplete_records=1)
            return None
        
        # Process company data
        self._add_company_data(stock_data, company_data)
        
        update_stats(stats, complete_records=1)
        return stock_data
    
    def _add_company_data(self, stock_data, company_data):
        """Add company information to stock data"""
        company_name = 'N/A'
        market_cap = None
        shares_outstanding = None
        
        if isinstance(company_data, dict) and company_data.get('results'):
            result = company_data['results']
            company_name = result.get('name', 'N/A')
            market_cap = result.get('market_cap')
            
            shares_outstanding = (
                result.get('share_class_shares_outstanding') or
                result.get('weighted_shares_outstanding') or
                result.get('shares_outstanding')
            )
        
        # Calculate market cap
        current_price = stock_data.get('previous_close')
        calculated_market_cap = None
        intraday_market_cap_millions = None
        
        if shares_outstanding and current_price and shares_outstanding > 0:
            calculated_market_cap = (shares_outstanding * current_price) / 1_000_000
            intraday_market_cap_millions = calculated_market_cap
        elif market_cap and current_price and current_price > 0:
            intraday_market_cap_millions = market_cap / 1_000_000
            calculated_market_cap = intraday_market_cap_millions
            shares_outstanding = market_cap / current_price
        
        stock_data.update({
            'company_name': company_name,
            'market_cap': market_cap,
            'share_class_shares_outstanding': shares_outstanding,
            'intraday_market_cap_millions': intraday_market_cap_millions,
            'calculated_market_cap': calculated_market_cap,
        })
    
    async def process_batch_async(self, symbols_batch, stats, period_info):
        """Process batch of symbols"""
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_REQUESTS,
            limit_per_host=MAX_CONCURRENT_REQUESTS,
            ttl_dns_cache=300,
            use_dns_cache=True,
            enable_cleanup_closed=True,
            keepalive_timeout=60,
        )
        
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=10)
        
        async with aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout,
            headers={'Connection': 'keep-alive', 'User-Agent': 'PolygonDataCollector/2.0'}
        ) as session:
            tasks = [self.get_stock_data(session, symbol, stats, period_info) for symbol in symbols_batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            complete_results = []
            
            for i, result in enumerate(results):
                symbol = symbols_batch[i]
                update_stats(stats, processed=1)
                
                if isinstance(result, dict) and self._is_complete_record(result):
                    complete_results.append(result)
                else:
                    if isinstance(result, Exception):
                        update_stats(stats, filtered_out=1)
            
            return complete_results
    
    def _is_complete_record(self, stock_data):
        """Check if record has essential data using new column names"""
        if not stock_data:
            return False
        
        essential_fields = ['previous_open', 'previous_high', 'previous_low', 'previous_close', 'previous_volume', 'previous_current_price']
        for field in essential_fields:
            value = stock_data.get(field)
            if value is None or value <= 0:
                return False
        
        # Sanity check: low <= open/close <= high
        low = stock_data.get('previous_low')
        high = stock_data.get('previous_high')
        open_price = stock_data.get('previous_open')
        close = stock_data.get('previous_close')
        
        if not (low <= open_price <= high and low <= close <= high):
            return False
        
        return True
    
    def fetch_all_stock_data(self, symbols, period_info, stats):
        """Fetch data for all symbols using batched processing"""
        all_complete_results = []
        
        # Process in batches
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (len(symbols)-1)//BATCH_SIZE + 1
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                batch_results = loop.run_until_complete(self.process_batch_async(batch, stats, period_info))
                all_complete_results.extend(batch_results)
            finally:
                loop.close()
            
            complete_rate = (stats['complete_records'] / stats['processed']) * 100 if stats['processed'] > 0 else 0
            logger.info(f"Batch {batch_num}/{total_batches}: {stats['complete_records']} complete ({complete_rate:.1f}%)")
            
            if i + BATCH_SIZE < len(symbols):
                time.sleep(BATCH_DELAY)
        
        return all_complete_results
    
    def get_latest_nasdaq_symbols_from_s3(self):
        """Download latest comprehensive symbols from S3"""
        try:
            import boto3
            s3 = boto3.client("s3")
            prefix = "stock_data/symbols/"
            response = s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
            
            if 'Contents' not in response:
                return None
                
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            
            # Look for nasdaq_symbols files (preferred) or comprehensive_symbols files
            date_pattern = re.compile(r'(nasdaq_symbols|comprehensive_symbols)_(\d{8})\.csv')
            latest_file = None
            latest_date = None
            
            for f in files:
                m = date_pattern.search(f)
                if m:
                    file_date = m.group(2)
                    if latest_date is None or file_date > latest_date:
                        latest_date = file_date
                        latest_file = f
            
            if not latest_file:
                return None
                
            local_path = f"symbols_{latest_date}.csv"
            if download_from_s3(self.s3_bucket, latest_file, local_path):
                return local_path
            return None
                
        except Exception as e:
            logger.error(f"Error downloading symbols from S3: {e}")
            return None
    
    def get_comprehensive_search_configs(self):
        """Get comprehensive search configurations for fallback"""
        return [
            {'market': 'stocks', 'exchange': 'XNAS', 'type_filter': 'CS'},  # NASDAQ
            {'market': 'stocks', 'exchange': 'XNYS', 'type_filter': 'CS'},  # NYSE
            {'market': 'stocks', 'exchange': None, 'type_filter': 'CS'},    # All US stocks
        ]
    
    def _fetch_symbols_from_polygon_api(self, max_symbols=None):
        """Enhanced fallback method with comprehensive search"""
        try:
            client = RESTClient(self.api_key)
            all_symbols = set()
            
            logger.info("Using comprehensive symbol search as fallback")
            
            # Try multiple search configurations
            configs = self.get_comprehensive_search_configs()
            
            for config in configs:
                try:
                    logger.info(f"Searching: market={config.get('market', 'all')}, exchange={config.get('exchange', 'all')}")
                    
                    search_params = {'active': True, 'limit': 1000}
                    if config['market']:
                        search_params['market'] = config['market']
                    if config['exchange']:
                        search_params['exchange'] = config['exchange']
                    
                    config_symbols = set()
                    item_count = 0
                    max_items_per_config = 50000  # Reasonable limit
                    
                    tickers_iter = client.list_tickers(**search_params)
                    
                    for ticker in tickers_iter:
                        # Apply type filter
                        if config['type_filter'] and hasattr(ticker, 'type'):
                            if ticker.type != config['type_filter']:
                                continue
                        
                        # Basic symbol validation
                        symbol = ticker.ticker.strip().upper()
                        if (symbol.isalpha() and 
                            len(symbol) <= 6 and 
                            symbol not in ['TEST', 'EXAMPLE']):
                            config_symbols.add(symbol)
                        
                        item_count += 1
                        if item_count >= max_items_per_config:
                            logger.info(f"Reached limit for config: {max_items_per_config}")
                            break
                    
                    logger.info(f"Config found {len(config_symbols)} symbols")
                    all_symbols.update(config_symbols)
                    
                    # Stop if we have enough symbols
                    if max_symbols and len(all_symbols) >= max_symbols * 2:
                        break
                        
                except Exception as e:
                    logger.warning(f"Config failed: {e}")
                    continue
            
            if not all_symbols:
                logger.error("No symbols found in comprehensive search")
                return []
            
            symbols_list = sorted(list(all_symbols))
            
            if max_symbols:
                symbols_list = symbols_list[:max_symbols]
            
            logger.info(f"Comprehensive search found {len(symbols_list)} symbols")
            return symbols_list
            
        except Exception as e:
            logger.error(f"Comprehensive symbol search failed: {e}")
            return []
    
    def get_symbols_with_fallback(self, max_symbols=None):
        """Get symbols from S3, fallback to comprehensive Polygon API search"""
        local_symbols_path = self.get_latest_nasdaq_symbols_from_s3()
        
        if local_symbols_path and os.path.exists(local_symbols_path):
            try:
                df = pd.read_csv(local_symbols_path)
                
                symbol_col = None
                for col in ['symbol', 'Symbol', 'ticker', 'Ticker']:
                    if col in df.columns:
                        symbol_col = col
                        break
                
                if symbol_col:
                    symbols = df[symbol_col].dropna().astype(str).tolist()
                    symbols = [s.strip().upper() for s in symbols if s.strip().isalpha() and len(s.strip()) <= 6]
                    symbols = sorted(list(set(symbols)))
                    
                    if symbols:
                        logger.info(f"Loaded {len(symbols)} symbols from S3")
                        if max_symbols:
                            symbols = symbols[:max_symbols]
                        return symbols
            except Exception as e:
                logger.warning(f"Error reading symbols from S3: {e}")
        
        # Fallback to comprehensive Polygon API search
        logger.info("Falling back to comprehensive Polygon API search")
        return self._fetch_symbols_from_polygon_api(max_symbols)