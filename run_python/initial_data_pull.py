import os
import csv
import asyncio
import aiohttp
import time
import pandas as pd
import re
import logging
from datetime import datetime
import pytz
from polygon import RESTClient
from config import (
    POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED,
    MAX_CONCURRENT_REQUESTS, REQUEST_TIMEOUT, BATCH_SIZE, 
    MAX_RETRIES, RETRY_DELAY, BATCH_DELAY, MIN_COMPLETE_RATE
)
from utils import (
    get_date_str, upload_to_s3, download_from_s3, send_sns_notification,
    create_stats_counter, update_stats, format_duration, format_number
)

logger = logging.getLogger(__name__)

def get_data_period():
    """Determine data period based on Chicago time (before/after 8:32 AM)"""
    cst = pytz.timezone('America/Chicago')
    now_cst = datetime.now(cst)
    cutoff_time = now_cst.replace(hour=8, minute=32, second=0, microsecond=0)
    
    is_after_cutoff = now_cst >= cutoff_time
    data_period = "today" if is_after_cutoff else "previous"
    
    return {
        'data_period': data_period,
        'current_time_cst': now_cst.strftime('%H:%M:%S'),
        'date_str': now_cst.strftime('%Y-%m-%d')
    }

async def fetch_with_retry(session, url, headers, symbol, max_retries=MAX_RETRIES):
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

async def get_stock_data(session, symbol, api_key, stats, period_info):
    """Get stock data for specified period"""
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Choose URL based on data period
    if period_info['data_period'] == "previous":
        data_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
    else:  # today
        today_date = period_info['date_str']
        data_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{today_date}/{today_date}"
    
    company_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
    
    # Fetch data and company info
    data_task = fetch_with_retry(session, data_url, headers, symbol)
    company_task = fetch_with_retry(session, company_url, headers, symbol)
    
    price_data, company_data = await asyncio.gather(data_task, company_task, return_exceptions=True)
    update_stats(stats, api_calls=2)
    
    # Process price data
    stock_data = {'symbol': symbol, 'data_period': period_info['data_period']}
    
    if isinstance(price_data, dict) and price_data.get('results') and len(price_data['results']) > 0:
        result = price_data['results'][0]
        
        open_price = result.get('o')
        high_price = result.get('h')
        low_price = result.get('l')
        close_price = result.get('c')
        volume = result.get('v')
        
        if all(x is not None and x > 0 for x in [open_price, high_price, low_price, close_price, volume]):
            stock_data.update({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'current_price': close_price,
                'avg_volume': volume,
            })
        else:
            update_stats(stats, incomplete_records=1)
            return None
    else:
        update_stats(stats, incomplete_records=1)
        return None
    
    # Process company data
    company_name = 'N/A'
    market_cap = None
    shares_outstanding = None
    intraday_market_cap_millions = None
    
    if isinstance(company_data, dict) and company_data.get('results'):
        result = company_data['results']
        company_name = result.get('name', 'N/A')
        market_cap = result.get('market_cap')
        
        shares_outstanding = (
            result.get('share_class_shares_outstanding') or
            result.get('weighted_shares_outstanding') or
            result.get('shares_outstanding')
        )
        
        current_price = stock_data.get('current_price')
        if shares_outstanding and current_price and shares_outstanding > 0:
            intraday_market_cap_millions = (shares_outstanding * current_price) / 1_000_000
        elif market_cap and current_price and current_price > 0:
            estimated_shares = market_cap / current_price
            intraday_market_cap_millions = market_cap / 1_000_000
            shares_outstanding = estimated_shares
    
    stock_data.update({
        'company_name': company_name,
        'market_cap': market_cap,
        'share_class_shares_outstanding': shares_outstanding,
        'intraday_market_cap_millions': intraday_market_cap_millions,
    })
    
    # Calculate percentage change
    open_price = stock_data.get('open')
    current_price = stock_data.get('current_price')
    
    if open_price and current_price and open_price != 0:
        pct_change = ((current_price - open_price) / open_price) * 100
        stock_data['current_price_pct_change_from_open'] = pct_change
    else:
        stock_data['current_price_pct_change_from_open'] = 0.0
    
    update_stats(stats, complete_records=1)
    return stock_data

def is_complete_record(stock_data):
    """Check if record has essential data"""
    if not stock_data:
        return False
    
    essential_fields = ['open', 'high', 'low', 'close', 'volume', 'current_price']
    for field in essential_fields:
        value = stock_data.get(field)
        if value is None or value <= 0:
            return False
    
    # Sanity check: low <= open/close <= high
    low = stock_data.get('low')
    high = stock_data.get('high')
    open_price = stock_data.get('open')
    close = stock_data.get('close')
    
    if not (low <= open_price <= high and low <= close <= high):
        return False
    
    return True

async def process_batch_async(symbols_batch, api_key, stats, period_info):
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
        tasks = [get_stock_data(session, symbol, api_key, stats, period_info) for symbol in symbols_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        complete_results = []
        
        for i, result in enumerate(results):
            symbol = symbols_batch[i]
            update_stats(stats, processed=1)
            
            if isinstance(result, dict) and is_complete_record(result):
                complete_results.append(result)
            else:
                if isinstance(result, Exception):
                    update_stats(stats, filtered_out=1)
        
        return complete_results

def get_latest_nasdaq_symbols_from_s3(s3_bucket):
    """Download latest NASDAQ symbols from S3"""
    try:
        import boto3
        s3 = boto3.client("s3")
        prefix = "stock_data/symbols/"
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            return None
            
        files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
        date_pattern = re.compile(r'nasdaq_symbols_(\d{8})\.csv')
        latest_file = None
        latest_date = None
        
        for f in files:
            m = date_pattern.search(f)
            if m:
                file_date = m.group(1)
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = f
        
        if not latest_file:
            return None
            
        local_path = f"nasdaq_symbols_{latest_date}.csv"
        if download_from_s3(s3_bucket, latest_file, local_path):
            return local_path
        return None
            
    except Exception as e:
        logger.error(f"Error downloading symbols from S3: {e}")
        return None

def get_symbols_with_fallback():
    """Get symbols from S3, fallback to Polygon API"""
    local_symbols_path = get_latest_nasdaq_symbols_from_s3(S3_BUCKET)
    
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
                    return symbols
        except Exception as e:
            logger.warning(f"Error reading symbols from S3: {e}")
    
    # Fallback to Polygon API
    try:
        client = RESTClient(POLYGON_API_KEY)
        
        all_tickers = []
        tickers_iter = client.list_tickers(
            market="stocks", 
            exchange="XNAS", 
            active=True, 
            limit=1000
        )
        
        for ticker in tickers_iter:
            if hasattr(ticker, 'type') and ticker.type != 'CS':
                continue
            if '/' in ticker.ticker:
                continue
            
            symbol = ticker.ticker.strip().upper()
            if symbol.isalpha() and len(symbol) <= 6:
                all_tickers.append(symbol)
        
        symbols = sorted(list(set(all_tickers)))
        logger.info(f"Fetched {len(symbols)} symbols from Polygon API")
        return symbols
        
    except Exception as e:
        logger.error(f"Fallback symbol fetch failed: {e}")
        return []

def run_initial_data_pull(date_str=None, max_symbols=None):
    """
    Run initial data pull with time-based period selection
    Before 8:32 AM CDT: pulls previous day data
    After 8:32 AM CDT: pulls today's data
    """
    if not date_str:
        date_str = get_date_str()
    
    try:
        period_info = get_data_period()
        symbols = get_symbols_with_fallback()
        
        if not symbols:
            raise Exception("No symbols available")
        
        if max_symbols:
            symbols = symbols[:max_symbols]
        
        logger.info(f"Starting data pull for {len(symbols)} symbols - Period: {period_info['data_period']} - Time: {period_info['current_time_cst']}")
        
        start_time = time.time()
        stats = create_stats_counter()
        all_complete_results = []
        
        # Process in batches
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (len(symbols)-1)//BATCH_SIZE + 1
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                batch_results = loop.run_until_complete(process_batch_async(batch, POLYGON_API_KEY, stats, period_info))
                all_complete_results.extend(batch_results)
            finally:
                loop.close()
            
            complete_rate = (stats['complete_records'] / stats['processed']) * 100 if stats['processed'] > 0 else 0
            logger.info(f"Batch {batch_num}/{total_batches}: {stats['complete_records']} complete ({complete_rate:.1f}%)")
            
            if i + BATCH_SIZE < len(symbols):
                time.sleep(BATCH_DELAY)
        
        total_time = time.time() - start_time
        
        if not all_complete_results:
            raise Exception("No complete records collected")
        
        # Write CSV
        filename = f"raw_data_{date_str}.csv"
        fieldnames = [
            'symbol', 'company_name', 'data_period',
            'open', 'high', 'low', 'close', 'volume', 
            'current_price', 'avg_volume', 'market_cap', 'share_class_shares_outstanding',
            'intraday_market_cap_millions', 'current_price_pct_change_from_open'
        ]
        
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Sort by market cap descending
            all_complete_results.sort(key=lambda x: -(x.get('intraday_market_cap_millions', 0) or 0))
            
            for row in all_complete_results:
                output_row = {k: row.get(k) for k in fieldnames}
                writer.writerow(output_row)
        
        # Upload to S3
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/{date_str}/{filename}"
            if not upload_to_s3(S3_BUCKET, s3_key, filename):
                raise Exception("Failed to upload to S3")
        
        # Summary
        complete_rate = (stats['complete_records'] / stats['processed']) * 100 if stats['processed'] > 0 else 0
        symbols_per_second = len(symbols) / total_time
        
        summary = (
            f"✅ INITIAL DATA PULL COMPLETE\n\n"
            f"Period: {period_info['data_period'].upper()}\n"
            f"Time: {period_info['current_time_cst']} CDT\n"
            f"Symbols: {format_number(len(symbols))}\n"
            f"Complete: {format_number(len(all_complete_results))} ({complete_rate:.1f}%)\n"
            f"Duration: {format_duration(total_time)}\n"
            f"Speed: {symbols_per_second:.1f} symbols/sec\n"
            f"File: {filename}"
        )
        
        logger.info(summary)
        
        if SNS_TOPIC_ARN:
            subject = f"✅ Data Pull Complete - {len(all_complete_results)} stocks ({period_info['data_period']})"
            send_sns_notification(SNS_TOPIC_ARN, subject, summary)
        
        return True
        
    except Exception as e:
        error_msg = f"Data pull failed: {str(e)}"
        logger.error(error_msg)
        
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "❌ Data Pull Failed",
                f"Error: {error_msg}\nDate: {date_str}"
            )
        
        return False

if __name__ == "__main__":
    from config import setup_logging, validate_config
    import sys
    
    setup_logging()
    validate_config()
    
    max_symbols = None
    if len(sys.argv) > 1:
        try:
            max_symbols = int(sys.argv[1])
        except ValueError:
            pass
    
    success = run_initial_data_pull(max_symbols=max_symbols)
    if success:
        print("✅ Data pull completed")
    else:
        print("❌ Data pull failed")
        exit(1)