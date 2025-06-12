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
    MAX_RETRIES, RETRY_DELAY, BATCH_DELAY, MIN_COMPLETE_RATE,
    MIN_MARKET_CAP_MILLIONS, MIN_PREVIOUS_CLOSE
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
    """Get stock data for specified period with prefixed column names"""
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
    
    # Initialize stock data with symbol and period prefix
    data_period = period_info['data_period']
    stock_data = {'symbol': symbol}
    
    # Process price data with prefixed column names
    if isinstance(price_data, dict) and price_data.get('results') and len(price_data['results']) > 0:
        result = price_data['results'][0]
        
        open_price = result.get('o')
        high_price = result.get('h')
        low_price = result.get('l')
        close_price = result.get('c')
        volume = result.get('v')
        
        if all(x is not None and x > 0 for x in [open_price, high_price, low_price, close_price, volume]):
            # Use prefixed column names
            stock_data.update({
                f'{data_period}_open': open_price,
                f'{data_period}_high': high_price,
                f'{data_period}_low': low_price,
                f'{data_period}_close': close_price,
                f'{data_period}_volume': volume,
                f'{data_period}_current_price': close_price,
                f'{data_period}_avg_volume': volume,
            })
        else:
            update_stats(stats, incomplete_records=1)
            return None
    else:
        update_stats(stats, incomplete_records=1)
        return None
    
    # Process company data (not prefixed as it's static)
    company_name = 'N/A'
    market_cap = None
    shares_outstanding = None
    intraday_market_cap_millions = None
    calculated_market_cap = None
    
    if isinstance(company_data, dict) and company_data.get('results'):
        result = company_data['results']
        company_name = result.get('name', 'N/A')
        market_cap = result.get('market_cap')
        
        shares_outstanding = (
            result.get('share_class_shares_outstanding') or
            result.get('weighted_shares_outstanding') or
            result.get('shares_outstanding')
        )
        
        current_price = stock_data.get(f'{data_period}_current_price')
        
        # Calculate market cap using current price and shares outstanding
        if shares_outstanding and current_price and shares_outstanding > 0:
            calculated_market_cap = (shares_outstanding * current_price) / 1_000_000
            intraday_market_cap_millions = calculated_market_cap
        elif market_cap and current_price and current_price > 0:
            # Use Polygon's market cap as fallback
            intraday_market_cap_millions = market_cap / 1_000_000
            calculated_market_cap = intraday_market_cap_millions
            # Estimate shares from market cap
            shares_outstanding = market_cap / current_price
    
    stock_data.update({
        'company_name': company_name,
        'market_cap': market_cap,
        'share_class_shares_outstanding': shares_outstanding,
        'intraday_market_cap_millions': intraday_market_cap_millions,
        'calculated_market_cap': calculated_market_cap,
    })
    
    # Calculate percentage change with prefixed columns
    open_price = stock_data.get(f'{data_period}_open')
    current_price = stock_data.get(f'{data_period}_current_price')
    
    if open_price and current_price and open_price != 0:
        pct_change = ((current_price - open_price) / open_price) * 100
        stock_data[f'{data_period}_price_pct_change_from_open'] = pct_change
    else:
        stock_data[f'{data_period}_price_pct_change_from_open'] = 0.0
    
    update_stats(stats, complete_records=1)
    return stock_data

def is_complete_record(stock_data, data_period):
    """Check if record has essential data with prefixed columns"""
    if not stock_data:
        return False
    
    essential_fields = [f'{data_period}_open', f'{data_period}_high', 
                       f'{data_period}_low', f'{data_period}_close', 
                       f'{data_period}_volume', f'{data_period}_current_price']
    
    for field in essential_fields:
        value = stock_data.get(field)
        if value is None or value <= 0:
            return False
    
    # Sanity check: low <= open/close <= high
    low = stock_data.get(f'{data_period}_low')
    high = stock_data.get(f'{data_period}_high')
    open_price = stock_data.get(f'{data_period}_open')
    close = stock_data.get(f'{data_period}_close')
    
    if not (low <= open_price <= high and low <= close <= high):
        return False
    
    return True

def apply_prefilter(stock_data, data_period):
    """
    Apply pre-filtering criteria with prefixed column names:
    - calculated_market_cap >= MIN_MARKET_CAP_MILLIONS (default: $50M)
    - previous_close >= MIN_PREVIOUS_CLOSE (default: $3.00)
    """
    if not stock_data:
        return False
    
    # Check market cap
    calculated_market_cap = stock_data.get('calculated_market_cap')
    if calculated_market_cap is None or calculated_market_cap < MIN_MARKET_CAP_MILLIONS:
        return False
    
    # Check previous close price (use appropriate period)
    close_price = stock_data.get(f'{data_period}_close')
    if close_price is None or close_price < MIN_PREVIOUS_CLOSE:
        return False
    
    return True

def get_base_fieldnames(data_period):
    """Get base fieldnames for a specific data period"""
    return [
        f'{data_period}_open', f'{data_period}_high', f'{data_period}_low', 
        f'{data_period}_close', f'{data_period}_volume', 
        f'{data_period}_current_price', f'{data_period}_avg_volume',
        f'{data_period}_price_pct_change_from_open'
    ]

def get_premarket_gainers_list(date_str):
    """Load premarket gainers list from local file or S3"""
    gainers_filename = f"premarket_top_gainers_{date_str}.csv"
    
    # Try to download from S3 if not local
    if not os.path.exists(gainers_filename) and S3_BUCKET:
        try:
            s3_key = f"stock_data/{date_str}/{gainers_filename}"
            download_from_s3(S3_BUCKET, s3_key, gainers_filename)
            logger.info(f"Downloaded {gainers_filename} from S3")
        except Exception as e:
            logger.debug(f"Could not download premarket gainers from S3: {e}")
    
    # Try to read the file
    if os.path.exists(gainers_filename):
        try:
            df = pd.read_csv(gainers_filename)
            
            # Look for ticker/symbol column
            symbol_col = None
            for col in ['ticker', 'symbol', 'Ticker', 'Symbol']:
                if col in df.columns:
                    symbol_col = col
                    break
            
            if symbol_col:
                gainers = set(df[symbol_col].dropna().astype(str).str.strip().str.upper())
                logger.info(f"Loaded {len(gainers)} premarket top gainers from {gainers_filename}")
                return gainers
            else:
                logger.warning(f"No ticker/symbol column found in {gainers_filename}")
                
        except Exception as e:
            logger.warning(f"Error reading premarket gainers file: {e}")
    else:
        logger.warning(f"Premarket gainers file not found: {gainers_filename}")
    
    return set()

def add_top_gainer_flag(stock_data_list, premarket_gainers_set):
    """Add top_gainer flag to stock data"""
    for stock_data in stock_data_list:
        symbol = stock_data.get('symbol', '').upper()
        stock_data['top_gainer'] = symbol in premarket_gainers_set
    
    return stock_data_list

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
            
            if isinstance(result, dict) and is_complete_record(result, period_info['data_period']):
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

def get_current_cst_time():
    """Get current CST time"""
    cst = pytz.timezone('America/Chicago')
    return datetime.now(cst)

def run_initial_data_pull(date_str=None, max_symbols=None, force_previous_day=False):
    """Run initial data pull with prefixed column names"""
    if not date_str:
        date_str = get_date_str()
    
    try:
        # Determine data period
        if force_previous_day:
            period_info = {
                'data_period': 'previous',
                'current_time_cst': get_current_cst_time().strftime('%H:%M:%S'),
                'date_str': get_current_cst_time().strftime('%Y-%m-%d')
            }
        else:
            period_info = get_data_period()
        
        data_period = period_info['data_period']
        symbols = get_symbols_with_fallback()
        
        if not symbols:
            raise Exception("No symbols available")
        
        if max_symbols:
            symbols = symbols[:max_symbols]
        
        logger.info(f"Starting data pull for {len(symbols)} symbols - Period: {data_period}")
        
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
        
        # Determine if we should apply pre-filtering (8:25 step)
        current_time = period_info['current_time_cst']
        is_prefilter_step = force_previous_day or current_time < "08:30"
        
        filtered_results = []
        if is_prefilter_step:
            # Load premarket gainers list for top_gainer flag
            logger.info("Loading premarket gainers list...")
            premarket_gainers = get_premarket_gainers_list(date_str)
            
            # Add top_gainer flag to all stocks
            all_complete_results = add_top_gainer_flag(all_complete_results, premarket_gainers)
            
            # Apply pre-filtering for 8:25 step
            original_count = len(all_complete_results)
            for stock_data in all_complete_results:
                if apply_prefilter(stock_data, data_period):
                    filtered_results.append(stock_data)
            
            # Count how many top gainers made it through pre-filtering
            top_gainer_count = sum(1 for stock in filtered_results if stock.get('top_gainer', False))
            
            logger.info(f"Pre-filtering: {len(filtered_results)}/{original_count} stocks passed")
            logger.info(f"Top gainers in filtered dataset: {top_gainer_count}/{len(filtered_results)} stocks")
            
            final_results = filtered_results
            filename = f"filtered_raw_data_{date_str}.csv"
        else:
            # No pre-filtering for later steps - don't add top_gainer column
            final_results = all_complete_results
            filename = f"raw_data_{date_str}.csv"
        
        if not final_results:
            raise Exception("No stocks passed pre-filtering criteria")
        
        # Create fieldnames with prefixed columns
        base_fieldnames = get_base_fieldnames(data_period)
        common_fieldnames = [
            'symbol', 'company_name', 'market_cap', 'share_class_shares_outstanding',
            'intraday_market_cap_millions', 'calculated_market_cap'
        ]
        
        if is_prefilter_step:
            fieldnames = ['symbol', 'company_name', 'top_gainer'] + base_fieldnames + common_fieldnames[2:]
        else:
            fieldnames = ['symbol', 'company_name'] + base_fieldnames + common_fieldnames[2:]
        
        # Write CSV
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Sort by calculated market cap descending
            final_results.sort(key=lambda x: -(x.get('calculated_market_cap', 0) or 0))
            
            for row in final_results:
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
        
        filter_info = ""
        if is_prefilter_step:
            top_gainer_count = sum(1 for stock in final_results if stock.get('top_gainer', False))
            premarket_gainers_total = len(get_premarket_gainers_list(date_str))
            
            filter_info = f"\nPre-filtered: {len(final_results)}/{len(all_complete_results)} stocks\nTop gainers in result: {top_gainer_count}/{len(final_results)} stocks"
        
        summary = (
            f"Initial Data Pull Complete\n\n"
            f"Period: {data_period.upper()}\n"
            f"Symbols: {format_number(len(symbols))}\n"
            f"Complete: {format_number(len(all_complete_results))} ({complete_rate:.1f}%)"
            f"{filter_info}\n"
            f"Duration: {format_duration(total_time)}\n"
            f"File: {filename}"
        )
        
        logger.info(summary)
        
        if SNS_TOPIC_ARN:
            step_type = "Pre-filtered" if is_prefilter_step else "Standard"
            subject = f"Data Pull Complete - {len(final_results)} stocks ({data_period})"
            send_sns_notification(SNS_TOPIC_ARN, subject, summary)
        
        return True
        
    except Exception as e:
        error_msg = f"Data pull failed: {str(e)}"
        logger.error(error_msg)
        
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "Data Pull Failed",
                f"Error: {error_msg}\nDate: {date_str}"
            )
        
        return False

def run_prefiltered_data_pull(date_str=None, max_symbols=None):
    """Run the 8:25 pre-filtered data pull (previous day data with filtering)"""
    logger.info("Running 8:25 pre-filtered data pull (previous day data)")
    return run_initial_data_pull(date_str=date_str, max_symbols=max_symbols, force_previous_day=True)


if __name__ == "__main__":
    from config import setup_logging, validate_config
    import sys
    
    setup_logging()
    validate_config()
    
    max_symbols = None
    force_previous_day = False
    
    # Parse command line arguments
    for arg in sys.argv[1:]:
        if arg == "--prefilter" or arg == "--8:25":
            force_previous_day = True
        elif arg.isdigit():
            max_symbols = int(arg)
    
    if force_previous_day:
        success = run_prefiltered_data_pull(max_symbols=max_symbols)
        print("✅ Pre-filtered data pull completed" if success else "❌ Pre-filtered data pull failed")
        
        # Show top gainer info if successful
        if success:
            try:
                import pandas as pd
                filename = f"filtered_raw_data_{get_date_str()}.csv"
                if os.path.exists(filename):
                    df = pd.read_csv(filename)
                    if 'top_gainer' in df.columns:
                        top_gainer_count = df['top_gainer'].sum()
                        print(f"📈 {top_gainer_count}/{len(df)} filtered stocks were premarket top gainers")
                        
                        if top_gainer_count > 0:
                            top_gainers = df[df['top_gainer'] == True]['symbol'].tolist()
                            print(f"🔥 Top gainers: {', '.join(top_gainers[:15])}")
                            if len(top_gainers) > 15:
                                print(f"... and {len(top_gainers) - 15} more")
            except Exception as e:
                pass  # Don't fail if we can't show the summary
    else:
        success = run_initial_data_pull(max_symbols=max_symbols)
        print("✅ Data pull completed" if success else "❌ Data pull failed")
    
    if not success:
        exit(1)