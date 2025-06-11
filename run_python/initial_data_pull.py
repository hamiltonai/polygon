import os
import csv
import asyncio
import aiohttp
import time
import threading
import pandas as pd
import re
import logging
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

async def fetch_with_retry(session, url, headers, symbol, max_retries=MAX_RETRIES):
    """Async fetch with robust retry logic"""
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK':
                        return data
                    else:
                        logger.debug(f"API returned non-OK status for {symbol}: {data.get('status')}")
                        return None
                elif response.status == 429:  # Rate limited
                    wait_time = RETRY_DELAY * (attempt + 1)
                    logger.debug(f"Rate limited for {symbol}, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status == 404:
                    logger.debug(f"Symbol {symbol} not found (404)")
                    return None
                elif response.status >= 500:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    logger.debug(f"Server error {response.status} for {symbol}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.debug(f"HTTP {response.status} for {symbol}")
                    return None
                    
        except asyncio.TimeoutError:
            if attempt == max_retries - 1:
                logger.debug(f"Final timeout for {symbol}")
                return None
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            if attempt == max_retries - 1:
                logger.debug(f"Final error for {symbol}: {e}")
                return None
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    
    return None

async def get_complete_stock_data(session, symbol, api_key, stats):
    """Get complete stock data with improved shares outstanding handling"""
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Create tasks for both endpoints
    prev_day_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
    company_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
    
    # Execute both requests concurrently
    prev_day_task = fetch_with_retry(session, prev_day_url, headers, symbol)
    company_task = fetch_with_retry(session, company_url, headers, symbol)
    
    prev_data, company_data = await asyncio.gather(prev_day_task, company_task, return_exceptions=True)
    
    # Update API call stats
    update_stats(stats, api_calls=2)
    
    # Initialize stock data
    stock_data = {'symbol': symbol}
    
    # Process previous day data (ESSENTIAL)
    if isinstance(prev_data, dict) and prev_data.get('results') and len(prev_data['results']) > 0:
        result = prev_data['results'][0]
        
        open_price = result.get('o')
        high_price = result.get('h')
        low_price = result.get('l')
        close_price = result.get('c')
        volume = result.get('v')
        
        # Validate essential OHLCV data
        if all(x is not None and x > 0 for x in [open_price, high_price, low_price, close_price, volume]):
            stock_data.update({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'current_price': close_price,  # Use close as current price initially
                'avg_volume': volume,  # Use current volume as avg volume proxy
            })
        else:
            # Missing or invalid OHLCV data - mark as incomplete
            logger.debug(f"Invalid OHLCV data for {symbol}: o={open_price}, h={high_price}, l={low_price}, c={close_price}, v={volume}")
            update_stats(stats, incomplete_records=1)
            return None
    else:
        # No previous day data - mark as incomplete
        logger.debug(f"No previous day data for {symbol}")
        update_stats(stats, incomplete_records=1)
        return None
    
    # Process company data (HIGHLY DESIRED)
    company_name = 'N/A'
    market_cap = None
    shares_outstanding = None
    weighted_shares_outstanding = None
    intraday_market_cap_millions = None
    
    if isinstance(company_data, dict) and company_data.get('results'):
        result = company_data['results']
        
        company_name = result.get('name', 'N/A')
        market_cap = result.get('market_cap')
        
        # IMPROVED: Try multiple fields for shares outstanding with proper fallback
        shares_outstanding = (
            result.get('share_class_shares_outstanding') or
            result.get('weighted_shares_outstanding') or
            result.get('shares_outstanding')
        )
        
        # Also store weighted shares as backup info
        weighted_shares_outstanding = result.get('weighted_shares_outstanding')
        
        # Debug logging for major stocks
        if symbol in ['AAPL', 'NVDA', 'MSFT', 'GOOGL', 'AMZN']:
            logger.debug(f"{symbol} shares data: share_class={result.get('share_class_shares_outstanding')}, "
                        f"weighted={result.get('weighted_shares_outstanding')}, "
                        f"shares_outstanding={result.get('shares_outstanding')}")
        
        # Calculate intraday market cap if we have both pieces
        current_price = stock_data.get('current_price')
        if shares_outstanding and current_price and shares_outstanding > 0:
            intraday_market_cap_millions = (shares_outstanding * current_price) / 1_000_000
        elif market_cap and current_price and current_price > 0:
            # Last resort: estimate shares from market cap and current price
            estimated_shares = market_cap / current_price
            intraday_market_cap_millions = market_cap / 1_000_000
            shares_outstanding = estimated_shares
            logger.debug(f"Estimated shares for {symbol}: {estimated_shares:,.0f} (from market_cap / price)")
    else:
        # Log when we can't get company data
        logger.debug(f"No company data for {symbol}")
    
    stock_data.update({
        'company_name': company_name,
        'market_cap': market_cap,
        'share_class_shares_outstanding': shares_outstanding,
        'weighted_shares_outstanding': weighted_shares_outstanding,
        'intraday_market_cap_millions': intraday_market_cap_millions,
    })
    
    # Calculate percentage change from open
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
    """Check if a stock record has all essential data"""
    if not stock_data:
        return False
    
    # Essential fields that must be present and valid
    essential_fields = ['open', 'high', 'low', 'close', 'volume', 'current_price']
    
    for field in essential_fields:
        value = stock_data.get(field)
        if value is None or value <= 0:
            return False
    
    # Additional quality checks
    open_price = stock_data.get('open')
    high_price = stock_data.get('high')
    low_price = stock_data.get('low')
    close_price = stock_data.get('close')
    
    # Basic sanity checks
    if not (low_price <= open_price <= high_price and low_price <= close_price <= high_price):
        logger.debug(f"Sanity check failed for {stock_data.get('symbol', 'Unknown')}: "
                    f"low={low_price}, open={open_price}, high={high_price}, close={close_price}")
        return False
    
    return True

async def process_batch_async(symbols_batch, api_key, stats):
    """Process a batch of symbols asynchronously with quality filtering"""
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
        headers={'Connection': 'keep-alive', 'User-Agent': 'PolygonDataCollector/1.0'}
    ) as session:
        tasks = [get_complete_stock_data(session, symbol, api_key, stats) for symbol in symbols_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter and process results
        complete_results = []
        
        for i, result in enumerate(results):
            symbol = symbols_batch[i]
            update_stats(stats, processed=1)
            
            if isinstance(result, dict) and is_complete_record(result):
                complete_results.append(result)
            else:
                if isinstance(result, Exception):
                    logger.debug(f"Exception for {symbol}: {result}")
                    update_stats(stats, filtered_out=1)
                # incomplete_records already updated in get_complete_stock_data
        
        return complete_results

def get_latest_nasdaq_symbols_from_s3(s3_bucket):
    """Download the latest NASDAQ symbols file from S3"""
    try:
        import boto3
        s3 = boto3.client("s3")
        prefix = "stock_data/symbols/"
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            logger.error(f"No files found in S3 bucket {s3_bucket} with prefix {prefix}")
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
            logger.error("No nasdaq_symbols CSV found in S3.")
            return None
            
        local_path = f"nasdaq_symbols_{latest_date}.csv"
        if download_from_s3(s3_bucket, latest_file, local_path):
            logger.info(f"Downloaded latest NASDAQ symbols file: {latest_file}")
            return local_path
        else:
            return None
            
    except Exception as e:
        logger.error(f"Error downloading symbols from S3: {e}")
        return None

def get_symbols_with_fallback():
    """Get symbols from S3, fallback to Polygon API if needed"""
    # Try to get from S3 first
    local_symbols_path = get_latest_nasdaq_symbols_from_s3(S3_BUCKET)
    
    if local_symbols_path and os.path.exists(local_symbols_path):
        try:
            df = pd.read_csv(local_symbols_path)
            
            # Handle different column name variations
            symbol_col = None
            for col in ['symbol', 'Symbol', 'ticker', 'Ticker']:
                if col in df.columns:
                    symbol_col = col
                    break
            
            if symbol_col:
                symbols = df[symbol_col].dropna().astype(str).tolist()
                
                # Clean and validate symbols
                symbols = [s.strip().upper() for s in symbols if s.strip().isalpha() and len(s.strip()) <= 6]
                symbols = sorted(list(set(symbols)))  # Remove duplicates and sort
                
                if symbols:
                    logger.info(f"Loaded {len(symbols)} symbols from S3")
                    return symbols
        except Exception as e:
            logger.warning(f"Error reading symbols from S3 file: {e}")
    
    # Fallback to Polygon API
    logger.info("Fallback: Fetching symbols from Polygon API...")
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
            # Filter out warrants, ETFs, and non-stock symbols
            if hasattr(ticker, 'type') and ticker.type != 'CS':
                continue
            
            if '/' in ticker.ticker:
                continue
            
            symbol = ticker.ticker.strip().upper()
            if symbol.isalpha() and len(symbol) <= 6:
                all_tickers.append(symbol)
        
        symbols = sorted(list(set(all_tickers)))
        logger.info(f"Fetched {len(symbols)} symbols from Polygon API as fallback")
        return symbols
        
    except Exception as e:
        logger.error(f"Fallback symbol fetch failed: {e}")
        return []

def run_initial_data_pull(date_str=None, max_symbols=None):
    """
    Run improved initial data pull with better shares outstanding handling.
    
    Args:
        date_str: Date string (YYYYMMDD), defaults to today
        max_symbols: Maximum number of symbols to process (for testing)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not date_str:
        date_str = get_date_str()
    
    try:
        # Get symbols
        symbols = get_symbols_with_fallback()
        
        if not symbols:
            raise Exception("No symbols available for processing")
        
        # Limit symbols for testing if requested
        if max_symbols:
            symbols = symbols[:max_symbols]
            logger.info(f"Limited to {max_symbols} symbols for testing")
        
        logger.info(f"Starting IMPROVED initial data pull for {len(symbols)} symbols...")
        logger.info(f"Settings: {MAX_CONCURRENT_REQUESTS} concurrent, {REQUEST_TIMEOUT}s timeout, "
                   f"{MAX_RETRIES} retries, {BATCH_SIZE} batch size")
        logger.info("Improvements: Better shares outstanding handling, enhanced error logging")
        
        start_time = time.time()
        
        # Initialize stats
        stats = create_stats_counter()
        
        # Process symbols in batches
        all_complete_results = []
        
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            batch_start_time = time.time()
            
            batch_num = i//BATCH_SIZE + 1
            total_batches = (len(symbols)-1)//BATCH_SIZE + 1
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} symbols)...")
            
            # Run async batch
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                batch_results = loop.run_until_complete(process_batch_async(batch, POLYGON_API_KEY, stats))
                all_complete_results.extend(batch_results)
            finally:
                loop.close()
            
            batch_time = time.time() - batch_start_time
            complete_rate = (stats['complete_records'] / stats['processed']) * 100 if stats['processed'] > 0 else 0
            
            logger.info(f"Batch {batch_num} completed in {batch_time:.1f}s "
                       f"({len(batch)/batch_time:.1f} symbols/sec)")
            logger.info(f"Progress: {stats['processed']}/{len(symbols)} processed, "
                       f"{stats['complete_records']} complete records ({complete_rate:.1f}%)")
            
            # Delay between batches for API stability
            if i + BATCH_SIZE < len(symbols):
                time.sleep(BATCH_DELAY)
        
        total_time = time.time() - start_time
        
        if not all_complete_results:
            raise Exception("No complete records collected")
        
        # Check if we meet quality threshold
        complete_rate = (stats['complete_records'] / stats['processed']) * 100 if stats['processed'] > 0 else 0
        if complete_rate < MIN_COMPLETE_RATE * 100:
            logger.warning(f"Quality below threshold: {complete_rate:.1f}% < {MIN_COMPLETE_RATE*100}%")
        
        # Count records with shares outstanding data
        shares_count = sum(1 for r in all_complete_results if r.get('share_class_shares_outstanding'))
        shares_rate = (shares_count / len(all_complete_results)) * 100 if all_complete_results else 0
        
        # Write ONLY complete results to CSV
        filename = f"raw_data_{date_str}.csv"
        fieldnames = [
            'symbol', 'company_name', 'open', 'high', 'low', 'close', 'volume', 
            'current_price', 'avg_volume', 'market_cap', 'share_class_shares_outstanding',
            'weighted_shares_outstanding', 'intraday_market_cap_millions', 'current_price_pct_change_from_open'
        ]
        
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Sort by market cap (descending) then by symbol
            def sort_key(item):
                market_cap = item.get('intraday_market_cap_millions', 0) or 0
                return (-market_cap, item.get('symbol', ''))
            
            all_complete_results.sort(key=sort_key)
            
            for row in all_complete_results:
                output_row = {k: row.get(k) for k in fieldnames}
                writer.writerow(output_row)
        
        logger.info(f"Complete data written to {filename}")
        
        # Upload to S3 if enabled
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/{date_str}/{filename}"
            if not upload_to_s3(S3_BUCKET, s3_key, filename):
                raise Exception("Failed to upload to S3")
            logger.info(f"Data uploaded to S3: {s3_key}")
        
        # Performance and quality summary
        symbols_per_second = len(symbols) / total_time
        retry_rate = (stats['retries'] / stats['api_calls']) * 100 if stats['api_calls'] > 0 else 0
        
        summary = (
            f"🎯 IMPROVED INITIAL DATA PULL COMPLETE\n\n"
            f"📊 PROCESSING METRICS:\n"
            f"   Total symbols attempted: {format_number(len(symbols))}\n"
            f"   Total time: {format_duration(total_time)}\n"
            f"   Speed: {symbols_per_second:.1f} symbols/second\n"
            f"   API calls made: {format_number(stats['api_calls'])}\n"
            f"   Retries performed: {format_number(stats['retries'])} ({retry_rate:.1f}% of calls)\n\n"
            f"📈 QUALITY RESULTS:\n"
            f"   Complete records: {format_number(stats['complete_records'])} ({complete_rate:.1f}%)\n"
            f"   Records with shares outstanding: {format_number(shares_count)} ({shares_rate:.1f}%)\n"
            f"   Incomplete records: {format_number(stats['incomplete_records'])}\n"
            f"   Failed requests: {format_number(stats['filtered_out'])}\n"
            f"   Records in final dataset: {format_number(len(all_complete_results))}\n\n"
            f"✨ DATA QUALITY IMPROVEMENTS:\n"
            f"   ✓ All records have complete OHLCV data\n"
            f"   ✓ Improved shares outstanding fallback logic\n"
            f"   ✓ Enhanced error logging and debugging\n"
            f"   ✓ Better market cap calculations\n"
            f"   ✓ Sorted by market cap (largest first)\n\n"
            f"📁 OUTPUT: {filename}\n"
            f"📂 S3: stock_data/{date_str}/{filename}"
        )
        
        logger.info(summary)
        
        # Send success notification
        if SNS_TOPIC_ARN:
            subject = f"✅ Improved Initial Data Pull Complete - {format_number(len(all_complete_results))} stocks"
            if complete_rate >= MIN_COMPLETE_RATE * 100:
                subject += " (Quality Target Met)"
            else:
                subject += " (Quality Below Target)"
            
            send_sns_notification(SNS_TOPIC_ARN, subject, summary)
        
        return True
        
    except Exception as e:
        error_msg = f"Error in initial data pull: {str(e)}"
        logger.error(error_msg)
        
        # Send error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "❌ Initial Data Pull Failed",
                f"Error: {error_msg}\n"
                f"Date: {date_str}\n"
                f"Processed: {stats.get('processed', 0)} symbols\n"
                f"API calls: {stats.get('api_calls', 0)}"
            )
        
        return False

if __name__ == "__main__":
    from config import setup_logging, validate_config
    import sys
    
    # Setup
    setup_logging()
    validate_config()
    
    # Parse command line arguments for testing
    max_symbols = None
    if len(sys.argv) > 1:
        try:
            max_symbols = int(sys.argv[1])
            print(f"Testing with {max_symbols} symbols")
        except ValueError:
            print("Invalid max_symbols argument, using all symbols")
    
    # Test the function
    success = run_initial_data_pull(max_symbols=max_symbols)
    if success:
        print("✅ Improved initial data pull completed successfully")
    else:
        print("❌ Initial data pull failed")
        exit(1)