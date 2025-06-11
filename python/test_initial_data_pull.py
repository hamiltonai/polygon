import os
import csv
import logging
import pandas as pd
from datetime import datetime
import boto3
import re
import sys
import asyncio
import aiohttp
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
AWS_S3_ENABLED = True
S3_BUCKET = os.getenv('BUCKET_NAME')
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

# QUALITY-FOCUSED Performance settings
MAX_CONCURRENT_REQUESTS = 35  # Conservative for stability
REQUEST_TIMEOUT = 18  # Longer timeout for better success
BATCH_SIZE = 75  # Moderate batch size
MAX_RETRIES = 3  # More retries for reliability
RETRY_DELAY = 0.3  # Longer delays between retries
BATCH_DELAY = 0.5  # Delay between batches

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Thread-safe counters
stats_lock = threading.Lock()
stats = {
    'processed': 0,
    'api_calls': 0,
    'retries': 0,
    'complete_records': 0,
    'incomplete_records': 0,
    'filtered_out': 0
}

def get_latest_nasdaq_symbols_from_s3(s3_bucket):
    """Download the latest NASDAQ symbols file from S3"""
    try:
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
        s3.download_file(s3_bucket, latest_file, local_path)
        logger.info(f"Downloaded latest NASDAQ symbols file: {latest_file}")
        return local_path
        
    except Exception as e:
        logger.error(f"Error downloading symbols from S3: {e}")
        return None

async def fetch_with_retry(session, url, headers, symbol, max_retries=MAX_RETRIES):
    """Async fetch with robust retry logic"""
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                with stats_lock:
                    stats['api_calls'] += 1
                    if attempt > 0:
                        stats['retries'] += 1
                
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK':
                        return data
                    else:
                        # API returned OK status but no useful data
                        return None
                elif response.status == 429:  # Rate limited
                    wait_time = RETRY_DELAY * (attempt + 1)
                    logger.warning(f"Rate limited for {symbol}, waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                elif response.status == 404:
                    # Symbol not found - don't retry
                    return None
                elif response.status >= 500:
                    # Server error - worth retrying
                    wait_time = RETRY_DELAY * (attempt + 1)
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Client error (400s) - don't retry
                    return None
                    
        except asyncio.TimeoutError:
            if attempt == max_retries - 1:
                logger.warning(f"Final timeout for {symbol}")
                return None
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            if attempt == max_retries - 1:
                logger.warning(f"Final error for {symbol}: {e}")
                return None
            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
    
    return None

async def get_complete_stock_data(session, symbol, api_key):
    """Get complete stock data with quality validation"""
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Create tasks for both endpoints
    prev_day_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
    company_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
    
    # Execute both requests concurrently
    prev_day_task = fetch_with_retry(session, prev_day_url, headers, symbol)
    company_task = fetch_with_retry(session, company_url, headers, symbol)
    
    prev_data, company_data = await asyncio.gather(prev_day_task, company_task, return_exceptions=True)
    
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
                'current_price': close_price,  # Use close as current price
                'avg_volume': volume,  # Use current volume as avg volume proxy
            })
        else:
            # Missing or invalid OHLCV data - mark as incomplete
            return None
    else:
        # No previous day data - mark as incomplete
        return None
    
    # Process company data (HIGHLY DESIRED)
    company_name = 'N/A'
    market_cap = None
    shares_outstanding = None
    intraday_market_cap_millions = None
    
    if isinstance(company_data, dict) and company_data.get('results'):
        result = company_data['results']
        
        company_name = result.get('name', 'N/A')
        market_cap = result.get('market_cap')
        shares_outstanding = result.get('share_class_shares_outstanding')
        
        # Calculate intraday market cap if we have both pieces
        current_price = stock_data.get('current_price')
        if shares_outstanding and current_price and shares_outstanding > 0:
            intraday_market_cap_millions = (shares_outstanding * current_price) / 1_000_000
    
    stock_data.update({
        'company_name': company_name,
        'market_cap': market_cap,
        'share_class_shares_outstanding': shares_outstanding,
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
        return False
    
    return True

async def process_batch_async(symbols_batch, api_key):
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
        tasks = [get_complete_stock_data(session, symbol, api_key) for symbol in symbols_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter and process results
        complete_results = []
        
        for i, result in enumerate(results):
            symbol = symbols_batch[i]
            
            with stats_lock:
                stats['processed'] += 1
            
            if isinstance(result, dict) and is_complete_record(result):
                complete_results.append(result)
                with stats_lock:
                    stats['complete_records'] += 1
            else:
                with stats_lock:
                    if isinstance(result, dict):
                        stats['incomplete_records'] += 1
                    else:
                        stats['filtered_out'] += 1
        
        return complete_results

def run_quality_filtered_pull(s3_bucket, api_key, date_str, max_symbols=None):
    """Run quality-focused data pull with complete record filtering"""
    local_symbols_path = get_latest_nasdaq_symbols_from_s3(s3_bucket)
    if not local_symbols_path:
        logger.error("No NASDAQ symbols file found, aborting data pull.")
        return False
    
    try:
        df = pd.read_csv(local_symbols_path)
        
        # Handle different column name variations
        symbol_col = None
        for col in ['symbol', 'Symbol', 'ticker', 'Ticker']:
            if col in df.columns:
                symbol_col = col
                break
        
        if not symbol_col:
            logger.error(f"No symbol column found in {local_symbols_path}")
            logger.info(f"Available columns: {df.columns.tolist()}")
            return False
        
        symbols = df[symbol_col].dropna().astype(str).tolist()
        
        # Clean and validate symbols
        symbols = [s.strip().upper() for s in symbols if s.strip().isalpha() and len(s.strip()) <= 6]
        symbols = sorted(list(set(symbols)))  # Remove duplicates and sort
        
        # Limit symbols for testing if requested
        if max_symbols:
            symbols = symbols[:max_symbols]
        
        logger.info(f"Starting QUALITY FILTERED data pull for {len(symbols)} symbols...")
        logger.info(f"Settings: {MAX_CONCURRENT_REQUESTS} concurrent, {REQUEST_TIMEOUT}s timeout, "
                   f"{MAX_RETRIES} retries, {BATCH_SIZE} batch size")
        logger.info("Only complete records with all essential data will be included in output")
        
        start_time = time.time()
        
        # Reset stats
        global stats
        stats = {
            'processed': 0, 'api_calls': 0, 'retries': 0,
            'complete_records': 0, 'incomplete_records': 0, 'filtered_out': 0
        }
        
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
                batch_results = loop.run_until_complete(process_batch_async(batch, api_key))
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
        
        # Write ONLY complete results to CSV
        filename = f"raw_data_{date_str}.csv"
        fieldnames = [
            'symbol', 'company_name', 'open', 'high', 'low', 'close', 'volume', 
            'current_price', 'avg_volume', 'market_cap', 'share_class_shares_outstanding', 
            'intraday_market_cap_millions', 'current_price_pct_change_from_open'
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
        if AWS_S3_ENABLED and s3_bucket:
            try:
                s3_key = f"stock_data/{date_str}/{filename}"
                s3 = boto3.client("s3")
                with open(filename, "rb") as f:
                    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
                logger.info(f"Data uploaded to S3: {s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload to S3: {e}")
        
        # Performance and quality summary
        symbols_per_second = len(symbols) / total_time
        complete_rate = (stats['complete_records'] / stats['processed']) * 100 if stats['processed'] > 0 else 0
        retry_rate = (stats['retries'] / stats['api_calls']) * 100 if stats['api_calls'] > 0 else 0
        
        print("\n" + "="*80)
        print("🎯 QUALITY FILTERED DATA PULL COMPLETE")
        print("="*80)
        print(f"📊 PROCESSING METRICS:")
        print(f"   Total symbols attempted: {len(symbols):,}")
        print(f"   Total time: {total_time:.1f} seconds")
        print(f"   Speed: {symbols_per_second:.1f} symbols/second")
        print(f"   API calls made: {stats['api_calls']:,}")
        print(f"   Retries performed: {stats['retries']:,} ({retry_rate:.1f}% of calls)")
        print(f"\n📈 QUALITY RESULTS:")
        print(f"   Complete records: {stats['complete_records']:,} ({complete_rate:.1f}%)")
        print(f"   Incomplete records: {stats['incomplete_records']:,}")
        print(f"   Failed requests: {stats['filtered_out']:,}")
        print(f"   Records in final dataset: {len(all_complete_results):,}")
        print(f"\n✨ DATA QUALITY:")
        print(f"   All records have complete OHLCV data")
        print(f"   All records pass sanity checks (high >= low, etc.)")
        print(f"   Sorted by market cap (largest first)")
        print(f"\n📁 OUTPUT: {filename}")
        
        if len(all_complete_results) >= len(symbols) * 0.6:  # 60% threshold
            print(f"✅ QUALITY TARGET MET: {complete_rate:.1f}% complete records")
        else:
            print(f"⚠️  Quality below 60%: {complete_rate:.1f}% complete records")
            print(f"💡 Consider adjusting timeouts or retry settings")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in quality filtered data pull: {e}")
        return False

if __name__ == '__main__':
    DATE_STR = datetime.now().strftime('%Y%m%d')
    
    # Validate environment variables
    if not S3_BUCKET or not POLYGON_API_KEY:
        print('Error: BUCKET_NAME and POLYGON_API_KEY must be set in your environment.')
        sys.exit(1)

    print("🎯 Starting QUALITY FILTERED Polygon data pull...")
    print("Only stocks with complete data will be included in the final dataset")
    print(f"Configuration:")
    print(f"  Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print(f"  Request timeout: {REQUEST_TIMEOUT}s")
    print(f"  Batch size: {BATCH_SIZE}")
    print(f"  Max retries: {MAX_RETRIES}")
    print(f"  Retry delay: {RETRY_DELAY}s")
    
    success = run_quality_filtered_pull(S3_BUCKET, POLYGON_API_KEY, DATE_STR)
    
    if success:
        output_csv = f"raw_data_{DATE_STR}.csv"
        if os.path.exists(output_csv):
            df = pd.read_csv(output_csv)
            print(f"\n📋 FINAL DATASET SUMMARY:")
            print(f"   Total records: {len(df):,}")
            print(f"   All records are complete and validated")
            
            print(f"\n📊 SAMPLE DATA (Top 10 by Market Cap):")
            sample_df = df.head(10)[['symbol', 'company_name', 'open', 'close', 'volume', 'intraday_market_cap_millions']]
            print(sample_df.to_string(index=False))
            
            # Data validation summary
            print(f"\n✅ DATA VALIDATION:")
            print(f"   All {len(df)} records have valid OHLCV data")
            print(f"   All records pass price sanity checks")
            print(f"   Records sorted by market cap (descending)")
            
        else:
            print(f"❌ Error: Output file {output_csv} not found.")
    else:
        print("❌ Quality filtered data pull failed")
        sys.exit(1)