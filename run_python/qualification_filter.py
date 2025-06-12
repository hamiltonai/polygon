import pandas as pd
import numpy as np
import asyncio
import aiohttp
import logging
import time
import re
from datetime import datetime
import pytz
from config import (
    POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED, REQUEST_TIMEOUT,
    MIN_VOLUME_MILLIONS, MIN_PRICE_CHANGE_PCT, MAX_PRICE_CHANGE_PCT
)
from utils import (
    get_date_str, get_time_str, upload_to_s3, download_from_s3, 
    send_sns_notification
)

logger = logging.getLogger(__name__)

def ensure_filtered_data_exists(date_str, s3_bucket):
    """Ensure filtered_raw_data_YYYYMMDD.csv exists locally"""
    filename = f"filtered_raw_data_{date_str}.csv"
    s3_key = f"stock_data/{date_str}/{filename}"
    
    if download_from_s3(s3_bucket, s3_key, filename):
        logger.info(f"Downloaded {filename} from S3")
        return filename
    else:
        raise FileNotFoundError(f"Filtered data file not found: {s3_key}")

def find_column_with_suffix(df, suffix):
    """Find column ending with specific suffix (e.g., '_close', '_open')"""
    matching_cols = [col for col in df.columns if col.endswith(suffix)]
    return matching_cols[0] if matching_cols else None

def get_previous_day_columns(df):
    """Get previous day column names from dataframe"""
    return {
        'close': find_column_with_suffix(df, '_close'),
        'open': find_column_with_suffix(df, '_open'),
        'volume': find_column_with_suffix(df, '_volume'),
        'current_price': find_column_with_suffix(df, '_current_price')
    }

async def fetch_current_price_and_volume(session, symbol, api_key):
    """Fetch current price and volume for a symbol using snapshot endpoint"""
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
    
    try:
        async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    ticker_data = data['results']
                    
                    # Get current price from last trade or day data
                    current_price = None
                    last_trade = ticker_data.get('lastTrade', {})
                    if last_trade:
                        current_price = last_trade.get('p')
                    
                    # Get volume from day data
                    volume = None
                    day_data = ticker_data.get('day', {})
                    if day_data:
                        volume = day_data.get('v')
                        # Use day close if we don't have last trade price
                        if not current_price:
                            current_price = day_data.get('c')
                    
                    return {
                        'symbol': symbol,
                        'current_price': current_price,
                        'volume': volume
                    }
    except Exception as e:
        logger.debug(f"Error fetching data for {symbol}: {e}")
    
    return {
        'symbol': symbol,
        'current_price': None,
        'volume': None
    }

async def fetch_batch_current_data(symbols, api_key, max_concurrent=50):
    """Fetch current price and volume data for a batch of symbols"""
    if not symbols:
        return []
    
    connector = aiohttp.TCPConnector(
        limit=max_concurrent, 
        limit_per_host=max_concurrent,
        ttl_dns_cache=300,
        use_dns_cache=True,
        enable_cleanup_closed=True
    )
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(
        connector=connector, 
        timeout=timeout,
        headers={'User-Agent': 'PolygonQualificationChecker/1.0'}
    ) as session:
        tasks = [fetch_current_price_and_volume(session, symbol, api_key) for symbol in symbols]
        
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_results = []
            for result in results:
                if isinstance(result, dict):
                    valid_results.append(result)
                else:
                    logger.debug(f"Exception in batch fetch: {result}")
            
            return valid_results
            
        except Exception as e:
            logger.warning(f"Error in batch fetch: {e}")
            return []

def has_required_data_for_qualification(row, prev_cols):
    """Check if row has all required non-null data for 8:37 qualification"""
    required_fields = ['volume', 'current_price'] + list(prev_cols.values())
    
    for field in required_fields:
        if field is None:  # Column doesn't exist
            return False
        value = row.get(field)
        if value is None or value == '' or value == 'N/A':
            return False
        
        try:
            float_val = float(value)
            if float_val <= 0:
                return False
        except (ValueError, TypeError):
            return False
    
    return True

def calculate_8_37_qualification(row, prev_cols):
    """
    Check if a stock meets 8:37 qualifying criteria:
    - Volume >= 1 Million
    - Price >= 5% and <= 60% from previous close price
    - Price should be > today's open price
    """
    try:
        if not has_required_data_for_qualification(row, prev_cols):
            return False, "Missing required data"
        
        # Get values and convert to float
        volume = float(row.get('volume', 0))
        close = float(row.get(prev_cols['close'], 0))  # Previous close
        open_price = float(row.get(prev_cols['open'], 0))  # Previous open (used as today's open reference)
        current_price = float(row.get('current_price', 0))  # 8:37 price
        
        # Check volume criterion (≥ 1 Million)
        if volume < MIN_VOLUME_MILLIONS * 1_000_000:
            return False, f"Volume too low: {volume:,.0f} < {MIN_VOLUME_MILLIONS*1_000_000:,.0f}"
        
        # Calculate percentage change from previous close
        if close <= 0:
            return False, "Invalid previous close price"
        
        pct_change = ((current_price - close) / close) * 100
        
        # Check price change range (5% to 60%)
        if pct_change < MIN_PRICE_CHANGE_PCT:
            return False, f"Price gain too low: {pct_change:.1f}% < {MIN_PRICE_CHANGE_PCT}%"
        
        if pct_change > MAX_PRICE_CHANGE_PCT:
            return False, f"Price gain too high: {pct_change:.1f}% > {MAX_PRICE_CHANGE_PCT}%"
        
        # Check if current price > previous open (approximation for today's open)
        if current_price <= open_price:
            return False, f"Price not above open: ${current_price:.2f} <= ${open_price:.2f}"
        
        return True, f"Qualified: Vol={volume/1_000_000:.1f}M, Gain={pct_change:.1f}%, Above open"
        
    except (ValueError, TypeError) as e:
        return False, f"Data error: {str(e)}"

def run_8_37_qualification(date_str=None, time_str=None):
    """Run 8:37 qualification on filtered dataset with current day data"""
    if not date_str:
        date_str = get_date_str()
    if not time_str:
        time_str = get_time_str()
    
    try:
        local_path = ensure_filtered_data_exists(date_str, S3_BUCKET)
        df = pd.read_csv(local_path)
        
        if df.empty:
            logger.warning(f"Filtered data file {local_path} is empty")
            return False
        
        # Find previous day columns
        prev_cols = get_previous_day_columns(df)
        if None in prev_cols.values():
            raise Exception(f"Missing required previous day columns: {prev_cols}")
        
        logger.info(f"Running 8:37 qualification on {len(df)} pre-filtered stocks...")
        
        symbols = df['symbol'].tolist()
        
        # Fetch current prices and volumes
        logger.info(f"Fetching current data for {len(symbols)} symbols...")
        start_time = time.time()
        current_data = asyncio.run(fetch_batch_current_data(symbols, POLYGON_API_KEY))
        fetch_time = time.time() - start_time
        
        logger.info(f"Fetched current data for {len(current_data)} symbols in {fetch_time:.1f}s")
        
        # Create mapping for quick lookup
        current_data_map = {item['symbol']: item for item in current_data}
        
        # Add 8:37 data columns
        df['today_price_8_37'] = np.nan
        df['today_volume_8_37'] = np.nan
        df['qualified_8_37'] = 'N/A'
        df['qualification_reason'] = 'N/A'
        
        # Update data and apply qualification
        qualified_count = 0
        updated_count = 0
        
        for idx, row in df.iterrows():
            symbol = row['symbol']
            current_info = current_data_map.get(symbol, {})
            
            # Update 8:37 price and volume
            current_price = current_info.get('current_price')
            current_volume = current_info.get('volume')
            
            if current_price is not None:
                df.loc[idx, 'today_price_8_37'] = current_price
                df.loc[idx, 'current_price'] = current_price  # For qualification
                updated_count += 1
            
            if current_volume is not None:
                df.loc[idx, 'today_volume_8_37'] = current_volume
                df.loc[idx, 'volume'] = current_volume  # For qualification
            
            # Apply qualification logic
            updated_row = df.loc[idx].to_dict()
            is_qualified, reason = calculate_8_37_qualification(updated_row, prev_cols)
            
            df.loc[idx, 'qualified_8_37'] = is_qualified
            df.loc[idx, 'qualification_reason'] = reason
            
            if is_qualified:
                qualified_count += 1
        
        logger.info(f"8:37 qualification results: {qualified_count}/{len(df)} stocks qualified")
        logger.info(f"Updated price data for {updated_count}/{len(df)} stocks")
        
        # Save updated CSV
        df.to_csv(local_path, index=False)
        logger.info(f"Updated {local_path} with 8:37 qualification data")
        
        # Upload to S3
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/{date_str}/{local_path}"
            if not upload_to_s3(S3_BUCKET, s3_key, local_path):
                logger.error("Failed to upload qualified data to S3")
                return False
        
        # Get qualified stocks for notification
        qualified_stocks = df[df['qualified_8_37'] == True].copy()
        
        # Prepare notification
        message = (
            f"8:37 Qualification Complete\n\n"
            f"Date: {date_str}\n"
            f"Time: {time_str} CDT\n"
            f"Pre-filtered stocks: {len(df):,}\n"
            f"QUALIFIED STOCKS: {qualified_count:,}\n\n"
            f"Criteria:\n"
            f"Volume >= {MIN_VOLUME_MILLIONS}M shares\n"
            f"Price gain {MIN_PRICE_CHANGE_PCT}%-{MAX_PRICE_CHANGE_PCT}% from previous close\n"
            f"Current price > previous open\n\n"
        )
        
        if qualified_count > 0:
            # Sort by price gain percentage
            prev_close_col = prev_cols['close']
            qualified_stocks['gain_pct'] = ((qualified_stocks['today_price_8_37'] - qualified_stocks[prev_close_col]) / qualified_stocks[prev_close_col]) * 100
            qualified_stocks = qualified_stocks.sort_values('gain_pct', ascending=False)
            
            message += f"TOP QUALIFIED STOCKS:\n"
            
            sample_size = min(15, len(qualified_stocks))
            for i, (_, stock) in enumerate(qualified_stocks.head(sample_size).iterrows(), 1):
                symbol = stock.get('symbol', 'N/A')
                price_837 = stock.get('today_price_8_37', 0)
                gain_pct = stock.get('gain_pct', 0)
                volume = stock.get('today_volume_8_37', 0) or stock.get('volume', 0)
                
                # Format volume
                if volume >= 1_000_000:
                    vol_str = f"{volume/1_000_000:.1f}M"
                else:
                    vol_str = f"{volume/1_000:.0f}K"
                
                message += f"{i:2d}. {symbol}: +{gain_pct:.1f}% (${price_837:.2f}), Vol: {vol_str}\n"
        else:
            message += "No stocks met the 8:37 qualification criteria"
        
        message += f"\nFile: stock_data/{date_str}/{local_path}"
        
        # Send notification
        if SNS_TOPIC_ARN:
            subject = f"8:37 Qualification - {qualified_count} stocks qualified"
            send_sns_notification(SNS_TOPIC_ARN, subject, message)
        
        return True
        
    except Exception as e:
        error_msg = f"Error in 8:37 qualification: {str(e)}"
        logger.error(error_msg)
        
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "8:37 Qualification Failed",
                f"Error: {error_msg}\nDate: {date_str}\nTime: {time_str} CDT"
            )
        
        return False

def get_qualified_symbols_8_37(date_str=None):
    """Get list of symbols that qualified at 8:37"""
    if not date_str:
        date_str = get_date_str()
    
    try:
        local_path = ensure_filtered_data_exists(date_str, S3_BUCKET)
        df = pd.read_csv(local_path)
        
        if 'qualified_8_37' not in df.columns:
            logger.warning("No qualified_8_37 column found in filtered data")
            return []
        
        qualified_mask = df['qualified_8_37'] == True
        qualified_stocks = df[qualified_mask]
        
        if 'symbol' in qualified_stocks.columns:
            symbols = qualified_stocks['symbol'].tolist()
            logger.info(f"Found {len(symbols)} stocks qualified at 8:37")
            return symbols
        else:
            logger.warning("No symbol column found in qualified data")
            return []
            
    except Exception as e:
        logger.error(f"Error getting 8:37 qualified symbols: {e}")
        return []

# Legacy compatibility functions
def update_qualified_column(date_str=None):
    """Legacy compatibility wrapper for run_8_37_qualification"""
    return run_8_37_qualification(date_str)

def get_qualified_symbols(date_str=None):
    """Legacy compatibility wrapper for get_qualified_symbols_8_37"""
    return get_qualified_symbols_8_37(date_str)

if __name__ == "__main__":
    from config import setup_logging, validate_config
    import sys
    
    setup_logging()
    validate_config()
    
    # Parse command line arguments
    date_str = None
    time_str = None
    
    if len(sys.argv) > 1:
        if re.match(r'^\d{8}$', sys.argv[1]):
            date_str = sys.argv[1]
        elif re.match(r'^\d{2}:\d{2}$', sys.argv[1]):
            time_str = sys.argv[1]
    
    if len(sys.argv) > 2:
        if re.match(r'^\d{2}:\d{2}$', sys.argv[2]):
            time_str = sys.argv[2]
    
    success = run_8_37_qualification(date_str, time_str)
    if success:
        qualified = get_qualified_symbols_8_37(date_str)
        print(f"8:37 qualification completed successfully")
        print(f"{len(qualified)} stocks qualified")
        if qualified:
            print(f"Qualified symbols: {', '.join(qualified[:20])}")
            if len(qualified) > 20:
                print(f"... and {len(qualified) - 20} more")
    else:
        print("8:37 qualification failed")
        exit(1)