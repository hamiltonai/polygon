import pandas as pd
import numpy as np
import asyncio
import aiohttp
import logging
import time
from datetime import datetime
import pytz
from config import (
    POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED, REQUEST_TIMEOUT,
    SEND_BUY_LIST_SNS
)
from utils import (
    get_date_str, get_time_str, upload_to_s3, send_sns_notification,
    format_buy_list_sns
)

logger = logging.getLogger(__name__)

def ensure_filtered_data_exists(date_str, s3_bucket):
    """Ensure filtered_raw_data_YYYYMMDD.csv exists locally"""
    from utils import download_from_s3
    
    filename = f"filtered_raw_data_{date_str}.csv"
    s3_key = f"stock_data/{date_str}/{filename}"
    
    if download_from_s3(s3_bucket, s3_key, filename):
        logger.info(f"Downloaded {filename} from S3")
        return filename
    else:
        raise FileNotFoundError(f"Filtered data file not found: {s3_key}")

def find_column_with_suffix(df, suffix):
    """Find column ending with specific suffix"""
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

async def fetch_current_price_only(session, symbol, api_key):
    """Fetch only current price for a symbol (optimized for momentum checks)"""
    headers = {"Authorization": f"Bearer {api_key}"}
    url = f"https://api.polygon.io/v2/last/trade/{symbol}"
    
    try:
        async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    price = data['results'].get('p')
                    return {
                        'symbol': symbol,
                        'current_price': price
                    }
    except Exception as e:
        logger.debug(f"Error fetching price for {symbol}: {e}")
    
    return {
        'symbol': symbol,
        'current_price': None
    }

async def fetch_batch_prices(symbols, api_key, max_concurrent=50):
    """Fetch current prices for a batch of symbols"""
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
        headers={'User-Agent': 'PolygonMomentumTracker/1.0'}
    ) as session:
        tasks = [fetch_current_price_only(session, symbol, api_key) for symbol in symbols]
        
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

def run_momentum_check(date_str=None, time_str=None, check_time="8:40"):
    """Run momentum check at specified time (8:40 or 8:50)"""
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
        
        # Determine previous price column and create new columns
        if check_time == "8:40":
            prev_price_col = 'today_price_8_37'
            current_price_col = 'today_price_8_40'
            momentum_col = 'momentum_8_40'
            qualified_col = 'qualified_8_40'
            
            # Only check stocks that qualified at 8:37
            if 'qualified_8_37' not in df.columns:
                raise Exception("No 8:37 qualification data found. Run 8:37 qualification first.")
            
            symbols_to_check = df[df['qualified_8_37'] == True]['symbol'].tolist()
            
        elif check_time == "8:50":
            prev_price_col = 'today_price_8_40'
            current_price_col = 'today_price_8_50'
            momentum_col = 'momentum_8_50'
            qualified_col = 'qualified_8_50'
            
            # Only check stocks that maintained momentum at 8:40
            if 'qualified_8_40' not in df.columns:
                raise Exception("No 8:40 momentum data found. Run 8:40 momentum check first.")
            
            symbols_to_check = df[df['qualified_8_40'] == True]['symbol'].tolist()
            
        else:
            raise ValueError("check_time must be '8:40' or '8:50'")
        
        if not symbols_to_check:
            logger.warning(f"No symbols to check for {check_time} momentum")
            # Still create columns but with N/A values
            df[current_price_col] = np.nan
            df[momentum_col] = False
            df[qualified_col] = False
            
            # Save and return success
            df.to_csv(local_path, index=False)
            return True
        
        logger.info(f"Running {check_time} momentum check on {len(symbols_to_check)} symbols...")
        
        # Fetch current prices
        logger.info(f"Fetching current prices for {len(symbols_to_check)} symbols...")
        start_time = time.time()
        current_data = asyncio.run(fetch_batch_prices(symbols_to_check, POLYGON_API_KEY))
        fetch_time = time.time() - start_time
        
        logger.info(f"Fetched prices for {len(current_data)} symbols in {fetch_time:.1f}s")
        
        # Create mapping for quick lookup
        price_map = {item['symbol']: item['current_price'] for item in current_data}
        
        # Add new columns
        df[current_price_col] = np.nan
        df[momentum_col] = False
        df[qualified_col] = False
        
        # Update prices and check momentum
        maintained_momentum = 0
        updated_prices = 0
        
        for idx, row in df.iterrows():
            symbol = row['symbol']
            
            # Only process symbols we're checking
            if symbol not in symbols_to_check:
                continue
            
            # Get current price
            current_price = price_map.get(symbol)
            if current_price is None:
                logger.debug(f"No current price for {symbol}")
                continue
            
            # Update current price
            df.loc[idx, current_price_col] = current_price
            updated_prices += 1
            
            # Get previous price for momentum comparison
            prev_price = row.get(prev_price_col)
            if pd.isna(prev_price) or prev_price is None:
                logger.debug(f"No previous price for {symbol}")
                continue
            
            # Check momentum (current price > previous price)
            has_momentum = current_price > prev_price
            df.loc[idx, momentum_col] = has_momentum
            
            # Update qualification status
            if check_time == "8:40":
                # At 8:40, must have momentum AND be qualified at 8:37
                was_qualified_837 = row.get('qualified_8_37', False)
                is_qualified = was_qualified_837 and has_momentum
            else:  # 8:50
                # At 8:50, must have momentum AND be qualified at 8:40
                was_qualified_840 = row.get('qualified_8_40', False)
                is_qualified = was_qualified_840 and has_momentum
            
            df.loc[idx, qualified_col] = is_qualified
            
            if is_qualified:
                maintained_momentum += 1
        
        logger.info(f"{check_time} momentum results: {maintained_momentum}/{len(symbols_to_check)} stocks maintained momentum")
        logger.info(f"Updated price data for {updated_prices}/{len(symbols_to_check)} stocks")
        
        # Save updated CSV
        df.to_csv(local_path, index=False)
        logger.info(f"Updated {local_path} with {check_time} momentum data")
        
        # Upload to S3
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/{date_str}/{local_path}"
            if not upload_to_s3(S3_BUCKET, s3_key, local_path):
                logger.error("Failed to upload momentum data to S3")
                return False
        
        # Prepare notification
        if check_time == "8:40":
            initial_qualified = df['qualified_8_37'].sum()
        else:  # 8:50
            initial_qualified = df['qualified_8_40'].sum()
        
        message = (
            f"{check_time.upper()} Momentum Check Complete\n\n"
            f"Date: {date_str}\n"
            f"Time: {time_str} CDT\n"
            f"Symbols checked: {len(symbols_to_check):,}\n"
            f"MAINTAINED MOMENTUM: {maintained_momentum:,}/{initial_qualified:,}\n\n"
            f"Criterion: Current price > {prev_price_col.replace('_', ':')} price\n\n"
        )
        
        if maintained_momentum > 0:
            # Get stocks that maintained momentum
            momentum_stocks = df[df[qualified_col] == True].copy()
            
            if not momentum_stocks.empty:
                message += f"STOCKS WITH MOMENTUM:\n"
                
                sample_size = min(15, len(momentum_stocks))
                for i, (_, stock) in enumerate(momentum_stocks.head(sample_size).iterrows(), 1):
                    symbol = stock.get('symbol', 'N/A')
                    current_price = stock.get(current_price_col, 0)
                    prev_price = stock.get(prev_price_col, 0)
                    
                    # Calculate momentum percentage
                    momentum_pct = 0
                    if prev_price and prev_price > 0:
                        momentum_pct = ((current_price - prev_price) / prev_price) * 100
                    
                    message += f"{i:2d}. {symbol}: ${prev_price:.2f} → ${current_price:.2f} (+{momentum_pct:.1f}%)\n"
        else:
            message += f"No stocks maintained momentum at {check_time}"
        
        message += f"\nFile: stock_data/{date_str}/{local_path}"
        
        if check_time == "8:50":
            message += f"\nThese are the FINAL BUY CANDIDATES!"
            
            # Send SNS notification for final buy list
            if SEND_BUY_LIST_SNS and maintained_momentum > 0:
                send_buy_list_sns(df, date_str, time_str)
        else:
            message += f"\nNext: 8:50 final momentum check"
        
        # Send notification
        if SNS_TOPIC_ARN:
            subject = f"{check_time} Momentum - {maintained_momentum} stocks maintained"
            if maintained_momentum == 0:
                subject = f"{check_time} Momentum - No stocks maintained"
            
            send_sns_notification(SNS_TOPIC_ARN, subject, message)
        
        return True
        
    except Exception as e:
        error_msg = f"Error in {check_time} momentum check: {str(e)}"
        logger.error(error_msg)
        
        # Send error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"{check_time} Momentum Check Failed",
                f"Error: {error_msg}\nDate: {date_str}\nTime: {time_str} CDT"
            )
        
        return False

def send_buy_list_sns(df, date_str, time_str):
    """Send final buy list via SNS notification"""
    try:
        # Get final qualified stocks (passed 8:50 momentum)
        final_qualified = df[df.get('qualified_8_50', False) == True].copy()
        
        if final_qualified.empty:
            logger.info("No stocks in final buy list - no SNS notification sent")
            return False
        
        # Prepare summary stats
        summary_stats = {
            'total_analyzed': len(df),
            'pre_filtered': len(df),  # This is already the pre-filtered dataset
            'qualified_8_37': df.get('qualified_8_37', pd.Series()).sum(),
            'maintained_8_40': df.get('qualified_8_40', pd.Series()).sum(),
            'final_buy_list': len(final_qualified)
        }
        
        # Convert to list of dictionaries for formatting
        qualified_stocks = final_qualified.to_dict('records')
        
        # Format SNS message
        subject, message_body = format_buy_list_sns(qualified_stocks, summary_stats, date_str, time_str)
        
        # Send SNS notification
        success = send_sns_notification(SNS_TOPIC_ARN, subject, message_body)
        
        if success:
            logger.info(f"Buy list SNS notification sent successfully")
            logger.info(f"Subject: {subject}")
            logger.info(f"{len(qualified_stocks)} stocks in buy list")
        else:
            logger.error("Failed to send buy list SNS notification")
        
        return success
        
    except Exception as e:
        logger.error(f"Error sending buy list SNS: {e}")
        return False

def run_8_40_momentum_check(date_str=None, time_str=None):
    """Run 8:40 momentum check"""
    return run_momentum_check(date_str, time_str, "8:40")

def run_8_50_momentum_check(date_str=None, time_str=None):
    """Run 8:50 momentum check and send final buy list"""
    return run_momentum_check(date_str, time_str, "8:50")

def get_final_buy_list(date_str=None):
    """Get final buy list (stocks that passed all checks through 8:50)"""
    if not date_str:
        date_str = get_date_str()
    
    try:
        local_path = ensure_filtered_data_exists(date_str, S3_BUCKET)
        df = pd.read_csv(local_path)
        
        if 'qualified_8_50' not in df.columns:
            logger.warning("No qualified_8_50 column found - run 8:50 momentum check first")
            return []
        
        # Filter for final qualified stocks
        final_qualified = df[df['qualified_8_50'] == True]
        
        if final_qualified.empty:
            logger.info("No stocks in final buy list")
            return []
        
        # Convert to list of dictionaries
        buy_list = final_qualified.to_dict('records')
        
        logger.info(f"Found {len(buy_list)} stocks in final buy list")
        return buy_list
        
    except Exception as e:
        logger.error(f"Error getting final buy list: {e}")
        return []

def get_final_buy_symbols(date_str=None):
    """Get final buy list symbols only"""
    buy_list = get_final_buy_list(date_str)
    return [stock.get('symbol', 'N/A') for stock in buy_list]

# Legacy compatibility functions
def update_intraday_data_and_qualified(date_str=None, time_str=None, include_high_low=True):
    """Legacy compatibility function - redirects to appropriate momentum check"""
    current_time = time_str or get_time_str()
    
    if current_time == "08:40":
        return run_8_40_momentum_check(date_str, time_str)
    elif current_time == "08:50":
        return run_8_50_momentum_check(date_str, time_str)
    else:
        logger.warning(f"No momentum check defined for time {current_time}")
        return False

def update_basic_intraday_data(date_str=None, time_str=None):
    """Legacy compatibility wrapper"""
    return update_intraday_data_and_qualified(date_str, time_str, include_high_low=False)

def update_full_intraday_data(date_str=None, time_str=None):
    """Legacy compatibility wrapper"""
    return update_intraday_data_and_qualified(date_str, time_str, include_high_low=True)

if __name__ == "__main__":
    from config import setup_logging, validate_config
    import sys
    import re
    
    setup_logging()
    validate_config()
    
    # Parse command line arguments
    date_str = None
    time_str = None
    check_type = "8:40"  # default
    
    for arg in sys.argv[1:]:
        if arg in ['8:40', '8:50']:
            check_type = arg
        elif re.match(r'^\d{8}$', arg):
            date_str = arg
        elif re.match(r'^\d{2}:\d{2}$', arg):
            time_str = arg
    
    print(f"Running {check_type} momentum check...")
    if date_str:
        print(f"Date: {date_str}")
    if time_str:
        print(f"Time: {time_str}")
    
    # Run the appropriate momentum check
    if check_type == "8:40":
        success = run_8_40_momentum_check(date_str, time_str)
    else:  # 8:50
        success = run_8_50_momentum_check(date_str, time_str)
    
    if success:
        print(f"{check_type} momentum check completed successfully")
        
        # Show current status
        if check_type == "8:40":
            try:
                local_path = ensure_filtered_data_exists(date_str or get_date_str(), S3_BUCKET)
                df = pd.read_csv(local_path)
                qualified_840 = df['qualified_8_40'].sum() if 'qualified_8_40' in df.columns else 0
                print(f"{qualified_840} stocks maintained momentum at 8:40")
            except:
                pass
        else:  # 8:50
            buy_symbols = get_final_buy_symbols(date_str)
            print(f"{len(buy_symbols)} stocks in FINAL BUY LIST")
            if buy_symbols:
                print(f"BUY: {', '.join(buy_symbols[:20])}")
                if len(buy_symbols) > 20:
                    print(f"... and {len(buy_symbols) - 20} more")
    else:
        print(f"{check_type} momentum check failed")
        sys.exit(1)