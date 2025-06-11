import pandas as pd
import numpy as np
import asyncio
import aiohttp
import logging
import time
from qualification_filter import ensure_raw_data_exists, calculate_qualification, get_qualified_symbols
from config import POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED, REQUEST_TIMEOUT
from utils import (
    get_date_str, get_time_str, upload_to_s3, send_sns_notification,
    create_stats_counter, update_stats
)

logger = logging.getLogger(__name__)

async def fetch_intraday_data(session, symbol, api_key):
    """
    Fetch current intraday data for a symbol using Polygon API.
    
    Returns:
        dict: Contains price, volume, high, low, shares_out data
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Get last trade (current price)
    trade_url = f"https://api.polygon.io/v2/last/trade/{symbol}"
    # Get previous day aggregates (for volume, high, low)
    prev_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
    # Get company details (for shares outstanding if needed)
    company_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
    
    result = {'symbol': symbol, 'price': None, 'volume': None, 'high': None, 'low': None, 'shares_out': None}
    
    try:
        # Fetch trade data
        async with session.get(trade_url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    result['price'] = data['results'].get('p')
        
        # Fetch aggregates data  
        async with session.get(prev_url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    agg_data = data['results'][0]
                    result['volume'] = agg_data.get('v')
                    result['high'] = agg_data.get('h')
                    result['low'] = agg_data.get('l')
        
        # Fetch company data for shares outstanding
        async with session.get(company_url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    result['shares_out'] = data['results'].get('share_class_shares_outstanding')
        
    except Exception as e:
        logger.debug(f"Error fetching intraday data for {symbol}: {e}")
    
    return result

async def fetch_batch_intraday_data(symbols, api_key, max_concurrent=50):
    """Fetch intraday data for a batch of symbols"""
    if not symbols:
        return []
    
    connector = aiohttp.TCPConnector(limit=max_concurrent, limit_per_host=max_concurrent)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_intraday_data(session, symbol, api_key) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = []
        for result in results:
            if isinstance(result, dict):
                valid_results.append(result)
            else:
                logger.debug(f"Exception in batch fetch: {result}")
        
        return valid_results

def update_intraday_data_and_qualified(date_str=None, time_str=None, include_high_low=True):
    """
    Update raw data with intraday prices and volumes, recalculate qualified status.
    
    Args:
        date_str: Date string (YYYYMMDD), defaults to today
        time_str: Time string (HH:MM), defaults to current time
        include_high_low: Whether to include high/low data (for full updates)
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not date_str:
        date_str = get_date_str()
    if not time_str:
        time_str = get_time_str()
    
    try:
        # Ensure raw data file exists
        local_path = ensure_raw_data_exists(date_str, S3_BUCKET)
        
        # Read the CSV
        df = pd.read_csv(local_path)
        
        if df.empty:
            logger.warning(f"Raw data file {local_path} is empty")
            return False
        
        # Get all symbols that need updates (prioritize qualified symbols)
        all_symbols = df['symbol'].tolist()
        qualified_symbols = get_qualified_symbols(date_str)
        
        # Prioritize qualified symbols, but update all
        priority_symbols = qualified_symbols + [s for s in all_symbols if s not in qualified_symbols]
        
        logger.info(f"Updating intraday data for {len(priority_symbols)} symbols at {time_str}")
        if qualified_symbols:
            logger.info(f"Prioritizing {len(qualified_symbols)} qualified symbols")
        
        # Fetch intraday data
        start_time = time.time()
        intraday_data = asyncio.run(fetch_batch_intraday_data(priority_symbols, POLYGON_API_KEY))
        fetch_time = time.time() - start_time
        
        logger.info(f"Fetched intraday data for {len(intraday_data)} symbols in {fetch_time:.1f}s")
        
        # Create mapping for quick lookup
        data_map = {item['symbol']: item for item in intraday_data}
        
        # Prepare new column names
        price_col = f'current_price_{time_str.replace(":", "")}'
        pct_col = f'current_price_pct_change_from_open_{time_str.replace(":", "")}'
        vol_col = f'current_volume_{time_str.replace(":", "")}'
        mcap_col = f'intraday_market_cap_millions_{time_str.replace(":", "")}'
        
        if include_high_low:
            high_col = f'high_{time_str.replace(":", "")}'
            low_col = f'low_{time_str.replace(":", "")}'
        
        shares_col = 'share_class_shares_outstanding'
        
        # Add new columns to DataFrame
        df[price_col] = np.nan
        df[vol_col] = np.nan
        df[mcap_col] = np.nan
        df[pct_col] = np.nan
        
        if include_high_low:
            df[high_col] = np.nan
            df[low_col] = np.nan
        
        # Ensure shares outstanding column exists
        if shares_col not in df.columns:
            df[shares_col] = np.nan
        
        # Update data for each symbol
        updated_count = 0
        for symbol in priority_symbols:
            symbol_data = data_map.get(symbol, {})
            
            if not symbol_data:
                continue
            
            # Get the row index for this symbol
            symbol_mask = df['symbol'] == symbol
            if not symbol_mask.any():
                continue
            
            # Update price data
            if symbol_data.get('price') is not None:
                df.loc[symbol_mask, price_col] = symbol_data['price']
                updated_count += 1
            
            # Update volume
            if symbol_data.get('volume') is not None:
                df.loc[symbol_mask, vol_col] = symbol_data['volume']
            
            # Update high/low if requested
            if include_high_low:
                if symbol_data.get('high') is not None:
                    df.loc[symbol_mask, high_col] = symbol_data['high']
                if symbol_data.get('low') is not None:
                    df.loc[symbol_mask, low_col] = symbol_data['low']
            
            # Update shares outstanding if available and not already set
            if symbol_data.get('shares_out') is not None:
                current_shares = df.loc[symbol_mask, shares_col].iloc[0]
                if pd.isna(current_shares) or current_shares in [None, 'N/A']:
                    df.loc[symbol_mask, shares_col] = symbol_data['shares_out']
        
        logger.info(f"Updated price data for {updated_count} symbols")
        
        # Calculate percentage change from open and market cap
        def calc_pct_change(row):
            open_price = row.get('open')
            current_price = row.get(price_col)
            if pd.notna(open_price) and pd.notna(current_price) and open_price != 0:
                return ((current_price - open_price) / open_price) * 100
            return np.nan
        
        def calc_market_cap(row):
            shares = row.get(shares_col)
            price = row.get(price_col)
            if pd.notna(shares) and pd.notna(price) and shares > 0:
                return (shares * price) / 1_000_000
            return np.nan
        
        # Apply calculations
        df[pct_col] = df.apply(calc_pct_change, axis=1)
        df[mcap_col] = df.apply(calc_market_cap, axis=1)
        
        # Update qualified column with new criteria using current time data
        def apply_qualification_with_current_data(row):
            # Use current time data for qualification
            volume = row.get(vol_col, row.get('volume', 0))
            current_price = row.get(price_col, row.get('current_price', 0))
            close = row.get('close', 0)  # Previous close
            open_price = row.get('open', 0)
            
            # Create temporary row with current data for qualification check
            temp_row = {
                'volume': volume,
                'current_price': current_price,
                'close': close,
                'open': open_price,
                'symbol': row.get('symbol', 'Unknown')
            }
            
            is_qualified = calculate_qualification(temp_row)
            return f"[{time_str}] - {'True' if is_qualified else 'False'}"
        
        df['qualified'] = df.apply(apply_qualification_with_current_data, axis=1)
        
        # Count qualified stocks
        qualified_count = df['qualified'].str.contains('True').sum()
        total_count = len(df)
        qualified_rate = (qualified_count / total_count) * 100 if total_count > 0 else 0
        
        logger.info(f"Qualification update: {qualified_count}/{total_count} stocks qualified ({qualified_rate:.1f}%)")
        
        # Save updated CSV
        df.to_csv(local_path, index=False)
        logger.info(f"Updated {local_path} with intraday data and qualification at {time_str}")
        
        # Upload to S3
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/{date_str}/{local_path}"
            if not upload_to_s3(S3_BUCKET, s3_key, local_path):
                logger.error("Failed to upload intraday data to S3")
                return False
            logger.info(f"Intraday data uploaded to S3: {s3_key}")
        
        # Prepare notification with qualified stocks
        newly_qualified = df[df['qualified'].str.contains('True')].copy()
        
        message = (
            f"📈 INTRADAY UPDATE COMPLETE\n\n"
            f"Date: {date_str}\n"
            f"Time: {time_str} CDT\n"
            f"Updated: {updated_count}/{len(priority_symbols)} symbols\n"
            f"Qualified stocks: {qualified_count:,} ({qualified_rate:.1f}%)\n"
            f"Data fetch time: {fetch_time:.1f}s\n\n"
        )
        
        if include_high_low:
            message += f"📊 Updated columns: price, volume, high, low, market cap, % change\n"
        else:
            message += f"📊 Updated columns: price, volume, market cap, % change\n"
        
        if qualified_count > 0:
            # Sort by percentage change or market cap
            if pct_col in newly_qualified.columns:
                newly_qualified = newly_qualified.sort_values(pct_col, ascending=False, na_last=True)
            elif 'intraday_market_cap_millions' in newly_qualified.columns:
                newly_qualified = newly_qualified.sort_values('intraday_market_cap_millions', ascending=False, na_last=True)
            
            message += f"\n🎯 TOP QUALIFIED STOCKS:\n"
            
            sample_size = min(10, len(newly_qualified))
            for _, stock in newly_qualified.head(sample_size).iterrows():
                symbol = stock.get('symbol', 'N/A')
                current_price = stock.get(price_col, stock.get('current_price', 0))
                pct_change = stock.get(pct_col, 0)
                volume = stock.get(vol_col, stock.get('volume', 0))
                
                message += f"{symbol}: {pct_change:+.1f}% (${current_price:.2f}), Vol: {int(volume):,}\n"
        else:
            message += f"\n⚠️ No stocks currently meet qualification criteria"
        
        message += f"\n📁 File: stock_data/{date_str}/{local_path}"
        
        # Send notification
        if SNS_TOPIC_ARN:
            data_type = "Full" if include_high_low else "Basic"
            subject = f"📈 {data_type} Intraday Update - {qualified_count} qualified ({time_str})"
            if qualified_count == 0:
                subject = f"⚠️ {data_type} Intraday Update - No qualified stocks ({time_str})"
            
            send_sns_notification(SNS_TOPIC_ARN, subject, message)
        
        return True
        
    except Exception as e:
        error_msg = f"Error updating intraday data: {str(e)}"
        logger.error(error_msg)
        
        # Send error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"❌ Intraday Update Failed ({time_str})",
                f"Error: {error_msg}\n"
                f"Date: {date_str}\n"
                f"Time: {time_str} CDT"
            )
        
        return False

def update_basic_intraday_data(date_str=None, time_str=None):
    """
    Update with basic intraday data (price, volume, market cap only).
    
    Args:
        date_str: Date string (YYYYMMDD), defaults to today
        time_str: Time string (HH:MM), defaults to current time
        
    Returns:
        bool: True if successful, False otherwise
    """
    return update_intraday_data_and_qualified(date_str, time_str, include_high_low=False)

def update_full_intraday_data(date_str=None, time_str=None):
    """
    Update with full intraday data (price, volume, high, low, market cap).
    
    Args:
        date_str: Date string (YYYYMMDD), defaults to today
        time_str: Time string (HH:MM), defaults to current time
        
    Returns:
        bool: True if successful, False otherwise
    """
    return update_intraday_data_and_qualified(date_str, time_str, include_high_low=True)

if __name__ == "__main__":
    from config import setup_logging, validate_config
    import sys
    import re
    
    # Setup
    setup_logging()
    validate_config()
    
    # Parse command line arguments
    date_str = None
    time_str = None
    update_type = "full"  # default
    
    if len(sys.argv) > 1:
        if sys.argv[1] in ['basic', 'full']:
            update_type = sys.argv[1]
        elif re.match(r'^\d{8}$', sys.argv[1]):
            date_str = sys.argv[1]
        else:
            print("Usage: python intraday_updates.py [basic|full] [YYYYMMDD] [HH:MM]")
            sys.exit(1)
    
    if len(sys.argv) > 2:
        if re.match(r'^\d{8}$', sys.argv[2]):
            date_str = sys.argv[2]
        elif re.match(r'^\d{2}:\d{2}$', sys.argv[2]):
            time_str = sys.argv[2]
    
    if len(sys.argv) > 3:
        if re.match(r'^\d{2}:\d{2}$', sys.argv[3]):
            time_str = sys.argv[3]
    
    print(f"Running {update_type} intraday update...")
    if date_str:
        print(f"Date: {date_str}")
    if time_str:
        print(f"Time: {time_str}")
    
    # Test the function
    if update_type == "basic":
        success = update_basic_intraday_data(date_str, time_str)
    else:
        success = update_full_intraday_data(date_str, time_str)
    
    if success:
        print(f"✅ {update_type.title()} intraday update completed successfully")
        
        # Show current qualified count
        qualified = get_qualified_symbols(date_str)
        print(f"📊 {len(qualified)} stocks currently qualified")
        if qualified:
            print(f"🎯 Sample qualified: {', '.join(qualified[:10])}")
            if len(qualified) > 10:
                print(f"... and {len(qualified) - 10} more")
    else:
        print(f"❌ {update_type.title()} intraday update failed")
        sys.exit(1)