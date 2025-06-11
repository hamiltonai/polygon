import pandas as pd
import numpy as np
import asyncio
import aiohttp
import logging
import time
from datetime import datetime
import pytz
from qualification_filter import (
    ensure_raw_data_exists, get_qualified_symbols, 
    is_after_qualification_time, has_required_data_for_qualification, calculate_qualification
)
from config import POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED, REQUEST_TIMEOUT
from utils import (
    get_date_str, get_time_str, upload_to_s3, send_sns_notification,
    create_stats_counter, update_stats
)

logger = logging.getLogger(__name__)

async def fetch_intraday_data(session, symbol, api_key):
    """Fetch improved intraday data for a symbol using Polygon API"""
    headers = {"Authorization": f"Bearer {api_key}"}
    
    # Multiple endpoints for comprehensive data
    endpoints = {
        'last_trade': f"https://api.polygon.io/v2/last/trade/{symbol}",
        'prev_day': f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev",
        'company': f"https://api.polygon.io/v3/reference/tickers/{symbol}",
        'snapshot': f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
    }
    
    result = {
        'symbol': symbol, 
        'price': None, 
        'volume': None, 
        'high': None, 
        'low': None, 
        'shares_out': None,
        'day_volume': None,
        'day_high': None,
        'day_low': None
    }
    
    try:
        # Fetch last trade for current price (most important)
        async with session.get(endpoints['last_trade'], headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    result['price'] = data['results'].get('p')
        
        # Fetch snapshot for comprehensive current day data
        async with session.get(endpoints['snapshot'], headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    ticker_data = data['results']
                    
                    # Get today's trading data
                    day_data = ticker_data.get('day', {})
                    if day_data:
                        result['day_volume'] = day_data.get('v')
                        result['day_high'] = day_data.get('h')
                        result['day_low'] = day_data.get('l')
                        
                        # Use snapshot price if we don't have last trade price
                        if not result['price']:
                            result['price'] = day_data.get('c')  # Current close from snapshot
                    
                    # Get last trade from snapshot as backup
                    if not result['price']:
                        last_trade = ticker_data.get('lastTrade', {})
                        if last_trade:
                            result['price'] = last_trade.get('p')
        
        # Fetch previous day aggregates for volume/high/low fallback
        async with session.get(endpoints['prev_day'], headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    agg_data = data['results'][0]
                    
                    # Use as fallback if we don't have current day data
                    if not result['volume']:
                        result['volume'] = agg_data.get('v')
                    if not result['high']:
                        result['high'] = agg_data.get('h')
                    if not result['low']:
                        result['low'] = agg_data.get('l')
        
        # Fetch company data for shares outstanding (only if not already available)
        async with session.get(endpoints['company'], headers=headers, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get('status') == 'OK' and data.get('results'):
                    company_data = data['results']
                    
                    # Try multiple fields for shares outstanding
                    shares_out = (
                        company_data.get('share_class_shares_outstanding') or
                        company_data.get('weighted_shares_outstanding') or
                        company_data.get('shares_outstanding')
                    )
                    result['shares_out'] = shares_out
        
        # Prioritize current day data over previous day data
        if result['day_volume']:
            result['volume'] = result['day_volume']
        if result['day_high']:
            result['high'] = result['day_high']
        if result['day_low']:
            result['low'] = result['day_low']
        
    except asyncio.TimeoutError:
        logger.debug(f"Timeout fetching intraday data for {symbol}")
    except Exception as e:
        logger.debug(f"Error fetching intraday data for {symbol}: {e}")
    
    return result

async def fetch_batch_intraday_data(symbols, api_key, max_concurrent=50):
    """Fetch intraday data for a batch of symbols with improved error handling"""
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
        headers={'User-Agent': 'PolygonIntradayCollector/1.0'}
    ) as session:
        # Process in smaller sub-batches to avoid overwhelming the API
        sub_batch_size = min(25, len(symbols))
        all_results = []
        
        for i in range(0, len(symbols), sub_batch_size):
            sub_batch = symbols[i:i + sub_batch_size]
            tasks = [fetch_intraday_data(session, symbol, api_key) for symbol in sub_batch]
            
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Filter out exceptions and add valid results
                for result in results:
                    if isinstance(result, dict):
                        all_results.append(result)
                    else:
                        logger.debug(f"Exception in batch fetch: {result}")
                
                # Small delay between sub-batches
                if i + sub_batch_size < len(symbols):
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.warning(f"Error processing sub-batch {i//sub_batch_size + 1}: {e}")
        
        return all_results

def update_intraday_data_and_qualified(date_str=None, time_str=None, include_high_low=True):
    """
    Update raw data with intraday prices and volumes, recalculate qualified status with proper validation.
    Only sets True/False after 8:50 AM CDT and when all required data is present.
    
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
        # Check if it's after qualification time
        after_qualification_time = is_after_qualification_time()
        
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
        logger.info(f"After 8:50 AM qualification time? {after_qualification_time}")
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
        shares_updated_count = 0
        
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
                if pd.isna(current_shares) or current_shares in [None, 'N/A', 0]:
                    df.loc[symbol_mask, shares_col] = symbol_data['shares_out']
                    shares_updated_count += 1
        
        logger.info(f"Updated price data for {updated_count} symbols")
        if shares_updated_count > 0:
            logger.info(f"Updated shares outstanding for {shares_updated_count} symbols")
        
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
        
        # *** FIXED QUALIFICATION LOGIC ***
        def apply_qualification_with_proper_validation(row):
            """Apply qualification logic with time and data validation - FIXED VERSION"""
            
            # If not after 8:50 AM, return N/A
            if not after_qualification_time:
                return f"[{time_str}] - N/A (Before 8:50 AM)"
            
            # Use current time data for qualification check
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
            
            # Check if we have all required data using the same logic as qualification_filter
            if not has_required_data_for_qualification(temp_row):
                return f"[{time_str}] - N/A (Missing Data)"
            
            # Calculate qualification using the same function as qualification_filter
            is_qualified = calculate_qualification(temp_row)
            
            if is_qualified is None:
                return f"[{time_str}] - N/A (Invalid Data)"
            elif is_qualified:
                return f"[{time_str}] - True"
            else:
                return f"[{time_str}] - False"
        
        # Update qualified column with proper validation
        df['qualified'] = df.apply(apply_qualification_with_proper_validation, axis=1)
        
        # Count qualification results
        true_count = df['qualified'].str.contains('True', na=False).sum()
        false_count = df['qualified'].str.contains('False', na=False).sum()
        na_count = df['qualified'].str.contains('N/A', na=False).sum()
        total_count = len(df)
        
        # Count stocks with market cap data
        mcap_count = df[mcap_col].notna().sum()
        mcap_rate = (mcap_count / total_count) * 100 if total_count > 0 else 0
        
        logger.info(f"Qualification update: {true_count} qualified, {false_count} not qualified, {na_count} N/A")
        logger.info(f"Market cap coverage: {mcap_count}/{total_count} stocks ({mcap_rate:.1f}%)")
        
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
        newly_qualified = df[df['qualified'].str.contains('True', na=False)].copy()
        
        time_status = "AFTER 8:50 AM" if after_qualification_time else "BEFORE 8:50 AM"
        
        message = (
            f"📈 INTRADAY UPDATE COMPLETE\n\n"
            f"Date: {date_str}\n"
            f"Time: {time_str} CDT ({time_status})\n"
            f"Updated: {updated_count}/{len(priority_symbols)} symbols\n"
            f"Qualified (True): {true_count:,}\n"
            f"Not Qualified (False): {false_count:,}\n"
            f"Insufficient Data (N/A): {na_count:,}\n"
            f"Market cap coverage: {mcap_count:,} ({mcap_rate:.1f}%)\n"
            f"Data fetch time: {fetch_time:.1f}s\n"
            f"Shares data updated: {shares_updated_count} symbols\n\n"
        )
        
        if include_high_low:
            message += f"📊 Updated columns: price, volume, high, low, market cap, % change, shares\n"
        else:
            message += f"📊 Updated columns: price, volume, market cap, % change, shares\n"
        
        if true_count > 0:
            # Sort by percentage change or market cap
            if pct_col in newly_qualified.columns:
                newly_qualified = newly_qualified.sort_values(pct_col, ascending=False, na_position='last')
            elif mcap_col in newly_qualified.columns:
                newly_qualified = newly_qualified.sort_values(mcap_col, ascending=False, na_position='last')
            
            message += f"\n🎯 QUALIFIED STOCKS:\n"
            
            sample_size = min(10, len(newly_qualified))
            for _, stock in newly_qualified.head(sample_size).iterrows():
                symbol = stock.get('symbol', 'N/A')
                current_price = stock.get(price_col, stock.get('current_price', 0))
                pct_change = stock.get(pct_col, 0)
                volume = stock.get(vol_col, stock.get('volume', 0))
                market_cap = stock.get(mcap_col, 0)
                
                # Format market cap
                if pd.notna(market_cap) and market_cap > 0:
                    if market_cap >= 1000:
                        mcap_str = f"${market_cap/1000:.1f}B"
                    else:
                        mcap_str = f"${market_cap:.0f}M"
                else:
                    mcap_str = "N/A"
                
                # Format volume
                if pd.notna(volume) and volume > 0:
                    if volume >= 1_000_000:
                        vol_str = f"{volume/1_000_000:.1f}M"
                    elif volume >= 1_000:
                        vol_str = f"{volume/1_000:.0f}K"
                    else:
                        vol_str = f"{int(volume):,}"
                else:
                    vol_str = "N/A"
                
                message += f"{symbol}: {pct_change:+.1f}% (${current_price:.2f}), Vol: {vol_str}, MCap: {mcap_str}\n"
        elif after_qualification_time:
            message += f"\n⚠️ No stocks currently meet qualification criteria"
        else:
            message += f"\nℹ️ Qualification pending until after 8:50 AM CDT"
        
        message += f"\n📁 File: stock_data/{date_str}/{local_path}"
        message += f"\n✅ FIXED: Proper qualification time and data validation applied"
        
        # Send notification
        if SNS_TOPIC_ARN:
            data_type = "Full" if include_high_low else "Basic"
            if after_qualification_time:
                subject = f"📈 {data_type} Intraday Update - {true_count} qualified ({time_str})"
                if true_count == 0:
                    subject = f"⚠️ {data_type} Intraday Update - No qualified stocks ({time_str})"
            else:
                subject = f"📈 {data_type} Intraday Update - Qualification pending ({time_str})"
            
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
    """Update with basic intraday data (price, volume, market cap only)"""
    return update_intraday_data_and_qualified(date_str, time_str, include_high_low=False)

def update_full_intraday_data(date_str=None, time_str=None):
    """Update with full intraday data (price, volume, high, low, market cap)"""
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
    
    print(f"Running FIXED {update_type} intraday update...")
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
        print(f"✅ FIXED {update_type.title()} intraday update completed successfully")
        
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