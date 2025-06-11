import pandas as pd
import logging
import re
from config import S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED
from utils import (
    get_date_str, get_time_str, upload_to_s3, download_from_s3, 
    send_sns_notification
)

logger = logging.getLogger(__name__)

def ensure_raw_data_exists(date_str, s3_bucket):
    """
    Ensure raw_data_YYYYMMDD.csv exists locally.
    Downloads from S3 if available, otherwise raises an error.
    
    Returns:
        str: Local path to the raw data file
    """
    filename = f"raw_data_{date_str}.csv"
    s3_key = f"stock_data/{date_str}/{filename}"
    
    # Try to download from S3
    if download_from_s3(s3_bucket, s3_key, filename):
        logger.info(f"Downloaded {filename} from S3")
        return filename
    else:
        raise FileNotFoundError(f"Raw data file not found: {s3_key}")

def calculate_qualification(row):
    """
    Check if a stock meets all qualifying criteria:
    - volume > 300K
    - current price >= 2.5% from previous close
    - close >= $0.01
    - open > close (gap up)
    
    Args:
        row: DataFrame row with stock data
        
    Returns:
        bool: True if qualified, False otherwise
    """
    try:
        # Get values and handle various formats
        volume = float(row.get('volume', 0) or 0)
        close = float(row.get('close', 0) or 0)  # Previous close
        open_price = float(row.get('open', 0) or 0)
        current_price = float(row.get('current_price', 0) or 0)
        
        # Calculate percentage change from previous close
        if close > 0:
            pct_change = ((current_price - close) / close) * 100
        else:
            pct_change = 0
        
        # Apply all criteria
        criteria_met = (
            volume > 300_000 and                    # Volume > 300K
            pct_change >= 2.5 and                   # >= 2.5% from previous close
            close >= 0.01 and                       # Previous close >= $0.01
            open_price > close                       # Gap up (open > previous close)
        )
        
        return criteria_met
        
    except (ValueError, TypeError):
        return False

def update_qualified_column(date_str=None):
    """
    Update the raw_data CSV with a new 'qualified' column using the qualification criteria.
    
    Args:
        date_str: Date string (YYYYMMDD), defaults to today
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not date_str:
        date_str = get_date_str()
    
    try:
        # Ensure raw data file exists
        local_path = ensure_raw_data_exists(date_str, S3_BUCKET)
        
        # Read the CSV
        df = pd.read_csv(local_path)
        
        if df.empty:
            logger.warning(f"Raw data file {local_path} is empty")
            return False
        
        logger.info(f"Processing {len(df)} stocks for qualification...")
        
        # Calculate qualification for each row
        current_time = get_time_str()
        
        def apply_qualification(row):
            is_qualified = calculate_qualification(row)
            return f"[{current_time}] - {'True' if is_qualified else 'False'}"
        
        # Add or update the qualified column
        df['qualified'] = df.apply(apply_qualification, axis=1)
        
        # Count qualified stocks
        qualified_count = df['qualified'].str.contains('True').sum()
        total_count = len(df)
        qualified_rate = (qualified_count / total_count) * 100 if total_count > 0 else 0
        
        logger.info(f"Qualification complete: {qualified_count}/{total_count} stocks qualified ({qualified_rate:.1f}%)")
        
        # Save updated CSV
        df.to_csv(local_path, index=False)
        logger.info(f"Updated {local_path} with qualified column")
        
        # Upload to S3
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/{date_str}/{local_path}"
            if not upload_to_s3(S3_BUCKET, s3_key, local_path):
                logger.error("Failed to upload qualified data to S3")
                return False
            logger.info(f"Qualified data uploaded to S3: {s3_key}")
        
        # Get sample of qualified stocks for notification
        qualified_stocks = df[df['qualified'].str.contains('True')].copy()
        
        if not qualified_stocks.empty:
            # Sort by market cap or volume for better sample
            if 'intraday_market_cap_millions' in qualified_stocks.columns:
                qualified_stocks = qualified_stocks.sort_values('intraday_market_cap_millions', ascending=False, na_last=True)
            elif 'volume' in qualified_stocks.columns:
                qualified_stocks = qualified_stocks.sort_values('volume', ascending=False, na_last=True)
            
            # Create sample list
            sample_size = min(10, len(qualified_stocks))
            sample_stocks = []
            
            for _, stock in qualified_stocks.head(sample_size).iterrows():
                symbol = stock.get('symbol', 'N/A')
                current_price = stock.get('current_price', 0)
                close = stock.get('close', 0)
                volume = stock.get('volume', 0)
                
                # Calculate percentage change
                pct_change = 0
                if close and close > 0:
                    pct_change = ((current_price - close) / close) * 100
                
                sample_stocks.append(f"{symbol}: +{pct_change:.1f}% (${current_price:.2f}), Vol: {int(volume):,}")
        
        # Send notification
        message = (
            f"📊 STOCK QUALIFICATION UPDATE\n\n"
            f"Date: {date_str}\n"
            f"Time: {current_time} CDT\n"
            f"Total stocks processed: {total_count:,}\n"
            f"Qualified stocks: {qualified_count:,} ({qualified_rate:.1f}%)\n\n"
            f"Qualification Criteria:\n"
            f"✓ Volume > 300,000\n"
            f"✓ Price change >= 2.5% from previous close\n"
            f"✓ Previous close >= $0.01\n"
            f"✓ Gap up (open > previous close)\n\n"
        )
        
        if qualified_count > 0:
            message += f"🎯 TOP QUALIFIED STOCKS:\n"
            message += "\n".join(sample_stocks[:10])
        else:
            message += "⚠️ No stocks currently meet all qualification criteria"
        
        message += f"\n\n📁 Updated file: stock_data/{date_str}/{local_path}"
        
        if SNS_TOPIC_ARN:
            subject = f"📊 Qualification Update - {qualified_count} qualified stocks"
            if qualified_count == 0:
                subject = "⚠️ Qualification Update - No qualified stocks"
            
            send_sns_notification(SNS_TOPIC_ARN, subject, message)
        
        return True
        
    except Exception as e:
        error_msg = f"Error updating qualified column: {str(e)}"
        logger.error(error_msg)
        
        # Send error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "❌ Qualification Update Failed",
                f"Error: {error_msg}\n"
                f"Date: {date_str}\n"
                f"Time: {get_time_str()} CDT"
            )
        
        return False

def get_qualified_symbols(date_str=None):
    """
    Get list of currently qualified symbol names from the raw data file.
    
    Args:
        date_str: Date string (YYYYMMDD), defaults to today
        
    Returns:
        list: List of qualified symbol strings, empty list if none or error
    """
    if not date_str:
        date_str = get_date_str()
    
    try:
        local_path = ensure_raw_data_exists(date_str, S3_BUCKET)
        df = pd.read_csv(local_path)
        
        if 'qualified' not in df.columns:
            logger.warning("No qualified column found in raw data")
            return []
        
        # Filter for qualified stocks
        qualified_mask = df['qualified'].str.contains('True', na=False)
        qualified_stocks = df[qualified_mask]
        
        if 'symbol' in qualified_stocks.columns:
            symbols = qualified_stocks['symbol'].tolist()
            logger.info(f"Found {len(symbols)} qualified symbols")
            return symbols
        else:
            logger.warning("No symbol column found in qualified data")
            return []
            
    except Exception as e:
        logger.error(f"Error getting qualified symbols: {e}")
        return []

if __name__ == "__main__":
    from config import setup_logging, validate_config
    import sys
    
    # Setup
    setup_logging()
    validate_config()
    
    # Parse command line arguments
    date_str = None
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        if not re.match(r'^\d{8}$', date_str):
            print("Invalid date format. Use YYYYMMDD")
            exit(1)
    
    # Test the function
    success = update_qualified_column(date_str)
    if success:
        # Show qualified symbols
        qualified = get_qualified_symbols(date_str)
        print(f"✅ Qualification update completed successfully")
        print(f"📊 {len(qualified)} stocks qualified")
        if qualified:
            print(f"🎯 Qualified symbols: {', '.join(qualified[:20])}")
            if len(qualified) > 20:
                print(f"... and {len(qualified) - 20} more")
    else:
        print("❌ Qualification update failed")
        exit(1)