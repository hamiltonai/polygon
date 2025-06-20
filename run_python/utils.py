import boto3
import threading
import time
from datetime import datetime
import pytz
import logging

logger = logging.getLogger(__name__)

# Thread-safe counters
stats_lock = threading.Lock()

def create_stats_counter():
    """Create a thread-safe stats counter"""
    return {
        'processed': 0,
        'api_calls': 0,
        'retries': 0,
        'complete_records': 0,
        'incomplete_records': 0,
        'filtered_out': 0
    }

def update_stats(stats, **kwargs):
    """Thread-safe stats update"""
    with stats_lock:
        for key, value in kwargs.items():
            if key in stats:
                stats[key] += value

def get_current_cst_time():
    """Get current time in CST"""
    cst = pytz.timezone('America/Chicago')
    return datetime.now(cst)

def get_date_str():
    """Get current date string in YYYYMMDD format"""
    return get_current_cst_time().strftime('%Y%m%d')

def get_time_str():
    """Get current time string in HH:MM format"""
    return get_current_cst_time().strftime('%H:%M')

def send_sns_notification(topic_arn, subject, message):
    """Send SNS notification"""
    try:
        sns = boto3.client('sns')
        sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message
        )
        logger.info(f"SNS notification sent: {subject}")
        return True
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {e}")
        return False

def format_buy_list_sns(qualified_stocks, summary_stats, date_str, time_str):
    """
    Format buy list for SNS notification with updated column structure
    
    Args:
        qualified_stocks (list): List of qualified stock dictionaries
        summary_stats (dict): Summary statistics
        date_str (str): Date string
        time_str (str): Time string
    
    Returns:
        tuple: (subject, message)
    """
    try:
        # Extract buy symbols for subject
        buy_symbols = [stock.get('symbol', 'N/A') for stock in qualified_stocks]
        symbols_header = ', '.join(buy_symbols[:10])  # First 10 in subject
        if len(buy_symbols) > 10:
            symbols_header += f" (+{len(buy_symbols)-10} more)"
        
        # Create subject
        subject = f"🚀 BUY LIST ({len(buy_symbols)} stocks) - {symbols_header}"
        
        # Create message body
        message = f"""🚀 STOCK BUY LIST

Date: {date_str}
Time: {time_str} CDT
Final Qualified Stocks: {len(buy_symbols)}

📊 SUMMARY STATISTICS:
• Total Stocks Analyzed: {summary_stats.get('total_analyzed', 'N/A'):,}
• Pre-filtered (8:25): {summary_stats.get('pre_filtered', 'N/A'):,}
• Qualified (8:37): {summary_stats.get('qualified_8_37', 'N/A'):,}
• Maintained Momentum (8:40): {summary_stats.get('maintained_8_40', 'N/A'):,}
• Final Buy List (8:50): {summary_stats.get('final_buy_list', 'N/A'):,}

🎯 BUY STOCKS:
{', '.join(buy_symbols)}
"""
        
        if qualified_stocks:
            message += f"\n📈 DETAILED BUY LIST:\n"
            
            for i, stock in enumerate(qualified_stocks, 1):
                symbol = stock.get('symbol', 'N/A')
                company = stock.get('company_name', 'N/A')
                
                # Use new column names
                price_837 = stock.get('today_price_8_37', 0)
                price_850 = stock.get('today_price_8_50', 0)
                prev_close = stock.get('previous_close', 0)
                volume = stock.get('today_volume_8_37', 0)
                mcap = stock.get('calculated_market_cap', 0)
                
                # Calculate gain percentage from previous close to 8:50
                gain_pct = 0
                if prev_close and prev_close > 0 and price_850:
                    gain_pct = ((price_850 - prev_close) / prev_close) * 100
                
                # Format volume
                volume_str = f"{volume/1_000_000:.1f}M" if volume >= 1_000_000 else f"{volume:,.0f}"
                
                # Format market cap
                if mcap >= 1000:
                    mcap_str = f"${mcap/1000:.1f}B"
                else:
                    mcap_str = f"${mcap:.0f}M"
                
                # Momentum indicators using new column names
                momentum_837_840 = "✅" if stock.get('momentum_8_40', False) else "❌"
                momentum_840_850 = "✅" if stock.get('momentum_8_50', False) else "❌"
                
                company_short = company[:25] + "..." if len(company) > 25 else company
                
                message += f"""{i:2d}. {symbol} ({company_short})
    8:37→8:50: ${price_837:.2f} → ${price_850:.2f} (+{gain_pct:.1f}%)
    Volume: {volume_str} | MCap: {mcap_str}
    Momentum: {momentum_837_840} 8:40 | {momentum_840_850} 8:50

"""
        else:
            message += "\nNo stocks qualified for the buy list today."
        
        message += f"""
QUALIFICATION CRITERIA:
✓ Pre-filter: Market cap ≥ $50M, Previous close ≥ $3.00
✓ 8:37 Qualification: Volume ≥ 1M, Gain 5-60%, Price > Open
✓ 8:40 Momentum: Price > 8:37 Price
✓ 8:50 Momentum: Price > 8:40 Price

Generated automatically by Polygon Stock Screener
"""
        
        return subject, message
        
    except Exception as e:
        logger.error(f"Error formatting buy list SNS: {e}")
        # Fallback to simple format
        symbols = ', '.join([stock.get('symbol', 'N/A') for stock in qualified_stocks])
        subject = f"🚀 BUY LIST ({len(qualified_stocks)} stocks)"
        message = f"Buy List for {date_str} at {time_str}:\n\n{symbols}"
        return subject, message

def upload_to_s3(bucket_name, s3_key, local_file_path, content_type='text/csv'):
    """Upload file to S3"""
    try:
        s3 = boto3.client('s3')
        with open(local_file_path, 'rb') as f:
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=f.read(),
                ContentType=content_type
            )
        logger.info(f"File uploaded to S3: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload to S3: {e}")
        return False

def download_from_s3(bucket_name, s3_key, local_file_path):
    """Download file from S3"""
    try:
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, s3_key, local_file_path)
        logger.info(f"File downloaded from S3: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to download from S3: {e}")
        return False

def cleanup_local_files(*file_paths):
    """Clean up local files"""
    import os
    for file_path in file_paths:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Cleaned up local file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to clean up {file_path}: {e}")

def format_duration(seconds):
    """Format duration in human readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def format_number(number):
    """Format number with commas"""
    return f"{number:,}" if isinstance(number, (int, float)) else str(number)

def get_stock_price_from_data(stock_data, time_step):
    """
    Helper function to get stock price for a specific time step
    
    Args:
        stock_data (dict): Stock data dictionary
        time_step (str): Time step like '8_37', '8_40', '8_50'
        
    Returns:
        float: Price for the time step, None if not available
    """
    price_col = f"today_price_{time_step}"
    return stock_data.get(price_col)

def get_stock_percentage_from_data(stock_data, time_step):
    """
    Helper function to get stock percentage change for a specific time step
    
    Args:
        stock_data (dict): Stock data dictionary
        time_step (str): Time step like '8_37', '8_40', '8_50'
        
    Returns:
        float: Percentage change for the time step, None if not available
    """
    pct_col = f"today_percentage_{time_step}"
    return stock_data.get(pct_col)

def validate_stock_data_columns(stock_data, required_columns):
    """
    Validate that stock data contains required columns
    
    Args:
        stock_data (dict): Stock data dictionary
        required_columns (list): List of required column names
        
    Returns:
        tuple: (is_valid: bool, missing_columns: list)
    """
    missing_columns = []
    
    for col in required_columns:
        if col not in stock_data or stock_data[col] is None:
            missing_columns.append(col)
    
    return len(missing_columns) == 0, missing_columns

def calculate_market_cap_millions(price, shares_outstanding):
    """
    Calculate market cap in millions
    
    Args:
        price (float): Stock price
        shares_outstanding (float): Number of shares outstanding
        
    Returns:
        float: Market cap in millions, None if invalid inputs
    """
    try:
        if price is None or shares_outstanding is None:
            return None
        if price <= 0 or shares_outstanding <= 0:
            return None
        
        return (price * shares_outstanding) / 1_000_000
    except (ValueError, TypeError):
        return None

def format_volume_display(volume):
    """
    Format volume for display (e.g., 1.5M, 500K)
    
    Args:
        volume (float): Volume in shares
        
    Returns:
        str: Formatted volume string
    """
    try:
        if volume is None:
            return "N/A"
        
        if volume >= 1_000_000:
            return f"{volume/1_000_000:.1f}M"
        elif volume >= 1_000:
            return f"{volume/1_000:.0f}K"
        else:
            return f"{volume:,.0f}"
    except (ValueError, TypeError):
        return "N/A"

def calculate_market_cap_millions(price, shares_outstanding):
    """
    Calculate market cap in millions
    
    Args:
        price (float): Stock price
        shares_outstanding (float): Number of shares outstanding
        
    Returns:
        float: Market cap in millions, None if invalid inputs
    """
    try:
        if price is None or shares_outstanding is None:
            return None
        if price <= 0 or shares_outstanding <= 0:
            return None
        
        return (price * shares_outstanding) / 1_000_000
    except (ValueError, TypeError):
        return None

def format_market_cap_display(market_cap_millions):
    """
    Format market cap for display (e.g., $1.5B, $500M)
    
    Args:
        market_cap_millions (float): Market cap in millions
        
    Returns:
        str: Formatted market cap string
    """
    try:
        if market_cap_millions is None:
            return "N/A"
        
        if market_cap_millions >= 1_000:
            return f"${market_cap_millions/1_000:.1f}B"
        else:
            return f"${market_cap_millions:.0f}M"
    except (ValueError, TypeError):
        return "N/A"