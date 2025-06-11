import csv
import logging
from polygon import RESTClient
from config import POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED
from utils import get_date_str, upload_to_s3, send_sns_notification

logger = logging.getLogger(__name__)

def get_nasdaq_symbols(date_str=None):
    """
    Get list of NASDAQ stocks from polygon.io (excluding warrants, ETFs, and other non-stock symbols)
    and save to /stock_data/symbols/nasdaq_symbols_YYYYMMDD.csv on S3.
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not date_str:
        date_str = get_date_str()
    
    try:
        client = RESTClient(POLYGON_API_KEY)
        
        # Fetch all NASDAQ tickers with pagination
        logger.info("Fetching NASDAQ symbols from Polygon...")
        
        all_tickers = []
        page_count = 0
        max_pages = 20  # Safety limit
        
        # Use pagination to get all tickers
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
            
            # Exclude if symbol contains "/" (often used for warrants)
            if '/' in ticker.ticker:
                continue
            
            # Additional filtering for valid stock symbols
            symbol = ticker.ticker.strip().upper()
            if symbol.isalpha() and len(symbol) <= 6:
                all_tickers.append(symbol)
            
            page_count += 1
            if page_count >= max_pages * 1000:  # Rough page estimation
                logger.warning(f"Reached safety limit, stopping at {len(all_tickers)} symbols")
                break
        
        # Remove duplicates and sort
        filtered_symbols = sorted(list(set(all_tickers)))
        
        if not filtered_symbols:
            logger.error("No valid NASDAQ symbols found")
            return False
        
        logger.info(f"Found {len(filtered_symbols)} valid NASDAQ symbols")
        
        # Write to CSV
        filename = f"nasdaq_symbols_{date_str}.csv"
        
        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["symbol"])
            for symbol in filtered_symbols:
                writer.writerow([symbol])
        
        logger.info(f"NASDAQ symbols written to {filename}")
        
        # Upload to S3 if enabled
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/symbols/{filename}"
            if upload_to_s3(S3_BUCKET, s3_key, filename):
                logger.info(f"NASDAQ symbols uploaded to S3: {s3_key}")
            else:
                logger.error("Failed to upload NASDAQ symbols to S3")
                return False
        
        # Send success notification
        message = (
            f"📋 NASDAQ SYMBOLS COLLECTION COMPLETE\n\n"
            f"Date: {date_str}\n"
            f"Total symbols collected: {len(filtered_symbols):,}\n"
            f"File: {filename}\n"
            f"S3 location: stock_data/symbols/{filename}\n\n"
            f"Filters applied:\n"
            f"• Exchange: NASDAQ (XNAS)\n"
            f"• Type: Common Stock (CS) only\n"
            f"• Excluded: Warrants, ETFs, special symbols\n"
            f"• Symbol format: Alphabetic, max 6 characters\n\n"
            f"Sample symbols: {', '.join(filtered_symbols[:10])}"
        )
        
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"✅ NASDAQ Symbols Complete - {len(filtered_symbols):,} symbols",
                message
            )
        
        return True
        
    except Exception as e:
        error_msg = f"Error collecting NASDAQ symbols: {str(e)}"
        logger.error(error_msg)
        
        # Send error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "❌ NASDAQ Symbols Collection Failed",
                f"Error: {error_msg}\nDate: {date_str or get_date_str()}"
            )
        
        return False

if __name__ == "__main__":
    from config import setup_logging, validate_config
    
    # Setup
    setup_logging()
    validate_config()
    
    # Test the function
    success = get_nasdaq_symbols()
    if success:
        print("✅ NASDAQ symbols collection completed successfully")
    else:
        print("❌ NASDAQ symbols collection failed")
        exit(1)