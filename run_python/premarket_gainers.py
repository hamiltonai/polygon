import csv
import asyncio
import aiohttp
import logging
from polygon import RESTClient
from config import POLYGON_API_KEY, S3_BUCKET, SNS_TOPIC_ARN, AWS_S3_ENABLED
from utils import get_date_str, upload_to_s3, send_sns_notification

logger = logging.getLogger(__name__)

async def fetch_gainers_with_mcap(gainers, api_key):
    """Fetch market cap data for gainers"""
    results = []
    
    async with aiohttp.ClientSession() as session:
        for g in gainers:
            symbol = getattr(g, "ticker", None)
            current_price = getattr(g, "last_trade", None)
            
            if hasattr(current_price, 'p'):
                current_price = current_price.p
            else:
                current_price = None
            
            shares_out = None
            
            # Try to fetch shares_outstanding from company endpoint
            if symbol:
                base_url = "https://api.polygon.io"
                company_url = f"{base_url}/v3/reference/tickers/{symbol}?apiKey={api_key}"
                try:
                    async with session.get(company_url) as resp:
                        company_data = await resp.json()
                        if company_data.get('status') == 'OK' and company_data.get('results'):
                            shares_out = company_data['results'].get('share_class_shares_outstanding')
                except Exception as e:
                    logger.debug(f"Error fetching company data for {symbol}: {e}")
                    shares_out = None
            
            # Calculate intraday market cap
            try:
                if shares_out is not None and current_price is not None:
                    intraday_market_cap_millions = float(shares_out) * float(current_price) / 1_000_000
                else:
                    intraday_market_cap_millions = None
            except Exception:
                intraday_market_cap_millions = None
            
            results.append({
                "ticker": symbol,
                "change_percent": getattr(g, "todays_change_percent", None),
                "direction": "gainer",
                "share_class_shares_outstanding": shares_out,
                "intraday_market_cap_millions": f"{intraday_market_cap_millions:.2f}" if intraday_market_cap_millions is not None else 'N/A',
            })
    
    return results

def get_premarket_top_gainers(date_str=None):
    """
    Get premarket top gainer data from polygon.io and save to CSV/S3.
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not date_str:
        date_str = get_date_str()
    
    try:
        client = RESTClient(POLYGON_API_KEY)
        
        # Get gainers from Polygon
        logger.info("Fetching premarket top gainers...")
        gainers = client.get_snapshot_direction("stocks", "gainers")
        
        if not gainers:
            logger.warning("No gainers data received from Polygon")
            return False
        
        # Process gainers with market cap data
        rows = asyncio.run(fetch_gainers_with_mcap(gainers, POLYGON_API_KEY))
        
        if not rows:
            logger.warning("No valid gainer data processed")
            return False
        
        # Prepare CSV data
        fieldnames = [
            "ticker", "change_percent", "direction", 
            "share_class_shares_outstanding", "intraday_market_cap_millions"
        ]
        
        filename = f"premarket_top_gainers_{date_str}.csv"
        
        # Write to local CSV
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        logger.info(f"Premarket top gainers written to {filename} ({len(rows)} records)")
        
        # Upload to S3 if enabled
        if AWS_S3_ENABLED and S3_BUCKET:
            s3_key = f"stock_data/{date_str}/{filename}"
            if upload_to_s3(S3_BUCKET, s3_key, filename):
                logger.info(f"Premarket top gainers uploaded to S3: {s3_key}")
            else:
                logger.error("Failed to upload premarket gainers to S3")
                return False
        
        # Send success notification
        message = (
            f"📈 PREMARKET TOP GAINERS COLLECTED\n\n"
            f"Date: {date_str}\n"
            f"Total gainers: {len(rows)}\n"
            f"File: {filename}\n\n"
            f"Top 5 gainers:\n"
        )
        
        # Add top 5 gainers to message
        sorted_gainers = sorted(rows, key=lambda x: x.get('change_percent', 0) or 0, reverse=True)
        for i, gainer in enumerate(sorted_gainers[:5], 1):
            ticker = gainer.get('ticker', 'N/A')
            change_pct = gainer.get('change_percent', 0) or 0
            mcap = gainer.get('intraday_market_cap_millions', 'N/A')
            message += f"{i}. {ticker}: +{change_pct:.2f}% (MCap: ${mcap}M)\n"
        
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"✅ Premarket Gainers Complete - {len(rows)} stocks",
                message
            )
        
        return True
        
    except Exception as e:
        error_msg = f"Error collecting premarket top gainers: {str(e)}"
        logger.error(error_msg)
        
        # Send error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "❌ Premarket Gainers Collection Failed",
                f"Error: {error_msg}\nDate: {date_str or get_date_str()}"
            )
        
        return False

if __name__ == "__main__":
    from config import setup_logging, validate_config
    
    # Setup
    setup_logging()
    validate_config()
    
    # Test the function
    success = get_premarket_top_gainers()
    if success:
        print("✅ Premarket gainers collection completed successfully")
    else:
        print("❌ Premarket gainers collection failed")
        exit(1)