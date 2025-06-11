import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_S3_ENABLED = True
S3_BUCKET = os.getenv('BUCKET_NAME')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# SNS Configuration for buy list notifications
SEND_BUY_LIST_SNS = os.getenv('SEND_BUY_LIST_SNS', 'True').lower() == 'true'

# Polygon API Configuration
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

# NEW: Pre-filtering Thresholds (8:25 AM step)
MIN_MARKET_CAP_MILLIONS = float(os.getenv('MIN_MARKET_CAP_MILLIONS', '50'))  # $50M minimum
MIN_PREVIOUS_CLOSE = float(os.getenv('MIN_PREVIOUS_CLOSE', '3.00'))  # $3.00 minimum

# NEW: Qualification Thresholds (8:37 AM step)
MIN_VOLUME_MILLIONS = float(os.getenv('MIN_VOLUME_MILLIONS', '1.0'))  # 1M minimum volume
MIN_PRICE_CHANGE_PCT = float(os.getenv('MIN_PRICE_CHANGE_PCT', '5.0'))  # 5% minimum gain
MAX_PRICE_CHANGE_PCT = float(os.getenv('MAX_PRICE_CHANGE_PCT', '60.0'))  # 60% maximum gain

# Performance Settings (configurable)
MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', '35'))
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '18'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '75'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = float(os.getenv('RETRY_DELAY', '0.3'))
BATCH_DELAY = float(os.getenv('BATCH_DELAY', '0.5'))

# Quality Settings
MIN_COMPLETE_RATE = float(os.getenv('MIN_COMPLETE_RATE', '0.6'))  # 60% threshold

# Logging Configuration
def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

# Validation
def validate_config():
    """Validate required configuration"""
    missing = []
    
    if not S3_BUCKET:
        missing.append('BUCKET_NAME')
    if not POLYGON_API_KEY:
        missing.append('POLYGON_API_KEY')
    if not SNS_TOPIC_ARN:
        missing.append('SNS_TOPIC_ARN')
    
    # No additional validation needed for SNS (already checked above)
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Validate threshold values
    if MIN_MARKET_CAP_MILLIONS <= 0:
        raise ValueError("MIN_MARKET_CAP_MILLIONS must be positive")
    if MIN_PREVIOUS_CLOSE <= 0:
        raise ValueError("MIN_PREVIOUS_CLOSE must be positive")
    if MIN_VOLUME_MILLIONS <= 0:
        raise ValueError("MIN_VOLUME_MILLIONS must be positive")
    if not (0 <= MIN_PRICE_CHANGE_PCT <= 100):
        raise ValueError("MIN_PRICE_CHANGE_PCT must be between 0 and 100")
    if not (0 <= MAX_PRICE_CHANGE_PCT <= 200):
        raise ValueError("MAX_PRICE_CHANGE_PCT must be between 0 and 200")
    if MIN_PRICE_CHANGE_PCT >= MAX_PRICE_CHANGE_PCT:
        raise ValueError("MIN_PRICE_CHANGE_PCT must be less than MAX_PRICE_CHANGE_PCT")
    
    return True