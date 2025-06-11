import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_S3_ENABLED = True
S3_BUCKET = os.getenv('BUCKET_NAME')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')

# Polygon API Configuration
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

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
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    return True