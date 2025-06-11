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