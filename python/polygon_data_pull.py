#polygon.io

import os
import csv
import requests
import boto3
from io import StringIO
from datetime import datetime
import json
import pytz

# Add dotenv support for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # If dotenv is not installed, skip (for AWS Lambda)

def get_polygon_previous_day_data(symbol, api_key):
    """
    Get previous day's open, close, and volume data for a symbol using Polygon.io v2 aggregates endpoint
    """
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if data.get('status') == 'OK' and data.get('results'):
            result = data['results'][0]
            return {
                'open': result.get('o'),
                'close': result.get('c'),
                'volume': result.get('v')
            }
    print(f"Error fetching data for {symbol}: {response.status_code} {response.text}")
    return None

def get_latest_screener_key_from_s3(s3, bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    screener_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    if not screener_files:
        raise FileNotFoundError('No nasdaq_screener_*.csv file found in S3 stock_screener directory')
    return max(screener_files)

def get_tickers_from_s3_screener(s3, bucket_name, screener_key):
    csv_obj = s3.get_object(Bucket=bucket_name, Key=screener_key)
    file_content = csv_obj['Body'].read().decode('utf-8')
    reader = csv.DictReader(StringIO(file_content))
    return [row['Symbol'] for row in reader if row.get('Symbol')]

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    bucket_name = os.getenv('BUCKET_NAME')
    lambda_client = boto3.client('lambda')

    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    input_date = event.get('override_date', datetime.now().strftime('%Y%m%d'))
    input_file_key = f'stock_data/{input_date}/stock_data_{input_date}.csv'

    try:
        # Check if we're in test environment
        is_test = event.get('environment') == 'test'
        
        if is_test:
            # For testing, only process AAPL
            tickers = ['AAPL']
            print("Running in test mode - processing only AAPL")
        else:
            # Get latest screener file from S3 for production
            screener_prefix = 'stock_data/stock_screener/nasdaq_screener_'
            screener_key = get_latest_screener_key_from_s3(s3, bucket_name, screener_prefix)
            tickers = get_tickers_from_s3_screener(s3, bucket_name, screener_key)
        
        valid_rows = []  # Store only rows with valid data
        invalid_tickers = []  # Store tickers with N/A values
        
        for symbol in tickers:
            row = {'ticker': symbol}
            # Get previous day's data
            prev_day_data = get_polygon_previous_day_data(symbol, POLYGON_API_KEY)
            if prev_day_data:
                row['open'] = f"{prev_day_data['open']:.2f}" if prev_day_data['open'] is not None else 'N/A'
                row['close'] = f"{prev_day_data['close']:.2f}" if prev_day_data['close'] is not None else 'N/A'
                row['volume'] = f"{int(prev_day_data['volume'])}" if prev_day_data['volume'] is not None else 'N/A'
            else:
                row['open'] = 'N/A'
                row['close'] = 'N/A'
                row['volume'] = 'N/A'
            
            # Check if any value is N/A
            if 'N/A' not in [row['open'], row['close'], row['volume']]:
                valid_rows.append(row)
            else:
                invalid_tickers.append(symbol)

        # Create CSV output for valid rows
        output = StringIO()
        fieldnames = ['ticker', 'open', 'close', 'volume']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in valid_rows:
            writer.writerow(row)

        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=input_file_key,
            Body=output.getvalue()
        )

        # Generate presigned URL
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': input_file_key},
            ExpiresIn=604800
        )

        # Send success notification with stats
        message = (
            f'Stock data updated using Polygon.io.\n\n'
            f'Environment: {"TEST" if is_test else "PRODUCTION"}\n'
            f'Valid tickers processed: {len(valid_rows)}\n'
            f'Invalid tickers (N/A values): {len(invalid_tickers)}\n'
            f'Invalid tickers list: {", ".join(invalid_tickers)}\n\n'
            f'Download link (expires in 7 days):\n{presigned_url}'
        )

        sns.publish(
            TopicArn=os.getenv('SNS_TOPIC_ARN'),
            Subject=f'Stock Data Update Complete - Polygon.io',
            Message=message
        )

        return {
            'statusCode': 200,
            'body': {
                'message': f'Successfully updated stock data using Polygon.io',
                'environment': 'test' if is_test else 'production',
                'valid_tickers_count': len(valid_rows),
                'invalid_tickers_count': len(invalid_tickers),
                'invalid_tickers': invalid_tickers
            }
        }

    except Exception as e:
        print(f"Exception occurred: {e}")
        error_message = f'Error updating stock data using Polygon.io: {str(e)}'
        sns.publish(
            TopicArn=os.getenv('SNS_TOPIC_ARN'),
            Subject=f'Stock Data Update Error - Polygon.io',
            Message=error_message
        )
        return {
            'statusCode': 500,
            'body': error_message
        }