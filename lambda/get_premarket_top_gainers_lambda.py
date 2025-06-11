"""
Lambda handler for get_premarket_top_gainers (self-contained)
Requirements:
- boto3
- pandas
- aiohttp
- polygon-api-client (or your RESTClient)
- python-dotenv (optional, for local testing)
- pytz

Environment variables required:
- POLYGON_API_KEY
- BUCKET_NAME
- SNS_TOPIC_ARN

If date_str is not provided in the event, the current date (America/Chicago, YYYYMMDD) will be used.
"""
import os
import csv
from polygon import RESTClient
from datetime import datetime
import pytz
import boto3
from firecrawl import FirecrawlApp
import re

def polygon_top_gainers():
    api_key = os.environ.get('POLYGON_API_KEY')
    if not api_key:
        print('POLYGON_API_KEY not set in environment')
        return []
    client = RESTClient(api_key)
    try:
        gainers = client.get_snapshot_direction("stocks", "gainers")
    except Exception as e:
        print(f"Error fetching premarket top gainers from Polygon: {e}")
        return []
    results = []
    for g in gainers:
        ticker = getattr(g, "ticker", None)
        # Polygon snapshot objects may not have a 'name' field; fallback to ticker if not present
        name = getattr(g, "name", None) or ticker
        if ticker:
            results.append([ticker, name, "polygon"])
    return results

def firecrawl_investing():
    api_key = os.getenv('FIRECRAWL_API_KEY')
    if not api_key:
        print('FIRECRAWL_API_KEY not set in environment')
        return []
    app = FirecrawlApp(api_key=api_key)
    investing_url = "https://www.investing.com/equities/pre-market"
    params = {'formats': ['markdown'], 'waitFor': 5000}
    try:
        data = app.scrape_url(investing_url, params=params)
        markdown_text = data['markdown']
        pattern = r"## Pre Market Top Gainers\n\n(\| Name \| Price \|\n\| --- \| --- \|\n(?:\| .+\n)+)"
        match = re.search(pattern, markdown_text)
        if match:
            pre_market_top_gainers_table = match.group(0)
            lines = pre_market_top_gainers_table.strip().split('\n')[3:]  # Skip headers
            cleaned_data = []
            for line in lines:
                match = re.search(r'\|\s*\[(.*?)\<br>\<br>(.*?)\]\((.*?)\)\s*\|', line)
                if match:
                    symbol = match.group(1).strip()
                    name = match.group(2).strip()
                    if symbol and name:
                        cleaned_data.append([symbol, name, "investing"])
            return cleaned_data
        print("No data found for Investing.com")
        return []
    except Exception as e:
        print(f"Error scraping Investing.com: {e}")
        return []

def lambda_handler(event, context):
    try:
        s3_bucket = os.environ.get('BUCKET_NAME')
        sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        # Use date_str from event if provided, else use current date in America/Chicago
        date_str = event.get('date_str')
        if not date_str:
            cst = pytz.timezone('America/Chicago')
            date_str = datetime.now(cst).strftime('%Y%m%d')
        if not s3_bucket:
            print('BUCKET_NAME not set in environment')
            return {'status': 'error', 'message': 'Missing BUCKET_NAME'}
        if not sns_topic_arn:
            print('SNS_TOPIC_ARN not set in environment')
            return {'status': 'error', 'message': 'Missing SNS_TOPIC_ARN'}
        # Get data from both sources
        investing_data = firecrawl_investing()
        polygon_data = polygon_top_gainers()
        all_data = investing_data + polygon_data
        # Remove duplicates by ticker+source (in case of overlap)
        seen = set()
        unique_data = []
        for row in all_data:
            key = (row[0], row[2])
            if key not in seen:
                unique_data.append(row)
                seen.add(key)
        # Write to CSV
        fieldnames = ["ticker", "name", "source"]
        filename = f"premarket_top_gainers_{date_str}.csv"
        local_path = f"/tmp/{filename}"
        with open(local_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(fieldnames)
            writer.writerows(unique_data)
        print(f"Premarket top gainers written to {local_path}")
        # Upload to S3
        s3_key = f"stock_data/{date_str}/{filename}"
        s3 = boto3.client('s3')
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
        print(f"Premarket top gainers uploaded to S3: {s3_key}")
        # Generate presigned URL
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': s3_bucket, 'Key': s3_key},
            ExpiresIn=604800  # 7 days
        )
        # Send SNS notification
        sns = boto3.client('sns')
        message = (
            f'Premarket Top Gainers updated using Polygon.io and Investing.com.\n\n'
            f'Date: {date_str}\n'
            f'Gainers processed: {len(unique_data)}\n\n'
            f'Download link (expires in 7 days):\n{presigned_url}'
        )
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f'Premarket Top Gainers Update Complete - Polygon.io + Investing.com',
            Message=message
        )
        return {
            'status': 'success',
            'date_str': date_str,
            's3_bucket': s3_bucket,
            's3_key': s3_key,
            'presigned_url': presigned_url,
            'gainers_count': len(unique_data)
        }
    except Exception as e:
        print('Error in get_premarket_top_gainers_lambda:', str(e))
        # Send SNS error notification
        try:
            sns = boto3.client('sns')
            sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
            if sns_topic_arn:
                error_message = f'Error updating premarket top gainers: {str(e)}'
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Subject=f'Premarket Top Gainers Update Error - Polygon.io + Investing.com',
                    Message=error_message
                )
        except Exception as sns_e:
            print(f"Failed to send SNS error notification: {sns_e}")
        return {'status': 'error', 'message': str(e)} 