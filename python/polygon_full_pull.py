import os
import csv
import requests
import boto3
from io import StringIO
from datetime import datetime
import json
import pytz
import concurrent.futures
from threading import Lock

# Add dotenv support for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # If dotenv is not installed, skip (for AWS Lambda)

# Thread-safe counter for progress tracking
progress_lock = Lock()
processed_count = 0

def get_polygon_comprehensive_data(symbol, api_key):
    """
    Get comprehensive stock data from Polygon.io including:
    - Previous day OHLCV
    - Current/last trade price
    - Company details (market cap)
    - Real-time quote (bid/ask)
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    data = {'symbol': symbol}
    
    # 1. Previous day aggregates
    prev_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev"
    try:
        prev_response = requests.get(prev_url, headers=headers, timeout=10)
        if prev_response.status_code == 200:
            prev_data = prev_response.json()
            if prev_data.get('status') == 'OK' and prev_data.get('results'):
                result = prev_data['results'][0]
                data.update({
                    'prev_open': result.get('o'),
                    'prev_close': result.get('c'),
                    'prev_high': result.get('h'),
                    'prev_low': result.get('l'),
                    'prev_volume': result.get('v'),
                    'prev_date': result.get('t')  # timestamp
                })
    except Exception as e:
        print(f"Error fetching previous day data for {symbol}: {e}")
    
    # 2. Last trade (current price)
    trade_url = f"https://api.polygon.io/v2/last/trade/{symbol}"
    try:
        trade_response = requests.get(trade_url, headers=headers, timeout=10)
        if trade_response.status_code == 200:
            trade_data = trade_response.json()
            if trade_data.get('status') == 'OK' and trade_data.get('results'):
                result = trade_data['results']
                data.update({
                    'current_price': result.get('p'),
                    'current_size': result.get('s'),
                    'current_timestamp': result.get('t')
                })
    except Exception as e:
        print(f"Error fetching current trade data for {symbol}: {e}")
    
    # 3. Company details (market cap)
    company_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
    try:
        company_response = requests.get(company_url, headers=headers, timeout=10)
        if company_response.status_code == 200:
            company_data = company_response.json()
            if company_data.get('status') == 'OK' and company_data.get('results'):
                result = company_data['results']
                data.update({
                    'market_cap': result.get('market_cap'),
                    'company_name': result.get('name'),
                    'primary_exchange': result.get('primary_exchange'),
                    'currency': result.get('currency_name'),
                    'share_class_shares_outstanding': result.get('share_class_shares_outstanding')
                })
    except Exception as e:
        print(f"Error fetching company data for {symbol}: {e}")
    
    # 4. Real-time quote (bid/ask)
    quote_url = f"https://api.polygon.io/v2/last/nbbo/{symbol}"
    try:
        quote_response = requests.get(quote_url, headers=headers, timeout=10)
        if quote_response.status_code == 200:
            quote_data = quote_response.json()
            if quote_data.get('status') == 'OK' and quote_data.get('results'):
                result = quote_data['results']
                data.update({
                    'bid_price': result.get('P'),
                    'bid_size': result.get('S'),
                    'ask_price': result.get('p'),
                    'ask_size': result.get('s'),
                    'quote_timestamp': result.get('t')
                })
    except Exception as e:
        print(f"Error fetching quote data for {symbol}: {e}")
    
    return data

def process_single_ticker(symbol, api_key):
    """Process a single ticker and return formatted row data"""
    global processed_count
    
    try:
        # Get comprehensive data from Polygon
        polygon_data = get_polygon_comprehensive_data(symbol, api_key)
        
        # Calculate derived metrics
        row = {'ticker': symbol}
        
        # Basic company info
        row['company_name'] = polygon_data.get('company_name', 'N/A')
        row['primary_exchange'] = polygon_data.get('primary_exchange', 'N/A')
        
        # Market cap (convert to millions)
        market_cap = polygon_data.get('market_cap')
        if market_cap is not None:
            row['market_cap_millions'] = f"{market_cap / 1_000_000:.2f}"
        else:
            row['market_cap_millions'] = 'N/A'
        
        # Previous day data
        prev_open = polygon_data.get('prev_open')
        prev_close = polygon_data.get('prev_close')
        prev_high = polygon_data.get('prev_high')
        prev_low = polygon_data.get('prev_low')
        prev_volume = polygon_data.get('prev_volume')
        
        row['previous_close'] = f"{prev_close:.3f}" if prev_close is not None else 'N/A'
        row['previous_open'] = f"{prev_open:.3f}" if prev_open is not None else 'N/A'
        row['previous_high'] = f"{prev_high:.3f}" if prev_high is not None else 'N/A'
        row['previous_low'] = f"{prev_low:.3f}" if prev_low is not None else 'N/A'
        row['previous_volume'] = f"{int(prev_volume)}" if prev_volume is not None else 'N/A'
        
        # Current price data
        current_price = polygon_data.get('current_price')
        if current_price is not None and prev_close is not None and prev_close != 0:
            # Current price vs previous close
            price_change = current_price - prev_close
            price_change_pct = (price_change / prev_close) * 100
            
            row['current_price'] = f"{current_price:.3f}"
            row['price_change'] = f"{price_change:+.3f}"
            row['price_change_pct'] = f"{price_change_pct:+.2f}%"
        else:
            row['current_price'] = 'N/A'
            row['price_change'] = 'N/A'
            row['price_change_pct'] = 'N/A'
        
        # Open vs previous close (gap)
        if prev_open is not None and prev_close is not None and prev_close != 0:
            open_change = prev_open - prev_close
            open_change_pct = (open_change / prev_close) * 100
            row['open_gap'] = f"{open_change:+.3f}"
            row['open_gap_pct'] = f"{open_change_pct:+.2f}%"
        else:
            row['open_gap'] = 'N/A'
            row['open_gap_pct'] = 'N/A'
        
        # Bid/Ask data
        bid_price = polygon_data.get('bid_price')
        ask_price = polygon_data.get('ask_price')
        
        row['bid_price'] = f"{bid_price:.3f}" if bid_price is not None else 'N/A'
        row['ask_price'] = f"{ask_price:.3f}" if ask_price is not None else 'N/A'
        row['bid_size'] = f"{polygon_data.get('bid_size', 'N/A')}"
        row['ask_size'] = f"{polygon_data.get('ask_size', 'N/A')}"
        
        # Bid-ask spread
        if bid_price is not None and ask_price is not None:
            spread = ask_price - bid_price
            spread_pct = (spread / ((bid_price + ask_price) / 2)) * 100 if (bid_price + ask_price) > 0 else 0
            row['bid_ask_spread'] = f"{spread:.3f}"
            row['bid_ask_spread_pct'] = f"{spread_pct:.2f}%"
        else:
            row['bid_ask_spread'] = 'N/A'
            row['bid_ask_spread_pct'] = 'N/A'
        
        # Add timestamp
        cst = pytz.timezone('America/Chicago')
        now_cst = datetime.now(cst)
        row['last_updated'] = now_cst.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        # Thread-safe progress update
        with progress_lock:
            processed_count += 1
            if processed_count % 50 == 0:  # Log every 50 processed
                print(f"Processed {processed_count} tickers...")
        
        return row, None  # (row_data, error)
        
    except Exception as e:
        error_msg = f"Error processing {symbol}: {str(e)}"
        print(error_msg)
        return None, error_msg

def get_latest_screener_key_from_s3(s3, bucket_name, prefix):
    """Get the latest screener file from S3"""
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    screener_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    if not screener_files:
        raise FileNotFoundError('No nasdaq_screener_*.csv file found in S3 stock_screener directory')
    return max(screener_files)

def get_tickers_from_s3_screener(s3, bucket_name, screener_key):
    """Extract ticker symbols from screener CSV"""
    csv_obj = s3.get_object(Bucket=bucket_name, Key=screener_key)
    file_content = csv_obj['Body'].read().decode('utf-8')
    reader = csv.DictReader(StringIO(file_content))
    return [row['Symbol'] for row in reader if row.get('Symbol')]

def lambda_handler(event, context):
    global processed_count
    processed_count = 0  # Reset counter
    
    # Initialize AWS clients
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    
    # Get environment variables
    bucket_name = os.getenv('BUCKET_NAME')
    polygon_api_key = os.getenv('POLYGON_API_KEY')
    sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
    
    # Generate file paths
    input_date = event.get('override_date', datetime.now().strftime('%Y%m%d'))
    output_file_key = f'stock_data/{input_date}/comprehensive_stock_data_{input_date}.csv'
    
    try:
        # Determine tickers to process
        is_test = event.get('environment') == 'test'
        max_workers = event.get('max_workers', 10)  # Allow configurable parallelism
        
        if is_test:
            tickers = ['AAPL', 'MSFT', 'GOOGL']  # Small test set
            print(f"Running in test mode - processing {len(tickers)} tickers")
        else:
            # Get tickers from screener file
            screener_prefix = 'stock_data/stock_screener/nasdaq_screener_'
            screener_key = get_latest_screener_key_from_s3(s3, bucket_name, screener_prefix)
            tickers = get_tickers_from_s3_screener(s3, bucket_name, screener_key)
            print(f"Processing {len(tickers)} tickers from screener")
        
        # Process tickers in parallel (since you have unlimited API calls)
        valid_rows = []
        errors = []
        
        print(f"Starting parallel processing with {max_workers} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_ticker = {
                executor.submit(process_single_ticker, ticker, polygon_api_key): ticker
                for ticker in tickers
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    row_data, error = future.result()
                    if row_data:
                        valid_rows.append(row_data)
                    if error:
                        errors.append(error)
                except Exception as e:
                    error_msg = f"Exception processing {ticker}: {e}"
                    print(error_msg)
                    errors.append(error_msg)
        
        print(f"Completed processing. Valid rows: {len(valid_rows)}, Errors: {len(errors)}")
        
        # Define column order for CSV output
        fieldnames = [
            'ticker', 'company_name', 'primary_exchange', 'market_cap_millions',
            'previous_close', 'previous_open', 'previous_high', 'previous_low', 'previous_volume',
            'current_price', 'price_change', 'price_change_pct',
            'open_gap', 'open_gap_pct',
            'bid_price', 'ask_price', 'bid_size', 'ask_size',
            'bid_ask_spread', 'bid_ask_spread_pct',
            'last_updated'
        ]
        
        # Create CSV output
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by ticker for consistent output
        valid_rows.sort(key=lambda x: x.get('ticker', ''))
        for row in valid_rows:
            writer.writerow(row)
        
        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=output_file_key,
            Body=output.getvalue(),
            ContentType='text/csv'
        )
        
        # Generate presigned URL
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': output_file_key},
            ExpiresIn=604800  # 7 days
        )
        
        # Send success notification
        message = (
            f'Comprehensive stock data updated using Polygon.io\n\n'
            f'Environment: {"TEST" if is_test else "PRODUCTION"}\n'
            f'Total tickers processed: {len(tickers)}\n'
            f'Successful records: {len(valid_rows)}\n'
            f'Errors encountered: {len(errors)}\n'
            f'Success rate: {(len(valid_rows)/len(tickers)*100):.1f}%\n\n'
            f'Data includes:\n'
            f'• Previous day OHLCV data\n'
            f'• Real-time pricing and changes\n'
            f'• Market capitalization\n'
            f'• Bid/ask spreads\n'
            f'• Gap analysis (open vs prev close)\n\n'
            f'Download link (expires in 7 days):\n{presigned_url}'
        )
        
        if errors:
            message += f'\n\nFirst 5 errors:\n' + '\n'.join(errors[:5])
        
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject='Comprehensive Stock Data Update Complete - Polygon.io',
            Message=message
        )
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Successfully updated comprehensive stock data using Polygon.io',
                'environment': 'test' if is_test else 'production',
                'total_tickers': len(tickers),
                'successful_records': len(valid_rows),
                'errors': len(errors),
                'success_rate': f"{(len(valid_rows)/len(tickers)*100):.1f}%",
                'output_file': output_file_key
            }
        }
        
    except Exception as e:
        error_message = f'Error updating comprehensive stock data: {str(e)}'
        print(f"Lambda exception: {error_message}")
        
        # Send error notification
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject='Stock Data Update Error - Polygon.io',
            Message=error_message
        )
        
        return {
            'statusCode': 500,
            'body': {'error': error_message}
        }