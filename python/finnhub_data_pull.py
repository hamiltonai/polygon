import os
import csv
import requests
import boto3
from io import StringIO
from datetime import datetime
import json
import pytz

def get_finnhub_quote(symbol, api_key):
    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": symbol, "token": api_key}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def get_finnhub_market_cap_millions(symbol, api_key):
    url = "https://finnhub.io/api/v1/stock/metric"
    params = {"symbol": symbol, "metric": "all", "token": api_key}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        # Market cap is under data['metric']['marketCapitalization']
        return data.get('metric', {}).get('marketCapitalization')
    else:
        return None

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    bucket_name = os.getenv('BUCKET_NAME')
    lambda_client = boto3.client('lambda')

    FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
    input_date = event.get('override_date', datetime.now().strftime('%Y%m%d'))
    timing = 'morning'  # Only morning run for now
    input_file_key = f'stock_data/{input_date}/stock_data_{input_date}.csv'

    # Get timestamp rounded to the nearest minute in CST (America/Chicago), only time (HHMM)
    cst = pytz.timezone('America/Chicago')
    now_utc = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=pytz.utc)
    now_cst = now_utc.astimezone(cst)
    time_str = now_cst.strftime('%H%M')

    # Dynamic column names (only time in header)
    price_col = f'price_{time_str}'
    change_col = f'change_{time_str}'
    change_pct_col = f'change_pct_{time_str}'
    open_col = 'open_price'  # Always use this name, no timestamp
    open_change_col = 'open_change'
    open_change_pct_col = 'open_change_pct'
    prev_close_col = 'previous_close'  # For appending if not present

    try:
        csv_obj = s3.get_object(Bucket=bucket_name, Key=input_file_key)
        file_content = csv_obj['Body'].read().decode('utf-8')
        csv_file = StringIO(file_content)
        csv_reader = csv.DictReader(csv_file)
        rows = list(csv_reader)

        # Determine fieldnames: preserve all original, add new columns if not present
        fieldnames = list(csv_reader.fieldnames) if csv_reader.fieldnames else []
        # Insert previous_close after source if not present
        if prev_close_col not in fieldnames:
            if 'source' in fieldnames:
                idx = fieldnames.index('source') + 1
                fieldnames.insert(idx, prev_close_col)
            else:
                fieldnames.append(prev_close_col)
        # Add open_price, open_change, open_change_pct after previous_close
        for col in [open_col, open_change_col, open_change_pct_col]:
            if col not in fieldnames:
                idx = fieldnames.index(prev_close_col) + 1
                fieldnames.insert(idx, col)
        # Add new dated columns if not present
        for col in [price_col, change_col, change_pct_col]:
            if col not in fieldnames:
                fieldnames.append(col)
        # Insert market_cap_millions after ticker if not present
        if 'market_cap_millions' not in fieldnames:
            if 'ticker' in fieldnames:
                idx = fieldnames.index('ticker') + 1
                fieldnames.insert(idx, 'market_cap_millions')
            else:
                fieldnames.insert(0, 'market_cap_millions')

        updated_rows = []
        for row in rows:
            symbol = row.get('ticker')
            quote = get_finnhub_quote(symbol, FINNHUB_API_KEY)
            market_cap_millions = get_finnhub_market_cap_millions(symbol, FINNHUB_API_KEY)
            row['market_cap_millions'] = f"{market_cap_millions:.2f}" if market_cap_millions is not None else 'N/A'
            # Only update open price/change if the column is missing or empty for this row
            open_col_exists = open_col in row and row[open_col] not in [None, '', 'N/A']
            open_change_exists = open_change_col in row and row[open_change_col] not in [None, '', 'N/A']
            open_change_pct_exists = open_change_pct_col in row and row[open_change_pct_col] not in [None, '', 'N/A']
            if quote and 'c' in quote and 'pc' in quote:
                price = quote['c']
                prev_close = quote['pc']
                open_price = quote.get('o')
                if prev_close and prev_close != 0:
                    change = price - prev_close
                    change_pct = (change / prev_close) * 100
                    row[prev_close_col] = f"{prev_close:.3f}"
                    row[price_col] = f"{price:.3f}"
                    row[change_col] = f"{change:+.3f}"
                    row[change_pct_col] = f"{change_pct:+.2f}%"
                else:
                    row[prev_close_col] = 'N/A'
                    row[price_col] = 'N/A'
                    row[change_col] = 'N/A'
                    row[change_pct_col] = 'N/A'
                # Only set open price if not already present
                if not open_col_exists:
                    if open_price is not None:
                        row[open_col] = f"{open_price:.3f}"
                    else:
                        row[open_col] = 'N/A'
                # Only set open_change and open_change_pct if not already present and both open and prev_close are valid
                if not open_change_exists or not open_change_pct_exists:
                    try:
                        open_val = float(row[open_col]) if row.get(open_col) not in [None, '', 'N/A'] else None
                        prev_close_val = float(row[prev_close_col]) if row.get(prev_close_col) not in [None, '', 'N/A'] else None
                        if open_val is not None and prev_close_val is not None and prev_close_val != 0:
                            open_change = open_val - prev_close_val
                            open_change_pct = (open_change / prev_close_val) * 100
                            if not open_change_exists:
                                row[open_change_col] = f"{open_change:+.3f}"
                            if not open_change_pct_exists:
                                row[open_change_pct_col] = f"{open_change_pct:+.2f}%"
                        else:
                            if not open_change_exists:
                                row[open_change_col] = 'N/A'
                            if not open_change_pct_exists:
                                row[open_change_pct_col] = 'N/A'
                    except Exception:
                        if not open_change_exists:
                            row[open_change_col] = 'N/A'
                        if not open_change_pct_exists:
                            row[open_change_pct_col] = 'N/A'
            else:
                row[prev_close_col] = 'N/A'
                row[price_col] = 'N/A'
                row[change_col] = 'N/A'
                row[change_pct_col] = 'N/A'
                if not open_col_exists:
                    row[open_col] = 'N/A'
                if not open_change_exists:
                    row[open_change_col] = 'N/A'
                if not open_change_pct_exists:
                    row[open_change_pct_col] = 'N/A'
            updated_rows.append(row)

        # After all fieldnames logic, ensure all keys in all rows are in fieldnames
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        # Reorder columns: all non-dated columns first, then all dated columns (ending with price/change columns)
        # Dated columns are those that match *_HHMM
        ordered_cols = []
        for col in fieldnames:
            if col in ['ticker', 'name', 'source', prev_close_col, open_col, open_change_col, open_change_pct_col]:
                if col not in ordered_cols:
                    ordered_cols.append(col)
        # Now add all other non-dated columns
        for col in fieldnames:
            if col not in ordered_cols and not (col.startswith('price_') or col.startswith('change_') or col.startswith('change_pct_')):
                ordered_cols.append(col)
        # Now add all dated columns in the order they appear
        for col in fieldnames:
            if (col.startswith('price_') or col.startswith('change_') or col.startswith('change_pct_')) and len(col.split('_')) == 2 and len(col.split('_')[1]) == 4:
                if col not in ordered_cols:
                    ordered_cols.append(col)
        # Ensure all columns from fieldnames are in ordered_cols
        for col in fieldnames:
            if col not in ordered_cols:
                ordered_cols.append(col)

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=ordered_cols)
        writer.writeheader()
        for row in updated_rows:
            writer.writerow(row)

        # Overwrite the same file in S3
        s3.put_object(
            Bucket=bucket_name,
            Key=input_file_key,
            Body=output.getvalue()
        )

        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': input_file_key},
            ExpiresIn=604800
        )

        message = (
            f'Stock data updated and appended using Finnhub.\n\n'
            f'Download link (expires in 7 days):\n{presigned_url}'
        )

        sns.publish(
            TopicArn=os.getenv('SNS_TOPIC_ARN'),
            Subject=f'Stock Data Update Complete - Finnhub',
            Message=message
        )

        return {
            'statusCode': 200,
            'body': {'message': f'Successfully updated and appended stock data using Finnhub'}
        }

    except Exception as e:
        error_message = f'Error updating/appending stock data using Finnhub: {str(e)}'
        sns.publish(
            TopicArn=os.getenv('SNS_TOPIC_ARN'),
            Subject=f'Stock Data Update Error - Finnhub',
            Message=error_message
        )
        return {
            'statusCode': 500,
            'body': error_message
        }