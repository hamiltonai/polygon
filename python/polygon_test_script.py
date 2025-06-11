import os
import csv
import requests
from datetime import datetime
import pytz
from dotenv import load_dotenv
import sys

# Load environment variables from .env if present
load_dotenv()

# Import the comprehensive data function from polygon_full_pull.py
from polygon_full_pull import get_polygon_comprehensive_data

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fetch comprehensive Polygon data for ticker(s) and save to CSV.')
    parser.add_argument('tickers', nargs='+', help='Ticker symbol(s), e.g. AAPL MSFT TSLA')
    parser.add_argument('--output', default=None, help='Output CSV file (default: auto-named with timestamp)')
    args = parser.parse_args()

    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        print('Error: POLYGON_API_KEY not set in environment or .env file.')
        sys.exit(1)

    tickers = args.tickers
    rows = []
    errors = []
    for ticker in tickers:
        print(f'Fetching data for {ticker}...')
        try:
            data = get_polygon_comprehensive_data(ticker, api_key)
            data['ticker'] = ticker
            cst = pytz.timezone('America/Chicago')
            now_cst = datetime.now(cst)
            data['retrieved_at'] = now_cst.strftime('%Y-%m-%d %H:%M:%S %Z')
            rows.append(data)
        except Exception as e:
            print(f'Error for {ticker}: {e}')
            errors.append({'ticker': ticker, 'error': str(e)})

    if not rows:
        print('No data retrieved.')
        sys.exit(1)

    # Determine output filename
    if args.output:
        out_csv = args.output
    else:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        out_csv = f'polygon_test_output_{ts}.csv'

    # Write to CSV
    with open(out_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=sorted(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f'Saved {len(rows)} rows to {out_csv}')

    if errors:
        print('Some tickers failed:')
        for err in errors:
            print(f"  {err['ticker']}: {err['error']}")

if __name__ == '__main__':
    main() 