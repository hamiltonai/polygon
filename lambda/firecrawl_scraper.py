from firecrawl import FirecrawlApp
import re
from io import StringIO
from datetime import datetime
import json
import boto3
import csv
import os
import requests
from polygon import RESTClient
from polygon.rest.models import TickerSnapshot
import asyncio
from firecrawl import AsyncFirecrawlApp

# Remove hardcoded API keys and load from environment variables
FIRECRAWL_API_KEY = os.getenv('FIRECRAWL_API_KEY')
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

def scrape(event):
    """
    Main scraping function that coordinates all data sources.
    """
    app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
    extracted_data = []

    sources = {
        "investing": scrape_investing,
        # "nasdaq": scrape_nasdaq_most_advanced,
        # "tip_ranks": scrape_tip_ranks,
        "polygon": lambda app: polygon_top_gainers_api(POLYGON_API_KEY)
    }

    source = event.get('source', 'combine')

    if source == 'combine':
        for scraper in sources.values():
            extracted_data.extend(scraper(app))
    elif source in sources:
        extracted_data.extend(sources[source](app))

    if not extracted_data:
        print("No data extracted from any source.")
        return []

    print(f"Total stocks extracted: {len(extracted_data)}")
    return extracted_data

def get_scrape_params():
    return {
        'formats': ['markdown'],
        'waitFor': 5000
    }

def scrape_trading_view(app):
    """
    Scrapes data from TradingView pre-market gainers page and extracts ticker symbols and company names.
    """
    trading_view_url = "https://www.tradingview.com/markets/stocks-usa/market-movers-pre-market-gainers/"
    data = app.scrape_url(trading_view_url, **get_scrape_params())
    markdown_text = data.markdown
    
    # Regular expression to extract ticker symbols and names
    pattern = r'\[([A-Z]+)\]\(/symbols/[A-Z-]+/.*?"([^"]+)"\)'
    matches = re.findall(pattern, markdown_text)
    
    cleaned_data = []
    for symbol, name in matches:
        # Remove ticker and dash from the name
        cleaned_name = name.split(" − ", 1)[-1]  # Splits on the dash and takes the part after it
        cleaned_data.append([symbol, cleaned_name, "trading_view"])
    
    if cleaned_data:
        return cleaned_data
    else:
        return log_not_found("TradingView")

def scrape_tip_ranks(app):
    """
    Scrapes data from TipRanks pre-market gainers page and extracts ticker symbols and company names.
    """           
    tip_rank_url = "https://www.tipranks.com/screener/pre-market-gainers"
    data = app.scrape_url(tip_rank_url, **get_scrape_params())
    markdown_text = data.markdown 
    
    # Parse the markdown table directly
    pattern = r'\| \[([A-Z]+)\][^<]*<br>([^|]+)'
    matches = re.findall(pattern, markdown_text)
    
    cleaned_data = []
    for symbol, name in matches:
        name = name.strip()
        if name and symbol:
            print(f"Found TipRanks stock: {symbol} - {name}")
            cleaned_data.append([symbol, name, "tip_ranks"])
    
    if cleaned_data:
        print(f"Successfully extracted {len(cleaned_data)} stocks from TipRanks")
        return cleaned_data
    
    print("No data found in TipRanks scraping")
    return []

def scrape_investing(app=None):
    """
    Scrapes data from Investing.com pre-market gainers page using AsyncFirecrawlApp.
    """
    async def async_scrape():
        async_app = AsyncFirecrawlApp(api_key=FIRECRAWL_API_KEY)
        response = await async_app.scrape_url(
            url='https://www.investing.com/equities/pre-market',
            formats=['markdown'],
            only_main_content=True
        )
        return response.markdown

    markdown_text = asyncio.run(async_scrape())
    # Parse the markdown for the pre-market top gainers table
    pattern = r"## Pre Market Top Gainers\n\n(\| Name \| Price \|[^\n]*\n\| --- \| --- \|[^\n]*\n(?:\| .+\n)+)"
    match = re.search(pattern, markdown_text)
    if match:
        pre_market_top_gainers_table = match.group(1)
        lines = pre_market_top_gainers_table.strip().split('\n')[2:]  # Skip headers
        cleaned_data = []
        for line in lines:
            # Try to extract symbol and name from the line
            # Example: | [AAPL]<br><br>Apple Inc. | 123.45 |
            match = re.search(r'\|\s*\[(.*?)<br><br>(.*?)\]\([^)]*\)\s*\|', line)
            if match:
                symbol = match.group(1).strip()
                name = match.group(2).strip()
                if symbol and name:
                    cleaned_data.append([symbol, name, "investing gainers"])
        if cleaned_data:
            print(f"Successfully extracted {len(cleaned_data)} stocks from Investing.com")
            return cleaned_data
    print("No data found for Investing.com")
    return []

def scrape_investing_losers(app):
    investing_url = "https://www.investing.com/equities/pre-market"
    data = app.scrape_url(investing_url, **get_scrape_params())
    markdown_text = data.markdown
    pattern = r"## Pre Market Top Losers\n\n(\| Name \| Price \|\n\| --- \| --- \|\n(?:\| .+\n)+)"
    match = re.search(pattern, markdown_text)
    return parse_investing_losers_data(match) if match else log_not_found("Investing.com")

def scrape_nasdaq_most_advanced(app):
    """
    Scrapes the Most Advanced table from NASDAQ's pre-market page using Firecrawl.
    Returns a list of [symbol, name] entries.
    """
    url = "https://www.nasdaq.com/market-activity/pre-market"
    
    params = {
        'formats': ['markdown', 'html'],
        'actions': [
            {
                "type": "wait",
                "milliseconds": 10000  # Wait for initial page load
            },
            {
                "type": "click",
                "selector": "button.table-tabs__tab[data-value='MostAdvanced']"
            },
            {
                "type": "wait",
                "milliseconds": 5000  # Wait for table to update
            },
            {
                "type": "scrape"  # Explicitly scrape after interaction
            }
        ]
    }
    
    try:
        print("Starting NASDAQ scrape...")
        data = app.scrape_url(url, **params)
        markdown_text = data.markdown
        
        # Process the table data
        cleaned_data = []
        lines = markdown_text.split('\n')
        
        for line in lines:
            if '|' not in line or '---' in line:  # Skip header/separator rows
                continue
                
            cells = [cell.strip() for cell in line.split('|')]
            if len(cells) > 3:  # Ensure we have enough cells
                # Extract symbol from cell 1
                symbol_match = re.search(r'\[([A-Z]+)\]', cells[1])
                # Extract name from cell 2
                name_match = re.search(r'\[([^]]+)\]', cells[2])
                
                if symbol_match and name_match:
                    symbol = symbol_match.group(1)
                    name = name_match.group(1)
                    print(f"Found NASDAQ stock: {symbol} - {name}")
                    cleaned_data.append([symbol, name, "nasdaq"])
        
        if cleaned_data:
            print(f"Successfully extracted {len(cleaned_data)} stocks from NASDAQ")
            return cleaned_data
            
        print("No stocks found in NASDAQ data")
        return []
            
    except Exception as e:
        print(f"Error scraping NASDAQ page: {str(e)}")
        return []

def parse_investing_data(match):
    pre_market_top_gainers_table = match.group(0)
    lines = pre_market_top_gainers_table.strip().split('\n')[3:]
    return [
        [match.group(1), match.group(2), "investing gainers"]
        for line in lines
        if (match := re.search(r'\|\s*\[(.*?)\\<br>\\<br>(.*?)\]\((.*?)\)\s*\|\s*([\d.]+)\+([\d.]+)\+([\d.]+%)\s*\|', line))
    ]

def parse_investing_losers_data(match):
    pre_market_top_losers_table = match.group(0)
    lines = pre_market_top_losers_table.strip().split('\n')
    parse_data = []
   
    for line in lines[2:]:  # Start from index 2 to skip header rows
        # Updated regex to handle escaped characters
        match = re.search(r'\|\s*\[(.*?)\\<br>\\<br>(.*?)\]\([^)]*\)\s*\|\s*([^|]+)\s*\|', line)
        if match:
            symbol = match.group(1).strip()
            company = match.group(2).strip()
            price_change = match.group(3).strip()
            parse_data.append([symbol, company, "investing losers"])
    return parse_data

def extract_stock_tickers(markdown_text):
    """
    Extracts stock tickers from the given markdown text using regular expressions.
    """
    ticker_pattern = r"\[([A-Z]+)\+"  # Pattern to match tickers in the markdown text
    tickers = re.findall(ticker_pattern, markdown_text)
    return list(set(tickers))  # Remove duplicates if necessary

def log_not_found(source):
    """
    Logs when no data is found from the provided source.
    """
    print(f"No data found for {source}.")
    return []

def save_csv_to_file(csv_data, event):
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    file_path = os.path.join(desktop_path, f'Pre-Market Top Gainers CSV - {event.get("source")}.csv')
    with open(file_path, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['ticker', 'Name', 'Source'])
        csv_writer.writerows(csv_data)

def send_sns_notification(message, source, bucket_name, file_name):
    s3 = boto3.client('s3')  # Initialize s3 client
    presigned_url = s3.generate_presigned_url(
        'get_object',
        Params={
            'Bucket': bucket_name,
            'Key': file_name
        },
        ExpiresIn=604800  # 7 days in seconds
    )
    sns = boto3.client('sns')
    topic_arn = os.getenv('SNS_TOPIC_ARN')
    sns.publish(
        TopicArn=topic_arn,
        Message=f"{message}\n\nDownload link: {presigned_url}",
        Subject="Pre-Market Top Gainers CSV"
    )

def handle_exception(e):
    sns = boto3.client('sns')
    topic_arn = os.getenv('SNS_TOPIC_ARN')
    error_message = f"Error in pre-market scraper lambda: {str(e)}"
    print(error_message)
    # sns.publish(TopicArn=topic_arn, Message=error_message, Subject='Pre-Market Scraper Error')

def create_response(status_code, message):
    return {
        'statusCode': status_code,
        'body': json.dumps(message)
    }

def save_to_s3(data, event):
    """
    Saves the scraped data to S3 and returns bucket and file information.
    """
    if not data:
        print("No data to save to S3")
        return None, None
        
    print(f"Preparing to save {len(data)} records to S3...")
    
    s3 = boto3.client('s3', region_name='us-east-1')
    bucket_name = 'hamiltonai.com'
    date = datetime.now().strftime('%Y%m%d')
    file_name = f'stock_data/{date}/stock_data_{date}.csv'
    
    # Create CSV content with proper line endings
    csv_content = convert_to_csv(data)
    print(f"CSV content created, size: {len(csv_content)}")
    
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=csv_content.encode('utf-8')  # Explicitly encode as UTF-8
        )
        print(f"Successfully saved to S3: s3://{bucket_name}/{file_name}")
        
        # Send notification only if we actually saved data
        if data:
            send_sns_notification(csv_content, event.get('source', 'unknown'), bucket_name, file_name)
            
        return bucket_name, file_name
    except Exception as e:
        print(f"Error saving to S3: {str(e)}")
        raise

def convert_to_csv(data):
    """
    Converts the data to CSV format with proper line endings.
    """
    output = StringIO()
    writer = csv.writer(output, lineterminator='\n')  # Explicitly set line ending
    writer.writerow(['ticker', 'name', 'source'])  # Header
    writer.writerows(data)
    return output.getvalue()

def polygon_top_gainers_api(api_key):
    """
    Calls the Polygon.io API to get pre-market top gainers and returns a list of [ticker, name, 'polygon'].
    """
    
    if not api_key:
        print("POLYGON_API_KEY environment variable not set.")
        return []
    try:
        client = RESTClient(api_key)
        tickers = client.get_snapshot_direction("stocks", direction="gainers")
        cleaned_data = []
        for item in tickers:
            if isinstance(item, TickerSnapshot):
                ticker = item.ticker
                name = getattr(item, 'name', None) or ''
                if ticker:
                    cleaned_data.append([ticker, name, 'polygon'])
        print(f"Successfully extracted {len(cleaned_data)} stocks from Polygon.io")
        return cleaned_data
    except Exception as e:
        print(f"Error fetching Polygon.io data: {str(e)}")
        return []

def lambda_handler(event, context):
    """
    Main handler function with improved error handling and logging.
    """
    try:
        print("Starting lambda handler...")
        data = scrape(event)
        
        if not data:
            print("No data was scraped")
            return create_response(200, "No data to save")
            
        print(f"Scraped {len(data)} records")
        bucket_name, file_name = save_to_s3(data, event)
        
        if bucket_name and file_name:
            return create_response(200, f'Successfully saved data to s3')
        else:
            return create_response(200, "No data was saved to S3")
            
    except Exception as e:
        print(f"Error in lambda handler: {str(e)}")
        handle_exception(e)
        return create_response(500, f'Error: {str(e)}')

# lambda_handler(event={"source": "combine"}, context=None)