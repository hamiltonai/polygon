import os
import csv
import json
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
import boto3
from io import StringIO
import logging
from typing import Dict, List, Set
import time
import aiohttp
import numpy as np

import websockets
from polygon import RESTClient

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env file loaded successfully")
except ImportError:
    print("⚠️  python-dotenv not installed - using system environment variables")

# Configuration
AWS_S3_ENABLED = True # Toggle S3 upload
S3_BUCKET = os.getenv('BUCKET_NAME')
S3_PREFIX = "stock_data/real-time-monitor/"
POLL_INTERVAL = 60  # seconds
FILTER_START_DELAY = 420  # 7 minutes (420 seconds)
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NASDAQMonitor:
    def __init__(self):
        self.rest_client = RESTClient(POLYGON_API_KEY)
        self.stocks_data = defaultdict(dict)  # Store latest data for each stock
        self.nasdaq_symbols = set()
        self.qualified_symbols = set()
        self.start_time = None
        self.filter_enabled = False
        self.running = True
        self.cst = pytz.timezone('America/Chicago')
        self.data_lock = asyncio.Lock()
        
        # S3 client
        if AWS_S3_ENABLED:
            self.s3_client = boto3.client('s3')
        
        # File paths
        self.date_str = datetime.now(self.cst).strftime('%Y%m%d')
        self.start_time_str = datetime.now(self.cst).strftime('%H%M')
        self.raw_file = f'nasdaq_monitor_raw_{self.date_str}_{self.start_time_str}.csv'
        self.filtered_file = f'nasdaq_monitor_filtered_{self.date_str}_{self.start_time_str}.csv'
        
        # CSV headers
        self.headers = [
            'timestamp', 'symbol', 'market_cap_millions', 'previous_close', 'open',
            'current_price', 'bid', 'ask', 'volume', 'day_high', 'day_low',
            'change_pct', 'change_from_open_pct', 'meets_criteria'
        ]
        
        # Initialize CSV files
        self._initialize_csv_files()
    
    def _initialize_csv_files(self):
        """Create CSV files with headers"""
        with open(self.raw_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writeheader()
        logger.info(f"Created raw data file: {self.raw_file}")
    
    async def fetch_nasdaq_symbols(self):
        """Fetch all NASDAQ symbols from Polygon using efficient pagination"""
        logger.info("Fetching NASDAQ symbols...")
        symbols = []
        
        try:
            # Use pagination efficiently
            next_url = None
            page_count = 0
            
            while True:
                page_count += 1
                if next_url:
                    # Parse the cursor from next_url
                    tickers_response = self.rest_client.list_tickers(
                        market="stocks",
                        exchange="XNAS",
                        active=True,
                        limit=1000,
                        cursor=next_url.split('cursor=')[1] if 'cursor=' in next_url else None
                    )
                else:
                    tickers_response = self.rest_client.list_tickers(
                        market="stocks",
                        exchange="XNAS",
                        active=True,
                        limit=1000
                    )
                
                # Process the current page
                page_symbols = []
                for ticker in tickers_response:
                    page_symbols.append(ticker.ticker)
                
                symbols.extend(page_symbols)
                logger.info(f"Page {page_count}: Found {len(page_symbols)} symbols, total: {len(symbols)}")
                
                # Check if there's a next page
                if hasattr(tickers_response, 'next_url') and tickers_response.next_url:
                    next_url = tickers_response.next_url
                else:
                    break
                
                # Safety limit
                if len(symbols) >= 10000:
                    logger.warning(f"Reached safety limit of 10000 symbols")
                    break
            
            # LIMIT TO FIRST 100 SYMBOLS FOR TESTING
            # symbols = symbols[:100]
            self.nasdaq_symbols = set(symbols)
            logger.info(f"Using first {len(self.nasdaq_symbols)} NASDAQ symbols for testing: {list(self.nasdaq_symbols)[:5]} ...")
            
            # Fetch initial data using efficient snapshot API
            await self.fetch_initial_data()
            
        except Exception as e:
            logger.error(f"Error fetching NASDAQ symbols: {e}")
            # Fallback to a small test set
            self.nasdaq_symbols = {'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'AMD', 'NFLX', 'TSLA'}
            logger.info(f"Using test set of {len(self.nasdaq_symbols)} symbols")
    
    async def fetch_initial_data(self):
        """Fetch initial data (market cap, open, previous close) for all symbols using minimal API calls"""
        logger.info("Fetching initial data for all symbols...")
        
        # Use snapshot endpoint for bulk data - much more efficient
        try:
            # Get all snapshots in one call
            snapshots = self.rest_client.get_snapshot_all("stocks")
            
            for snapshot in snapshots:
                symbol = snapshot.ticker
                if symbol in self.nasdaq_symbols:
                    async with self.data_lock:
                        self.stocks_data[symbol].update({
                            'market_cap_millions': getattr(snapshot, 'market_cap', 0) / 1_000_000 if hasattr(snapshot, 'market_cap') else 0,
                            'previous_close': snapshot.prev_day.close if snapshot.prev_day else 0,
                            'open': snapshot.day.open if snapshot.day else 0,
                            'volume': snapshot.day.volume if snapshot.day else 0,
                            'day_high': snapshot.day.high if snapshot.day else 0,
                            'day_low': snapshot.day.low if snapshot.day else float('inf'),
                            'current_price': snapshot.day.close if snapshot.day else 0
                        })
            
            logger.info(f"Initial data fetched for {len(self.stocks_data)} symbols using snapshot API")
            
        except Exception as e:
            logger.error(f"Error fetching snapshot data: {e}")
            # Fallback to individual calls only if snapshot fails
            logger.info("Falling back to individual API calls...")
            
            # Limit to top symbols to minimize calls
            limited_symbols = list(self.nasdaq_symbols)[:100]
            for symbol in limited_symbols:
                try:
                    # Get previous day data
                    prev_day = self.rest_client.get_previous_close(symbol)
                    if prev_day and len(prev_day) > 0:
                        async with self.data_lock:
                            self.stocks_data[symbol].update({
                                'market_cap_millions': 0,  # Skip market cap in fallback
                                'previous_close': prev_day[0].close,
                                'open': prev_day[0].open,
                                'volume': 0,
                                'day_high': 0,
                                'day_low': float('inf')
                            })
                except Exception as e:
                    logger.debug(f"Error fetching data for {symbol}: {e}")
    
    def calculate_qualifying_criteria(self, symbol: str) -> bool:
        """Check if stock meets all qualifying criteria"""
        data = self.stocks_data.get(symbol, {})
        
        # Get required values
        volume = data.get('volume', 0)
        prev_close = data.get('previous_close', 0)
        open_price = data.get('open', 0)
        current_price = data.get('current_price', 0)
        
        # Calculate change from previous close
        if prev_close > 0:
            change_pct = ((current_price - prev_close) / prev_close) * 100
        else:
            change_pct = 0
        
        # Check all criteria
        meets_criteria = (
            volume > 300_000 and
            change_pct >= 2.5 and
            prev_close >= 0.01 and
            current_price > open_price
        )
        
        return meets_criteria
    
    async def handle_message(self, msg):
        """Handle incoming WebSocket messages (raw JSON)"""
        try:
            data = json.loads(msg)
            if isinstance(data, list):
                for event in data:
                    await self._process_event(event)
            elif isinstance(data, dict):
                await self._process_event(data)
        except Exception as e:
            logger.error(f"Error handling message: {e}")
    
    async def _process_event(self, event):
        symbol = event.get('sym')
        ev_type = event.get('ev')
        if not symbol or not ev_type:
            return
        async with self.data_lock:
            if symbol not in self.stocks_data:
                self.stocks_data[symbol] = {}
            if ev_type == 'T':  # Trade event
                self.stocks_data[symbol]['current_price'] = event.get('p', 0)
                self.stocks_data[symbol]['volume'] = self.stocks_data[symbol].get('volume', 0) + event.get('s', 0)
                # Update high/low
                current_high = self.stocks_data[symbol].get('day_high', 0)
                current_low = self.stocks_data[symbol].get('day_low', float('inf'))
                price = event.get('p', 0)
                self.stocks_data[symbol]['day_high'] = max(current_high, price)
                self.stocks_data[symbol]['day_low'] = min(current_low, price)
            elif ev_type == 'Q':  # Quote event
                self.stocks_data[symbol]['bid'] = event.get('b', 0)
                self.stocks_data[symbol]['ask'] = event.get('a', 0)
            elif ev_type == 'AM':  # Minute aggregate
                self.stocks_data[symbol]['current_price'] = event.get('c', 0)
                self.stocks_data[symbol]['volume'] = event.get('v', 0)
                self.stocks_data[symbol]['day_high'] = event.get('h', 0)
                self.stocks_data[symbol]['day_low'] = event.get('l', 0)
    
    async def write_data_snapshot(self):
        """Write current data snapshot to CSV"""
        import aiohttp
        timestamp = datetime.now(self.cst).strftime('%Y-%m-%d %H:%M:%S')
        rows = []
        async with self.data_lock:
            async with aiohttp.ClientSession() as session:
                for symbol in self.nasdaq_symbols:
                    if symbol not in self.stocks_data:
                        continue
                    data = self.stocks_data[symbol]
                    # Skip if no current price
                    if 'current_price' not in data:
                        continue
                    prev_close = data.get('previous_close', 0)
                    open_price = data.get('open', 0)
                    current_price = data.get('current_price', 0)
                    # Get shares outstanding, fetch if not present
                    shares_out = data.get('share_class_shares_outstanding')
                    if shares_out is None:
                        base_url = "https://api.polygon.io"
                        company_url = f"{base_url}/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
                        try:
                            async with session.get(company_url) as resp:
                                company_data = await resp.json()
                                if company_data.get('status') == 'OK' and company_data.get('results'):
                                    shares_out = company_data['results'].get('share_class_shares_outstanding')
                                    data['share_class_shares_outstanding'] = shares_out
                        except Exception as e:
                            shares_out = None
                    # Calculate intraday market cap
                    try:
                        if shares_out is not None and current_price is not None:
                            intraday_market_cap_millions = float(shares_out) * float(current_price) / 1_000_000
                        else:
                            intraday_market_cap_millions = None
                    except Exception:
                        intraday_market_cap_millions = None
                    change_pct = ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0
                    change_from_open_pct = ((current_price - open_price) / open_price * 100) if open_price > 0 else 0
                    meets_criteria = self.calculate_qualifying_criteria(symbol)
                    row = {
                        'timestamp': timestamp,
                        'symbol': symbol,
                        'share_class_shares_outstanding': shares_out,
                        'intraday_market_cap_millions': f"{intraday_market_cap_millions:.2f}" if intraday_market_cap_millions is not None else 'N/A',
                        'previous_close': f"{prev_close:.2f}",
                        'open': f"{open_price:.2f}",
                        'current_price': f"{current_price:.2f}",
                        'bid': f"{data.get('bid', 0):.2f}",
                        'ask': f"{data.get('ask', 0):.2f}",
                        'volume': data.get('volume', 0),
                        'day_high': f"{data.get('day_high', 0):.2f}",
                        'day_low': f"{data.get('day_low', 0):.2f}" if data.get('day_low', float('inf')) != float('inf') else "0.00",
                        'change_pct': f"{change_pct:.2f}",
                        'change_from_open_pct': f"{change_from_open_pct:.2f}",
                        'meets_criteria': 'Y' if meets_criteria else 'N'
                    }
                    rows.append(row)
                    if meets_criteria:
                        self.qualified_symbols.add(symbol)
                    elif symbol in self.qualified_symbols:
                        self.qualified_symbols.remove(symbol)
        # Update headers
        self.headers = [
            'timestamp', 'symbol', 'share_class_shares_outstanding', 'intraday_market_cap_millions', 'previous_close', 'open',
            'current_price', 'bid', 'ask', 'volume', 'day_high', 'day_low',
            'change_pct', 'change_from_open_pct', 'meets_criteria'
        ]
        with open(self.raw_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerows(rows)
        if self.filter_enabled and self.qualified_symbols:
            filtered_rows = [row for row in rows if row['meets_criteria'] == 'Y']
            if not os.path.exists(self.filtered_file):
                with open(self.filtered_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=self.headers)
                    writer.writeheader()
            with open(self.filtered_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writerows(filtered_rows)
        if AWS_S3_ENABLED:
            await self.upload_to_s3()
        logger.info(f"Data snapshot written - Total: {len(rows)}, Qualified: {len(self.qualified_symbols)}")
        if self.filter_enabled and len(self.qualified_symbols) == 0:
            logger.info("No qualifying stocks at this time, continuing to monitor...")
    
    async def upload_to_s3(self):
        """Upload CSV files to S3"""
        try:
            # Upload raw file
            with open(self.raw_file, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=f"{S3_PREFIX}{self.raw_file}",
                    Body=f.read()
                )
            
            # Upload filtered file if it exists
            if os.path.exists(self.filtered_file):
                with open(self.filtered_file, 'rb') as f:
                    self.s3_client.put_object(
                        Bucket=S3_BUCKET,
                        Key=f"{S3_PREFIX}{self.filtered_file}",
                        Body=f.read()
                    )
            
            logger.info("Files uploaded to S3")
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
    
    async def periodic_writer(self):
        """Write data snapshots every minute"""
        while self.running:
            await asyncio.sleep(POLL_INTERVAL)
            
            # Enable filtering after 7 minutes
            if not self.filter_enabled and self.start_time:
                elapsed = time.time() - self.start_time
                if elapsed >= FILTER_START_DELAY:
                    self.filter_enabled = True
                    logger.info("Filtering enabled - creating filtered output file")
            
            await self.write_data_snapshot()
    
    async def run(self):
        self.start_time = time.time()
        await self.fetch_nasdaq_symbols()
        # Start periodic writer
        writer_task = asyncio.create_task(self.periodic_writer())
        
        uri = "wss://socket.polygon.io/stocks"
        try:
            async with websockets.connect(uri) as ws:
                logger.info("Connected to Polygon WebSocket!")
                # Authenticate
                await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
                logger.info(f"Auth response: {await ws.recv()}")
                # Subscribe to trades for all symbols
                subscribe_str = ",".join(f"T.{s}" for s in self.nasdaq_symbols)
                await ws.send(json.dumps({"action": "subscribe", "params": subscribe_str}))
                logger.info(f"Subscribed to trades for {len(self.nasdaq_symbols)} symbols.")
                # Listen for messages
                while self.running:
                    msg = await ws.recv()
                    await self.handle_message(msg)
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
        finally:
            writer_task.cancel()
            logger.info("Monitor stopped")

def get_premarket_top_gainers(client, date_str, s3_bucket):
    """
    At 8:25am CDT, get premarket top gainer data from polygon.io and add to a CSV on AWS S3.
    """
    import aiohttp
    import asyncio
    async def fetch_gainers_with_mcap(gainers):
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
                    company_url = f"{base_url}/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
                    try:
                        async with session.get(company_url) as resp:
                            company_data = await resp.json()
                            if company_data.get('status') == 'OK' and company_data.get('results'):
                                shares_out = company_data['results'].get('share_class_shares_outstanding')
                    except Exception:
                        shares_out = None
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
    try:
        gainers = client.get_snapshot_direction("stocks", "gainers")
    except Exception as e:
        logger.error(f"Error fetching premarket top gainers: {e}")
        return
    # Prepare CSV data
    fieldnames = ["ticker", "change_percent", "direction", "share_class_shares_outstanding", "intraday_market_cap_millions"]
    rows = asyncio.run(fetch_gainers_with_mcap(gainers))
    # Write to CSV
    filename = f"premarket_top_gainers_{date_str}.csv"
    local_path = filename
    with open(local_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Premarket top gainers written to {local_path}")
    # Upload to S3 if enabled
    if AWS_S3_ENABLED and s3_bucket:
        s3_key = f"stock_data/{date_str}/{filename}"
        s3 = boto3.client("s3")
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
        logger.info(f"Premarket top gainers uploaded to S3: {s3_key}")

def get_nasdaq_symbols(client, date_str, s3_bucket):
    """
    Step 2: Get list of NASDAQ stocks from polygon.io (excluding warrants, ETFs, and other non-stock symbols)
    and save to /stock_data/YYYYMMDD/nasdaq_symbols.csv on S3.
    """
    try:
        # Fetch all NASDAQ tickers
        tickers = client.list_tickers(market="stocks", exchange="XNAS", active=True, limit=1000)
        # Filter out warrants, ETFs, and non-stock symbols
        filtered = []
        for t in tickers:
            # Exclude if type is not 'CS' (Common Stock)
            if hasattr(t, 'type') and t.type != 'CS':
                continue
            # Exclude if symbol contains "/" (often used for warrants)
            if '/' in t.ticker:
                continue
            filtered.append(t.ticker)
    except Exception as e:
        logger.error(f"Error fetching NASDAQ symbols: {e}")
        return
    # Write to CSV
    filename = f"nasdaq_symbols_{date_str}.csv"
    local_path = filename
    with open(local_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol"])
        for symbol in filtered:
            writer.writerow([symbol])
    logger.info(f"NASDAQ symbols written to {local_path} ({len(filtered)} symbols)")
    # Upload to S3 if enabled
    if AWS_S3_ENABLED and s3_bucket:
        s3_key = f"stock_data/symbols/{filename}"
        s3 = boto3.client("s3")
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
        logger.info(f"NASDAQ symbols uploaded to S3: {s3_key}")

def get_latest_nasdaq_symbols_from_s3(s3_bucket, _):
    """
    Find and download the latest nasdaq_symbols CSV from S3 (ignore date_str argument).
    """
    import boto3
    import re
    s3 = boto3.client("s3")
    prefix = "stock_data/symbols/"
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    # Find the latest file by date in filename
    date_pattern = re.compile(r'nasdaq_symbols_(\d{8})\.csv')
    latest_file = None
    latest_date = None
    for f in files:
        m = date_pattern.search(f)
        if m:
            file_date = m.group(1)
            if latest_date is None or file_date > latest_date:
                latest_date = file_date
                latest_file = f
    if not latest_file:
        logger.error("No nasdaq_symbols CSV found in S3.")
        return None
    # Download file
    local_path = f"nasdaq_symbols_{latest_date}.csv"
    s3.download_file(s3_bucket, latest_file, local_path)
    logger.info(f"Downloaded latest NASDAQ symbols file: {latest_file}")
    return local_path

async def fetch_symbol_data_http(session, symbol, api_key):
    """
    Fallback: Fetch required data for a symbol using HTTP requests to Polygon endpoints.
    """
    base_url = "https://api.polygon.io"
    headers = {"Authorization": f"Bearer {api_key}"}
    # Company details (market cap, avg volume)
    company_url = f"{base_url}/v3/reference/tickers/{symbol}?apiKey={api_key}"
    # Previous day OHLCV
    prev_url = f"{base_url}/v2/aggs/ticker/{symbol}/prev?apiKey={api_key}"
    # Last trade (current price)
    trade_url = f"{base_url}/v2/last/trade/{symbol}?apiKey={api_key}"
    result = {"symbol": symbol}
    try:
        async with session.get(company_url) as resp:
            data = await resp.json()
            if data.get('status') == 'OK' and data.get('results'):
                r = data['results']
                result['market_cap'] = r.get('market_cap')
                result['avg_volume'] = r.get('avg_volume')
        async with session.get(prev_url) as resp:
            data = await resp.json()
            if data.get('status') == 'OK' and data.get('results'):
                r = data['results'][0]
                result['open'] = r.get('o')
                result['close'] = r.get('c')
                result['volume'] = r.get('v')
        async with session.get(trade_url) as resp:
            data = await resp.json()
            if data.get('status') == 'OK' and data.get('results'):
                r = data['results']
                result['current_price'] = r.get('p')
    except Exception as e:
        logger.error(f"HTTP fallback error for {symbol}: {e}")
    return result

async def fetch_all_symbol_data(symbols, api_key, client):
    """
    For each symbol, try to fetch data using the client. If any required field is missing, use HTTP fallback.
    Throttle with a 1 second delay between each symbol.
    """
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for symbol in symbols:
            # Try client first
            try:
                snapshot = client.get_snapshot_ticker(market_type='stocks', ticker=symbol)
                market_cap = getattr(snapshot, 'market_cap', None)
                avg_volume = getattr(snapshot.ticker, 'avg_volume', None) if hasattr(snapshot, 'ticker') else None
                open_ = getattr(snapshot.day, 'o', None) if hasattr(snapshot, 'day') else None
                close = getattr(snapshot.day, 'c', None) if hasattr(snapshot, 'day') else None
                volume = getattr(snapshot.day, 'v', None) if hasattr(snapshot, 'day') else None
                current_price = getattr(snapshot.last_trade, 'p', None) if hasattr(snapshot, 'last_trade') else None
                # If any required field is missing, use HTTP fallback
                if None in [market_cap, avg_volume, open_, close, volume, current_price]:
                    tasks.append(fetch_symbol_data_http(session, symbol, api_key))
                    await asyncio.sleep(0.1)
                    continue
                result = {
                    'symbol': symbol,
                    'open': open_,
                    'close': close,
                    'volume': volume,
                    'avg_volume': avg_volume,
                    'market_cap': market_cap,
                    'current_price': current_price,
                }
                results.append(result)
            except Exception as e:
                logger.error(f"Client error for {symbol}: {e}")
                tasks.append(fetch_symbol_data_http(session, symbol, api_key))
            await asyncio.sleep(0.1)
        # Await all HTTP fallback tasks
        if tasks:
            http_results = await asyncio.gather(*tasks)
            results.extend(http_results)
    return results

def run_initial_data_pull(client, s3_bucket, api_key, date_str):
    """
    Step 3: Run initial data pull at 8:43am, fetch all required data for all symbols, write to raw_data_YYYYMMDD.csv, upload to S3.
    """
    import pandas as pd
    import aiohttp
    import asyncio
    local_symbols_path = get_latest_nasdaq_symbols_from_s3(s3_bucket, None)
    if not local_symbols_path:
        logger.error("No NASDAQ symbols file found, aborting initial data pull.")
        return
    df = pd.read_csv(local_symbols_path)
    symbols = df['symbol'].tolist()
    logger.info(f"Pulling initial data for {len(symbols)} symbols...")
    async def fetch_all_symbol_data(symbols, api_key, client):
        results = []
        async with aiohttp.ClientSession() as session:
            for symbol in symbols:
                try:
                    snapshot = client.get_snapshot_ticker(market_type='stocks', ticker=symbol)
                    open_ = getattr(snapshot.day, 'o', None) if hasattr(snapshot, 'day') else None
                    close = getattr(snapshot.day, 'c', None) if hasattr(snapshot, 'day') else None
                    volume = getattr(snapshot.day, 'v', None) if hasattr(snapshot, 'day') else None
                    avg_volume = getattr(snapshot.ticker, 'avg_volume', None) if hasattr(snapshot, 'ticker') else None
                    current_price = getattr(snapshot.last_trade, 'p', None) if hasattr(snapshot, 'last_trade') else None
                    # Fetch shares outstanding
                    shares_out = None
                    base_url = "https://api.polygon.io"
                    company_url = f"{base_url}/v3/reference/tickers/{symbol}?apiKey={api_key}"
                    try:
                        async with session.get(company_url) as resp:
                            company_data = await resp.json()
                            if company_data.get('status') == 'OK' and company_data.get('results'):
                                shares_out = company_data['results'].get('share_class_shares_outstanding')
                    except Exception:
                        shares_out = None
                    try:
                        if shares_out is not None and current_price is not None:
                            intraday_market_cap_millions = float(shares_out) * float(current_price) / 1_000_000
                        else:
                            intraday_market_cap_millions = None
                    except Exception:
                        intraday_market_cap_millions = None
                    result = {
                        'symbol': symbol,
                        'open': open_,
                        'close': close,
                        'volume': volume,
                        'avg_volume': avg_volume,
                        'current_price': current_price,
                        'share_class_shares_outstanding': shares_out,
                        'intraday_market_cap_millions': intraday_market_cap_millions
                    }
                    results.append(result)
                except Exception as e:
                    logger.error(f"Client error for {symbol}: {e}")
                    results.append({'symbol': symbol, 'open': None, 'close': None, 'volume': None, 'avg_volume': None, 'current_price': None, 'share_class_shares_outstanding': None, 'intraday_market_cap_millions': None})
                await asyncio.sleep(0.1)
        return results
    all_data = asyncio.run(fetch_all_symbol_data(symbols, api_key, client))
    for row in all_data:
        try:
            open_ = row.get('open')
            current_price = row.get('current_price')
            if open_ and current_price and open_ != 0:
                row['current_price_pct_change_from_open'] = ((current_price - open_) / open_) * 100
            else:
                row['current_price_pct_change_from_open'] = None
        except Exception as e:
            logger.error(f"Error calculating pct change for {row.get('symbol')}: {e}")
    filename = f"raw_data_{date_str}.csv"
    local_path = filename
    fieldnames = ['symbol', 'open', 'close', 'volume', 'avg_volume', 'current_price', 'share_class_shares_outstanding', 'intraday_market_cap_millions', 'current_price_pct_change_from_open']
    with open(local_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_data:
            writer.writerow({k: row.get(k) for k in fieldnames})
    logger.info(f"Raw data written to {local_path}")
    if AWS_S3_ENABLED and s3_bucket:
        s3_key = f"stock_data/{date_str}/{filename}"
        s3 = boto3.client("s3")
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
        logger.info(f"Raw data uploaded to S3: {s3_key}")

def ensure_raw_data_with_symbols(date_str, s3_bucket, fieldnames, client):
    """
    Ensure raw_data_YYYYMMDD.csv exists and has at least the latest NASDAQ symbols as rows.
    If not, fetch symbols and create a new file with N/A for all columns except symbol.
    Returns the local path to the file.
    """
    import boto3
    import pandas as pd
    filename = f"raw_data_{date_str}.csv"
    local_path = filename
    s3_key = f"stock_data/{date_str}/{filename}"
    s3 = boto3.client("s3")
    # Try to download
    try:
        s3.download_file(s3_bucket, s3_key, local_path)
        logger.info(f"Downloaded {filename} from S3.")
        df = pd.read_csv(local_path)
        if df.empty or 'symbol' not in df.columns or df['symbol'].isnull().all():
            raise Exception("Downloaded file is empty or missing symbols.")
    except Exception as e:
        logger.warning(f"{filename} not found or empty in S3, creating new file with latest symbols.")
        # Get latest symbols
        tickers = client.list_tickers(market="stocks", exchange="XNAS", active=True, limit=1000)
        symbols = [t.ticker for t in tickers if hasattr(t, 'type') and t.type == 'CS' and '/' not in t.ticker]
        # Create DataFrame
        df = pd.DataFrame({'symbol': symbols})
        for col in fieldnames:
            if col != 'symbol':
                df[col] = 'N/A'
        df.to_csv(local_path, index=False)
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=open(local_path, "rb").read())
        logger.info(f"Created and uploaded new {filename} to S3 with {len(symbols)} symbols.")
    return local_path

def update_qualified_column(date_str, s3_bucket, client):
    """
    Step 4: Update the raw_data CSV with a new column 'qualified' using the qualifiers:
    - volume > 300K
    - current price at 8:43 >= 2.5% from previous close
    - close >= $0.01
    - open > close
    """
    import pandas as pd
    import numpy as np
    fieldnames = ['symbol', 'open', 'close', 'volume', 'avg_volume', 'market_cap_millions', 'current_price', 'current_price_pct_change_from_open']
    local_path = ensure_raw_data_with_symbols(date_str, s3_bucket, fieldnames, client)
    filename = os.path.basename(local_path)
    df = pd.read_csv(local_path)
    # Calculate qualification
    def is_qualified(row):
        try:
            vol = float(row.get('volume', 0) or 0)
            close = float(row.get('close', 0) or 0)
            open_ = float(row.get('open', 0) or 0)
            curr = float(row.get('current_price', 0) or 0)
            prev_close = float(row.get('close', 0) or 0)  # Using close as previous close at 8:43
            pct_change = float(row.get('current_price_pct_change_from_open', 0) or 0)
            return (
                vol > 300_000 and
                (curr - prev_close) / prev_close * 100 >= 2.5 if prev_close else False and
                close >= 0.01 and
                open_ > close
            )
        except Exception as e:
            logger.error(f"Error qualifying row: {e}")
            return False
    # Add qualified column with timestamp
    now = datetime.now(pytz.timezone('America/Chicago'))
    hhmm = now.strftime('%H:%M')
    df['qualified'] = df.apply(lambda row: f"[{hhmm}] - {'True' if is_qualified(row) else 'False'}", axis=1)
    # Overwrite CSV
    df.to_csv(local_path, index=False)
    logger.info(f"Updated {filename} with qualified column.")
    # Upload to S3
    if AWS_S3_ENABLED and s3_bucket:
        s3_key = f"stock_data/{date_str}/{filename}"
        s3 = boto3.client("s3")
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
        logger.info(f"Qualified data uploaded to S3: {s3_key}")

def update_intraday_data_and_qualified(date_str, s3_bucket, hhmm, client):
    """
    Step 5: At a given time (e.g., 08:40), pull price and volume for each stock in raw data and append to the CSV
    with new columns and update the qualified column for that time.
    """
    import pandas as pd
    import numpy as np
    import aiohttp
    import asyncio
    fieldnames = ['symbol', 'open', 'close', 'volume', 'avg_volume', 'market_cap_millions', 'current_price', 'current_price_pct_change_from_open']
    local_path = ensure_raw_data_with_symbols(date_str, s3_bucket, fieldnames, client)
    filename = os.path.basename(local_path)
    df = pd.read_csv(local_path)
    symbols = df['symbol'].tolist()
    # Prepare new columns
    price_col = f'current_price_{hhmm}'
    pct_col = f'current_price_pct_change_from_open_{hhmm}'
    vol_col = f'current_volume_{hhmm}'
    mcap_col = f'intraday_market_cap_millions_{hhmm}'
    shares_col = 'share_class_shares_outstanding'
    # Fetch new data for each symbol (client, fallback to HTTP if needed)
    async def fetch_intraday(symbols, api_key, client):
        results = []
        async with aiohttp.ClientSession() as session:
            tasks = []
            for symbol in symbols:
                try:
                    snapshot = client.get_snapshot_ticker(market_type='stocks', ticker=symbol)
                    price = getattr(snapshot.last_trade, 'p', None) if hasattr(snapshot, 'last_trade') else None
                    volume = getattr(snapshot.day, 'v', None) if hasattr(snapshot, 'day') else None
                    open_ = getattr(snapshot.day, 'o', None) if hasattr(snapshot, 'day') else None
                    # Fetch shares outstanding if not present
                    shares_out = None
                    if shares_col in df.columns:
                        shares_out = df.loc[df['symbol'] == symbol, shares_col].values[0]
                        if pd.isna(shares_out) or shares_out == 'N/A':
                            shares_out = None
                    if shares_out is None:
                        # Fetch from company endpoint
                        base_url = "https://api.polygon.io"
                        company_url = f"{base_url}/v3/reference/tickers/{symbol}?apiKey={api_key}"
                        async with session.get(company_url) as resp:
                            data = await resp.json()
                            if data.get('status') == 'OK' and data.get('results'):
                                shares_out = data['results'].get('share_class_shares_outstanding')
                    if None in [price, volume, open_]:
                        # Fallback HTTP
                        base_url = "https://api.polygon.io"
                        trade_url = f"{base_url}/v2/last/trade/{symbol}?apiKey={api_key}"
                        prev_url = f"{base_url}/v2/aggs/ticker/{symbol}/prev?apiKey={api_key}"
                        async with session.get(trade_url) as resp:
                            data = await resp.json()
                            if data.get('status') == 'OK' and data.get('results'):
                                price = data['results'].get('p')
                        async with session.get(prev_url) as resp:
                            data = await resp.json()
                            if data.get('status') == 'OK' and data.get('results'):
                                open_ = data['results'][0].get('o')
                                volume = data['results'][0].get('v')
                    results.append({'symbol': symbol, 'price': price, 'open': open_, 'volume': volume, 'shares_out': shares_out})
                except Exception as e:
                    logger.error(f"Error fetching intraday for {symbol}: {e}")
                    results.append({'symbol': symbol, 'price': None, 'open': None, 'volume': None, 'shares_out': None})
                await asyncio.sleep(0.1)
            return results
    intraday_data = asyncio.run(fetch_intraday(symbols, POLYGON_API_KEY, client))
    # Map symbol to new data
    data_map = {row['symbol']: row for row in intraday_data}
    # Add new columns
    df[price_col] = df['symbol'].map(lambda s: data_map.get(s, {}).get('price'))
    df[vol_col] = df['symbol'].map(lambda s: data_map.get(s, {}).get('volume'))
    # Add/fill shares outstanding column
    if shares_col not in df.columns:
        df[shares_col] = np.nan
    for s in symbols:
        shares_val = data_map.get(s, {}).get('shares_out')
        if shares_val is not None:
            df.loc[df['symbol'] == s, shares_col] = shares_val
    # Calculate intraday market cap in millions
    def calc_intraday_mcap(row):
        try:
            shares = row.get(shares_col)
            price = row.get(price_col)
            if pd.isna(shares) or shares in [None, 'N/A'] or pd.isna(price) or price in [None, 'N/A']:
                return None
            return float(shares) * float(price) / 1_000_000
        except Exception:
            return None
    df[mcap_col] = df.apply(calc_intraday_mcap, axis=1)
    # Calculate pct change from open for this time
    def calc_pct(row):
        open_ = row.get('open')
        price = row.get(price_col)
        if open_ and price and open_ != 0:
            return ((price - open_) / open_) * 100
        return None
    df[pct_col] = df.apply(calc_pct, axis=1)
    # Update qualified column for this time
    def is_qualified(row):
        try:
            vol = float(row.get(vol_col, 0) or 0)
            close = float(row.get('close', 0) or 0)
            open_ = float(row.get('open', 0) or 0)
            curr = float(row.get(price_col, 0) or 0)
            prev_close = float(row.get('close', 0) or 0)
            return (
                vol > 300_000 and
                (curr - prev_close) / prev_close * 100 >= 2.5 if prev_close else False and
                close >= 0.01 and
                open_ > close
            )
        except Exception as e:
            logger.error(f"Error qualifying row: {e}")
            return False
    df['qualified'] = df.apply(lambda row: f"[{hhmm}] - {'True' if is_qualified(row) else 'False'}", axis=1)
    # Overwrite CSV
    df.to_csv(local_path, index=False)
    logger.info(f"Updated {filename} with intraday columns and qualified at {hhmm}.")
    # Upload to S3
    if AWS_S3_ENABLED and s3_bucket:
        s3_key = f"stock_data/{date_str}/{filename}"
        s3 = boto3.client("s3")
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
        logger.info(f"Intraday data uploaded to S3: {s3_key}")

def update_intraday_full_data_and_qualified(date_str, s3_bucket, hhmm, client):
    """
    Step 6: At specified intervals, pull price, volume, high, and low for each stock in raw data and append to the CSV
    with new columns and update the qualified column for that time.
    """
    import pandas as pd
    import numpy as np
    import aiohttp
    import asyncio
    fieldnames = ['symbol', 'open', 'close', 'volume', 'avg_volume', 'market_cap_millions', 'current_price', 'current_price_pct_change_from_open']
    local_path = ensure_raw_data_with_symbols(date_str, s3_bucket, fieldnames, client)
    filename = os.path.basename(local_path)
    df = pd.read_csv(local_path)
    symbols = df['symbol'].tolist()
    # Prepare new columns
    price_col = f'current_price_{hhmm}'
    pct_col = f'current_price_pct_change_from_open_{hhmm}'
    vol_col = f'current_volume_{hhmm}'
    high_col = f'high_{hhmm}'
    low_col = f'low_{hhmm}'
    mcap_col = f'intraday_market_cap_millions_{hhmm}'
    shares_col = 'share_class_shares_outstanding'
    # Fetch new data for each symbol (client, fallback to HTTP if needed)
    async def fetch_intraday_full(symbols, api_key, client):
        results = []
        async with aiohttp.ClientSession() as session:
            tasks = []
            for symbol in symbols:
                try:
                    snapshot = client.get_snapshot_ticker(market_type='stocks', ticker=symbol)
                    price = getattr(snapshot.last_trade, 'p', None) if hasattr(snapshot, 'last_trade') else None
                    volume = getattr(snapshot.day, 'v', None) if hasattr(snapshot, 'day') else None
                    open_ = getattr(snapshot.day, 'o', None) if hasattr(snapshot, 'day') else None
                    high = getattr(snapshot.day, 'h', None) if hasattr(snapshot, 'day') else None
                    low = getattr(snapshot.day, 'l', None) if hasattr(snapshot, 'day') else None
                    # Fetch shares outstanding if not present
                    shares_out = None
                    if shares_col in df.columns:
                        shares_out = df.loc[df['symbol'] == symbol, shares_col].values[0]
                        if pd.isna(shares_out) or shares_out == 'N/A':
                            shares_out = None
                    if shares_out is None:
                        # Fetch from company endpoint
                        base_url = "https://api.polygon.io"
                        company_url = f"{base_url}/v3/reference/tickers/{symbol}?apiKey={api_key}"
                        async with session.get(company_url) as resp:
                            data = await resp.json()
                            if data.get('status') == 'OK' and data.get('results'):
                                shares_out = data['results'].get('share_class_shares_outstanding')
                    if None in [price, volume, open_, high, low]:
                        # Fallback HTTP
                        base_url = "https://api.polygon.io"
                        trade_url = f"{base_url}/v2/last/trade/{symbol}?apiKey={api_key}"
                        prev_url = f"{base_url}/v2/aggs/ticker/{symbol}/prev?apiKey={api_key}"
                        async with session.get(trade_url) as resp:
                            data = await resp.json()
                            if data.get('status') == 'OK' and data.get('results'):
                                price = data['results'].get('p')
                        async with session.get(prev_url) as resp:
                            data = await resp.json()
                            if data.get('status') == 'OK' and data.get('results'):
                                open_ = data['results'][0].get('o')
                                volume = data['results'][0].get('v')
                                high = data['results'][0].get('h')
                                low = data['results'][0].get('l')
                    results.append({'symbol': symbol, 'price': price, 'open': open_, 'volume': volume, 'high': high, 'low': low, 'shares_out': shares_out})
                except Exception as e:
                    logger.error(f"Error fetching intraday full for {symbol}: {e}")
                    results.append({'symbol': symbol, 'price': None, 'open': None, 'volume': None, 'high': None, 'low': None, 'shares_out': None})
                await asyncio.sleep(0.1)
            return results
    intraday_data = asyncio.run(fetch_intraday_full(symbols, POLYGON_API_KEY, client))
    # Map symbol to new data
    data_map = {row['symbol']: row for row in intraday_data}
    # Add new columns
    df[price_col] = df['symbol'].map(lambda s: data_map.get(s, {}).get('price'))
    df[vol_col] = df['symbol'].map(lambda s: data_map.get(s, {}).get('volume'))
    df[high_col] = df['symbol'].map(lambda s: data_map.get(s, {}).get('high'))
    df[low_col] = df['symbol'].map(lambda s: data_map.get(s, {}).get('low'))
    # Add/fill shares outstanding column
    if shares_col not in df.columns:
        df[shares_col] = np.nan
    for s in symbols:
        shares_val = data_map.get(s, {}).get('shares_out')
        if shares_val is not None:
            df.loc[df['symbol'] == s, shares_col] = shares_val
    # Calculate intraday market cap in millions
    def calc_intraday_mcap(row):
        try:
            shares = row.get(shares_col)
            price = row.get(price_col)
            if pd.isna(shares) or shares in [None, 'N/A'] or pd.isna(price) or price in [None, 'N/A']:
                return None
            return float(shares) * float(price) / 1_000_000
        except Exception:
            return None
    df[mcap_col] = df.apply(calc_intraday_mcap, axis=1)
    # Calculate pct change from open for this time
    def calc_pct(row):
        open_ = row.get('open')
        price = row.get(price_col)
        if open_ and price and open_ != 0:
            return ((price - open_) / open_) * 100
        return None
    df[pct_col] = df.apply(calc_pct, axis=1)
    # Update qualified column for this time
    def is_qualified(row):
        try:
            vol = float(row.get(vol_col, 0) or 0)
            close = float(row.get('close', 0) or 0)
            open_ = float(row.get('open', 0) or 0)
            curr = float(row.get(price_col, 0) or 0)
            prev_close = float(row.get('close', 0) or 0)
            return (
                vol > 300_000 and
                (curr - prev_close) / prev_close * 100 >= 2.5 if prev_close else False and
                close >= 0.01 and
                open_ > close
            )
        except Exception as e:
            logger.error(f"Error qualifying row: {e}")
            return False
    df['qualified'] = df.apply(lambda row: f"[{hhmm}] - {'True' if is_qualified(row) else 'False'}", axis=1)
    # Overwrite CSV
    df.to_csv(local_path, index=False)
    logger.info(f"Updated {filename} with intraday full columns and qualified at {hhmm}.")
    # Upload to S3
    if AWS_S3_ENABLED and s3_bucket:
        s3_key = f"stock_data/{date_str}/{filename}"
        s3 = boto3.client("s3")
        with open(local_path, "rb") as f:
            s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=f.read())
        logger.info(f"Intraday full data uploaded to S3: {s3_key}")

# --- Add a helper to fetch initial data for a symbol ---
def fetch_initial_data_for_symbol(symbol, api_key, client):
    """
    Fetch open, close, volume, avg volume, market cap, current price for a symbol using Polygon client.
    Returns a dict with the values or N/A if not found.
    """
    try:
        snapshot = client.get_snapshot_ticker(market_type='stocks', ticker=symbol)
        open_ = getattr(snapshot.day, 'o', None) if hasattr(snapshot, 'day') else None
        close = getattr(snapshot.day, 'c', None) if hasattr(snapshot, 'day') else None
        volume = getattr(snapshot.day, 'v', None) if hasattr(snapshot, 'day') else None
        avg_volume = getattr(snapshot.ticker, 'avg_volume', None) if hasattr(snapshot, 'ticker') else None
        market_cap = getattr(snapshot, 'market_cap', None)
        current_price = getattr(snapshot.last_trade, 'p', None) if hasattr(snapshot, 'last_trade') else None
        return {
            'open': open_ if open_ is not None else 'N/A',
            'close': close if close is not None else 'N/A',
            'volume': volume if volume is not None else 'N/A',
            'avg_volume': avg_volume if avg_volume is not None else 'N/A',
            'market_cap_millions': market_cap / 1_000_000 if market_cap is not None else 'N/A',
            'current_price': current_price if current_price is not None else 'N/A',
        }
    except Exception as e:
        logger.error(f"Initial data fallback error for {symbol}: {e}")
        return {
            'open': 'N/A', 'close': 'N/A', 'volume': 'N/A', 'avg_volume': 'N/A', 'market_cap_millions': 'N/A', 'current_price': 'N/A'
        }

def main():
    POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
    client = RESTClient(POLYGON_API_KEY)
    s3_bucket = S3_BUCKET
    cst = pytz.timezone('America/Chicago')
    date_str = datetime.now(cst).strftime('%Y%m%d')
    steps_run = set()

    # Define the schedule: (time_str, function, args)
    schedule = [
        ('08:24', get_premarket_top_gainers, [client, date_str, s3_bucket]),
        ('08:27', get_nasdaq_symbols, [client, date_str, s3_bucket]),
        ('08:35', run_initial_data_pull, [client, s3_bucket, POLYGON_API_KEY, date_str]),
        ('08:44', update_qualified_column, [date_str, s3_bucket, client]),
        ('08:45', update_intraday_data_and_qualified, [date_str, s3_bucket, '08:45', client]),
        ('08:50', update_intraday_full_data_and_qualified, [date_str, s3_bucket, '08:50', client]),
        ('08:55', update_intraday_full_data_and_qualified, [date_str, s3_bucket, '08:55', client]),
        ('09:00', update_intraday_full_data_and_qualified, [date_str, s3_bucket, '09:00', client]),
        ('09:15', update_intraday_full_data_and_qualified, [date_str, s3_bucket, '09:15', client]),
        ('09:30', update_intraday_full_data_and_qualified, [date_str, s3_bucket, '09:30', client]),
        ('14:30', update_intraday_full_data_and_qualified, [date_str, s3_bucket, '14:30', client]),
    ]

    logger.info("Starting persistent scheduler loop for Polygon stock data workflow.")
    while True:
        now = datetime.now(cst)
        time_str = now.strftime('%H:%M')
        # If the date changes (new day), reset steps_run and date_str
        new_date_str = now.strftime('%Y%m%d')
        if new_date_str != date_str:
            logger.info(f"Date changed to {new_date_str}, resetting step tracker.")
            date_str = new_date_str
            steps_run = set()
            # Update args for new date
            for i, (sched_time, func, args) in enumerate(schedule):
                new_args = list(args)
                for j, arg in enumerate(new_args):
                    if isinstance(arg, str) and arg.isdigit() and len(arg) == 8:
                        new_args[j] = date_str
                # For steps that require client, ensure client is present in args
                if func in [update_qualified_column, update_intraday_data_and_qualified, update_intraday_full_data_and_qualified]:
                    if client not in new_args:
                        new_args.append(client)
                schedule[i] = (sched_time, func, new_args)
        for sched_time, func, args in schedule:
            step_id = f"{date_str}-{sched_time}-{func.__name__}"
            if time_str == sched_time and step_id not in steps_run:
                logger.info(f"Running scheduled step: {func.__name__} at {sched_time}")
                try:
                    func(*args)
                    steps_run.add(step_id)
                except Exception as e:
                    logger.error(f"Error running {func.__name__} at {sched_time}: {e}")
        time.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    main()