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

import websockets

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env file loaded successfully")
except ImportError:
    print("⚠️  python-dotenv not installed - using system environment variables")

# Configuration
AWS_S3_ENABLED = True  # Toggle S3 upload
S3_BUCKET = os.getenv('BUCKET_NAME')
S3_PREFIX = "stock_data/real-time-monitor/"
POLL_INTERVAL = 30  # seconds
FILTER_START_DELAY = 420  # 7 minutes (420 seconds)
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NASDAQMonitor:
    def __init__(self):
        from polygon import RESTClient
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
            symbols = symbols[:100]
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
        timestamp = datetime.now(self.cst).strftime('%Y-%m-%d %H:%M:%S')
        rows = []
        
        async with self.data_lock:
            for symbol in self.nasdaq_symbols:
                if symbol not in self.stocks_data:
                    continue
                
                data = self.stocks_data[symbol]
                
                # Skip if no current price
                if 'current_price' not in data:
                    continue
                
                # Calculate percentages
                prev_close = data.get('previous_close', 0)
                open_price = data.get('open', 0)
                current_price = data.get('current_price', 0)
                
                change_pct = ((current_price - prev_close) / prev_close * 100) if prev_close > 0 else 0
                change_from_open_pct = ((current_price - open_price) / open_price * 100) if open_price > 0 else 0
                
                # Check if meets criteria
                meets_criteria = self.calculate_qualifying_criteria(symbol)
                
                row = {
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'market_cap_millions': f"{data.get('market_cap_millions', 0):.2f}",
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
                
                # Track qualified symbols
                if meets_criteria:
                    self.qualified_symbols.add(symbol)
                elif symbol in self.qualified_symbols:
                    self.qualified_symbols.remove(symbol)
        
        # Write to raw file
        with open(self.raw_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerows(rows)
        
        # Write to filtered file if enabled and there are qualified stocks
        if self.filter_enabled and self.qualified_symbols:
            filtered_rows = [row for row in rows if row['meets_criteria'] == 'Y']
            
            # Create filtered file if it doesn't exist
            if not os.path.exists(self.filtered_file):
                with open(self.filtered_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=self.headers)
                    writer.writeheader()
            
            with open(self.filtered_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writerows(filtered_rows)
        
        # Upload to S3 if enabled
        if AWS_S3_ENABLED:
            await self.upload_to_s3()
        
        logger.info(f"Data snapshot written - Total: {len(rows)}, Qualified: {len(self.qualified_symbols)}")
        
        # Log when no qualifiers but keep running
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

def main():
    monitor = NASDAQMonitor()
    asyncio.run(monitor.run())

if __name__ == "__main__":
    main()