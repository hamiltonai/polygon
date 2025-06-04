# Real-Time NASDAQ Stock Monitor (Premium Edition)

## Overview
A high-performance real-time stock monitoring system that uses Polygon.io's premium WebSocket API to track all NASDAQ-listed stocks, capturing aggregated data every minute with automatic filtering based on qualifying criteria. Optimized for minimal API calls while maintaining comprehensive coverage.

## Features
- **Premium Real-time Feed**: Uses SIP feed for zero-delay market data
- **Efficient API Usage**: Bulk snapshot API for initial data, WebSocket for real-time updates
- **Continuous Operation**: Runs indefinitely, even when no stocks qualify
- **Minute Aggregation**: Collects real-time data but writes snapshots every 60 seconds
- **Initial Bulk Fetch**: Retrieves all stock data in one efficient API call
- **Automatic Filtering**: After 7 minutes, creates a filtered output with only qualifying stocks
- **Dynamic Qualification**: Continuously evaluates stocks against criteria
- **Local Storage**: Saves data as CSV files with optional S3 upload

## Optimizations for Unlimited API Access
- **Bulk Snapshot API**: Fetches initial data for all symbols in a single call
- **Efficient Pagination**: Properly handles pagination when fetching symbol lists
- **Batch Subscriptions**: Subscribes to symbols in optimized batches
- **WebSocket-First**: Relies on WebSocket for real-time data to minimize REST calls
- **Smart Symbol Grouping**: Groups symbols in subscription strings for efficiency

## Data Flow
1. **Startup**: Fetches all NASDAQ symbols using paginated API
2. **Bulk Initial Data**: Uses snapshot API to get all stock data in one call
3. **Real-time Subscription**: Connects to premium SIP WebSocket feed
4. **Data Aggregation**: Collects trades, quotes, and minute aggregates in memory
5. **Minute Snapshots**: Every 60 seconds, writes current state to CSV
6. **Filtering**: After 7 minutes, starts writing qualified stocks to separate file
7. **Continuous Monitoring**: Runs indefinitely, tracking qualification status

## Qualifying Criteria
Stocks must meet ALL of the following:
- Volume > 300,000 shares
- Price change >= 2.5% from previous close
- Previous close >= $0.01
- Current price > open price (gap up maintained)

## Output Files

### 1. Raw Data File
`nasdaq_monitor_raw_YYYYMMDD_HHMM.csv`
- Contains all NASDAQ stocks with data
- Updated every minute
- Includes qualification status

### 2. Filtered Data File
`nasdaq_monitor_filtered_YYYYMMDD_HHMM.csv`
- Only stocks meeting ALL criteria
- Starts after 7 minutes of monitoring
- Dynamically updated as stocks qualify/disqualify

### CSV Structure
```csv
timestamp,symbol,market_cap_millions,previous_close,open,current_price,bid,ask,volume,day_high,day_low,change_pct,change_from_open_pct,meets_criteria
2024-01-15 08:32:00,AAPL,2890000.00,149.50,150.25,151.00,150.95,151.05,425000,151.50,150.00,1.00,0.50,Y
```

## Configuration

### Environment Variables
```bash
POLYGON_API_KEY=your_api_key_here
BUCKET_NAME=your-s3-bucket
```

### Python Configuration
```python
AWS_S3_ENABLED = False  # Toggle S3 upload
S3_BUCKET = os.getenv('BUCKET_NAME', 'your-bucket-name')
S3_PREFIX = "real-time-monitor/"
POLL_INTERVAL = 60  # seconds between data writes
FILTER_START_DELAY = 420  # 7 minutes before filtering starts
```

## Usage

### In Jupyter Notebook
```python
# Load and run the monitor
%run polygon_nasdaq_monitor.py

# Or use async directly
monitor = NASDAQMonitor()
await monitor.run()
```

### As a Script
```bash
python polygon_nasdaq_monitor.py
```

### Stopping the Monitor
- **Manual Only**: Use Ctrl+C or Kernel > Interrupt in Jupyter
- The monitor will continue running even if no stocks qualify

## Dependencies
```python
polygon-api-client>=1.12.0
boto3>=1.26.0
pandas>=2.0.0
pytz>=2023.3
python-dotenv>=1.0.0
asyncio
```

## Performance Considerations
- **Premium SIP Feed**: Uses real-time SIP feed for zero delay
- **Bulk API Calls**: Initial data fetched in one snapshot call
- **Efficient Subscriptions**: Symbols grouped in comma-separated strings
- **Memory Usage**: Stores latest data for all symbols in memory (~4-5k symbols)
- **File I/O**: Writes to disk every minute
- **API Call Minimization**: After initial load, relies entirely on WebSocket

## Sample Output
```
2024-01-15 08:32:00 - INFO - Fetching NASDAQ symbols...
2024-01-15 08:32:02 - INFO - Page 1: Found 1000 symbols, total: 1000
2024-01-15 08:32:03 - INFO - Page 2: Found 1000 symbols, total: 2000
2024-01-15 08:32:04 - INFO - Page 3: Found 1000 symbols, total: 3000
2024-01-15 08:32:05 - INFO - Page 4: Found 547 symbols, total: 3547
2024-01-15 08:32:05 - INFO - Found 3,547 NASDAQ symbols
2024-01-15 08:32:06 - INFO - Initial data fetched for 3,547 symbols using snapshot API
2024-01-15 08:32:10 - INFO - Subscribing to batch 1/36
2024-01-15 08:32:15 - INFO - Subscriptions complete. Starting real-time monitoring...
2024-01-15 08:33:00 - INFO - Data snapshot written - Total: 3,245, Qualified: 47
2024-01-15 08:34:00 - INFO - Data snapshot written - Total: 3,312, Qualified: 52
...
2024-01-15 08:39:00 - INFO - Filtering enabled - creating filtered output file
2024-01-15 08:40:00 - INFO - Data snapshot written - Total: 3,398, Qualified: 38
2024-01-15 08:41:00 - INFO - Data snapshot written - Total: 3,412, Qualified: 22
2024-01-15 08:42:00 - INFO - Data snapshot written - Total: 3,425, Qualified: 0
2024-01-15 08:43:00 - INFO - No qualifying stocks at this time, continuing to monitor...
2024-01-15 08:44:00 - INFO - Data snapshot written - Total: 3,438, Qualified: 3
```

## Troubleshooting

### Common Issues
1. **API Key Error**: Ensure POLYGON_API_KEY is set in environment
2. **WebSocket Connection**: Check internet connectivity and API limits
3. **Memory Usage**: Monitor RAM usage for large symbol sets
4. **No Data**: Verify market hours and symbol validity

### Debug Mode
Enable detailed logging:
```python
logging.basicConfig(level=logging.DEBUG)
```

## API Usage Optimization

### Initial Load (One-time)
- **Symbol List**: 1-5 API calls (paginated)
- **Snapshot Data**: 1 API call (bulk snapshot for all symbols)
- **Total**: ~2-6 API calls for complete initialization

### Ongoing Operation
- **REST API Calls**: 0 (all data via WebSocket)
- **WebSocket Messages**: Continuous stream (unlimited)
- **Efficiency**: After initial load, zero REST API calls needed

## Notes
- Data is captured during market hours for best results
- Premium SIP feed provides real-time data with no delay
- Qualified stocks list may be empty during low volatility periods
- Monitor continues running regardless of qualification status
- Consider running during pre-market and regular market hours for maximum coverage
- Efficient bulk APIs minimize call usage while maintaining comprehensive data