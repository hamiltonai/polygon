# Polygon Stock Screener

A Python application that monitors NASDAQ stocks using Polygon.io API, applies screening criteria, and sends alerts via AWS SNS.

## Overview

This application periodically fetches market data for NASDAQ stocks, applies specific screening criteria at 8:37 AM CDT, and alerts users to qualifying stocks. The system maintains historical data in AWS S3 and tracks key metrics throughout the trading day.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Polygon.io    │────│  Python App     │────│    AWS S3       │
│     API         │    │   (Scheduler)   │    │   (CSV Data)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │    AWS SNS      │
                       │   (Alerts)      │
                       └─────────────────┘
```

## Features

- **Automated Data Collection**: Fetches real-time market data from Polygon.io
- **Stock Screening**: Applies qualifying criteria at 8:37 AM CDT and at additional timepoints
- **Data Persistence**: Appends historical data to a single CSV per day in AWS S3
- **Alert System**: Sends filtered results via AWS SNS at each checkpoint (timepoint)
- **Multi-timepoint Tracking**: Captures quotes at specific CDT intervals, with the timepoint passed as an event parameter
- **Flexible Symbol Source**: Symbol list source is controlled by an event parameter: 'Static' uses a static list, 'Dynamic' fetches from Polygon
- **Robust Error Handling**: Errors are logged and major errors are sent to SNS; stocks with N/A data are ignored

## Requirements

### APIs & Services
- Polygon.io API key (premium tier recommended for real-time data)
- AWS Account with S3 and SNS access
- AWS credentials configured (IAM role or access keys)

### Python Dependencies
```python
# Core dependencies to implement
polygon-api-client>=1.12.0
boto3>=1.26.0
pandas>=2.0.0
pytz>=2023.3
schedule>=1.2.0
python-dotenv>=1.0.0
```

## Data Structure

### CSV Schema (S3 Storage)
- Data is appended to a single CSV per day.
- Schema:
```csv
symbol,date,timestamp_cdt,open,high,low,close,volume,prev_close,market_cap,avg_volume,change_pct
AAPL,2024-01-15,08:37:00,150.25,151.00,149.80,150.75,425000,148.50,2500000000,45000000,1.18
```

### Qualifying Criteria (8:37 AM CDT and other timepoints)
- Volume > 300,000 shares
- Price change >= 2.5% from previous close
- Previous close >= $0.01
- Open > previous close (gap up only)
- Stocks with N/A data are ignored

### Tracked Timepoints (CDT)
- Market open
- 8:37 AM (screening time)
- 8:40 AM
- 8:50 AM
- 8:55 AM
- 9:00 AM
- 9:15 AM
- 9:30 AM (market open)
- 2:30 PM
- The timepoint is passed as an event parameter to the Lambda function.

### Additional Metrics
- High of day (after 8:50 AM CDT)
- Low of day (after 8:50 AM CDT)
- Market capitalization
- Average volume (30-day)

## Implementation Guide

### 1. Project Structure
```
polygon-screener/
├── src/
│   ├──polygon_screener.py
├── requirements.txt
├── .env.example
└── README.md
```

### 2. Environment Variables
```bash
# .env file
POLYGON_API_KEY=""
BUCKET_NAME=""
SNS_TOPIC_ARN=""
TIMEZONE=America/Chicago  # CDT timezone
```

### 3. Core Classes to Implement

#### DataFetcher
```python
class DataFetcher:
    """Handles Polygon.io API interactions"""
    def get_nasdaq_symbols(self, mode: str = 'Static') -> List[str]:
        """mode: 'Static' uses a static list, 'Dynamic' fetches from Polygon.io"""
    def get_real_time_quote(self, symbol: str) -> Dict
    def get_previous_close(self, symbol: str) -> float
    def get_market_cap(self, symbol: str) -> float
    def get_avg_volume(self, symbol: str) -> float
```

#### StockScreener
```python
class StockScreener:
    """Applies screening criteria to stock data"""
    def apply_qualifying_criteria(self, stocks_data: List[Dict]) -> List[Dict]
    def calculate_change_percentage(self, current: float, previous: float) -> float
    def filter_by_volume(self, stocks: List[Dict], min_volume: int) -> List[Dict]
```

#### StorageManager
```python
class StorageManager:
    """Manages AWS S3 data storage"""
    def upload_csv_to_s3(self, df: pd.DataFrame, filename: str) -> bool
    def download_csv_from_s3(self, filename: str) -> pd.DataFrame
    def append_data_to_csv(self, new_data: List[Dict], filename: str) -> bool
```

#### NotificationManager
```python
class NotificationManager:
    """Handles AWS SNS notifications"""
    def send_screening_alert(self, qualified_stocks: List[Dict], timepoint: str) -> bool
    def format_alert_message(self, stocks: List[Dict], timepoint: str) -> str
```

### 4. Scheduler Implementation
- The Lambda function supports multiple timepoints, with the timepoint passed as an event parameter.
- Alerts are sent at each checkpoint (timepoint).
- The symbol list source is controlled by an event parameter: 'Static' uses a static list, 'Dynamic' fetches from Polygon.io.
- Errors are logged and major errors are sent to SNS; stocks with N/A data are ignored.

```python
# main.py example structure
import schedule
import time
from datetime import datetime
import pytz

def main():
    cdt = pytz.timezone('America/Chicago')
    # Schedule data collection at specific times
    schedule.every().day.at("08:37").do(run_screening)
    schedule.every().day.at("08:40").do(collect_timepoint_data, "08:40")
    schedule.every().day.at("08:50").do(collect_timepoint_data, "08:50")
    # ... other timepoints
    while True:
        schedule.run_pending()
        time.sleep(30)  # Check every 30 seconds
```

## Development Tips for Cursor

### Prompt Examples for Cursor AI

1. **Data Fetcher Implementation**:
   ```
   Create a DataFetcher class that uses the polygon-api-client to fetch real-time quotes, previous close, market cap, and average volume for NASDAQ stocks. Include error handling and rate limiting.
   ```

2. **Screening Logic**:
   ```
   Implement the StockScreener class with methods to filter stocks based on: volume > 300K, change >= 2.5%, prev_close >= $0.01, and open > prev_close. Return the filtered results as a list of dictionaries.
   ```

3. **AWS Integration**:
   ```
   Create StorageManager and NotificationManager classes using boto3 to handle S3 CSV operations and SNS notifications. Include proper error handling and logging.
   ```

4. **Scheduler Setup**:
   ```
   Implement a scheduler using the schedule library to run stock screening at 8:37 AM CDT and collect timepoint data at specified intervals. Handle timezone conversion properly.
   ```

### Testing Strategy

1. **Unit Tests**: Test each class method independently
2. **Integration Tests**: Test API connections and AWS services

## Usage

1. Set up environment variables
2. Install dependencies: `pip install -r requirements.txt`
3. Configure AWS credentials
4. Run the application: `python src/main.py`
5. Monitor logs for screening results and alerts

## Sample Output

When screening criteria are met, expect alerts like:
```
🚨 Stock Alert - 8:37 AM CDT
Found 47 qualifying stocks:

Top Movers:
TSLA: +4.2% ($187.50), Vol: 2.1M
NVDA: +3.8% ($425.75), Vol: 1.8M
AMD: +3.1% ($92.25), Vol: 890K

Full results saved to S3: screening-results-2024-01-15.csv
```

## Next Steps

1. Create python file that pulls data
2. Store data on existing AWS infrastructure
3. Test with small set of nasdaq stocks
4. Deploy to production environment
5. Monitor and optimize performance

---

*This README serves as a comprehensive guide for developing the Polygon stock screener with AI assistance. Use the provided structure and prompts with Cursor to accelerate development.*