import os
import boto3
import pandas as pd
from datetime import datetime
import pytz
from polygon import RESTClient
from polygon.rest.models import TickerSnapshot
import json
from io import StringIO

def get_static_nasdaq_symbols():
    # Example static list; replace with your actual static list
    return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

def get_dynamic_nasdaq_symbols(client, limit=100):
    # Fetch NASDAQ symbols from Polygon (example: all tickers with primary_exchange 'XNAS')
    symbols = []
    for result in client.list_tickers(market="stocks", exchange="XNAS"):
        symbols.append(result.ticker)
        if len(symbols) >= limit:
            break
    return symbols

def fetch_stock_data(client, symbol):
    try:
        snapshot = client.get_snapshot_ticker(symbol)
        if not snapshot or not snapshot.day:
            return None
        open_price = snapshot.day.o
        high = snapshot.day.h
        low = snapshot.day.l
        close = snapshot.day.c
        volume = snapshot.day.v
        prev_close = snapshot.prev_day.c if snapshot.prev_day else None
        market_cap = snapshot.market_cap
        avg_volume = snapshot.ticker.avg_volume
        if None in [open_price, high, low, close, volume, prev_close, market_cap, avg_volume]:
            return None
        change_pct = ((open_price - prev_close) / prev_close) * 100 if prev_close else None
        return {
            "symbol": symbol,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "prev_close": prev_close,
            "market_cap": market_cap,
            "avg_volume": avg_volume,
            "change_pct": change_pct
        }
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def apply_screening_criteria(stock):
    try:
        return (
            stock["volume"] > 300_000 and
            stock["change_pct"] is not None and stock["change_pct"] >= 2.5 and
            stock["prev_close"] >= 0.01 and
            stock["open"] > stock["prev_close"]
        )
    except Exception:
        return False

def lambda_handler(event, context):
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")
    TIMEZONE = os.getenv("TIMEZONE", "America/Chicago")
    s3 = boto3.client("s3")
    sns = boto3.client("sns")
    client = RESTClient(POLYGON_API_KEY)

    # Get timepoint and symbol_mode from event
    timepoint = event.get("timepoint", datetime.now(pytz.timezone(TIMEZONE)).strftime("%H:%M"))
    symbol_mode = event.get("symbol_mode") or "Dynamic"

    # Get symbol list
    if symbol_mode == "Dynamic":
        symbols = get_dynamic_nasdaq_symbols(client, limit=100)
    else:
        symbols = get_static_nasdaq_symbols()

    # Fetch and screen data
    stocks_data = []
    for symbol in symbols:
        data = fetch_stock_data(client, symbol)
        if data is not None:
            stocks_data.append(data)
    # Ignore stocks with N/A data (already filtered)
    # Apply screening criteria
    qualified = [s for s in stocks_data if apply_screening_criteria(s)]

    # Prepare DataFrame for S3
    date_str = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
    timestamp_cdt = timepoint + ":00"
    for s in stocks_data:
        s["date"] = date_str
        s["timestamp_cdt"] = timestamp_cdt
    df = pd.DataFrame(stocks_data)
    # Reorder columns to match schema
    columns = ["symbol", "date", "timestamp_cdt", "open", "high", "low", "close", "volume", "prev_close", "market_cap", "avg_volume", "change_pct"]
    df = df[columns]

    # S3 file path
    s3_key = f"stock_data/{date_str}/screening-results-{date_str}.csv"
    # Download existing CSV if exists, append new data
    try:
        csv_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        existing_df = pd.read_csv(csv_obj["Body"])
        df = pd.concat([existing_df, df], ignore_index=True)
    except s3.exceptions.NoSuchKey:
        pass  # No existing file, will create new
    except Exception as e:
        print(f"Error reading existing CSV: {e}")
    # Write to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())

    # Format alert message
    def format_alert_message(stocks, timepoint):
        if not stocks:
            return f"No qualifying stocks at {timepoint} CDT."
        msg = f"🚨 Stock Alert - {timepoint} CDT\nFound {len(stocks)} qualifying stocks:\n\n"
        for s in stocks[:10]:
            msg += f"{s['symbol']}: {s['change_pct']:+.2f}% (${s['open']:.2f}), Vol: {int(s['volume']):,}\n"
        msg += f"\nFull results saved to S3: {s3_key}"
        return msg

    # Send SNS alert
    try:
        alert_message = format_alert_message(qualified, timepoint)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"Stock Screening Results - {timepoint} CDT",
            Message=alert_message
        )
    except Exception as e:
        print(f"Error sending SNS alert: {e}")

    return {
        "statusCode": 200,
        "body": {
            "message": f"Screening complete for {timepoint} CDT.",
            "qualified_count": len(qualified),
            "s3_key": s3_key
        }
    }
