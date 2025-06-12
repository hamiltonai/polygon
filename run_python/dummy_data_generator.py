import pandas as pd
import numpy as np
import random
from datetime import datetime

# Set random seed for reproducible results
np.random.seed(42)
random.seed(42)

def generate_stock_dummy_data(num_stocks=100):
    """
    Generate dummy data with prefixed column names representing the final state 
    at 8:50 of the stock screening pipeline
    """
    
    # Sample stock symbols and company names
    stock_symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM',
        'QCOM', 'INTC', 'AMD', 'AVGO', 'CSCO', 'ORCL', 'IBM', 'TXN', 'INTU', 'AMAT',
        'LRCX', 'ADI', 'KLAC', 'MCHP', 'SNPS', 'CDNS', 'FTNT', 'TEAM', 'WDAY', 'OKTA',
        'ZM', 'DOCU', 'CRWD', 'ZS', 'DDOG', 'SNOW', 'NET', 'PLTR', 'U', 'PATH',
        'COIN', 'RBLX', 'HOOD', 'UPST', 'AFRM', 'SQ', 'PYPL', 'SHOP', 'SPOT', 'ROKU',
        'TWLO', 'UBER', 'LYFT', 'DASH', 'ABNB', 'PINS', 'SNAP', 'TWTR', 'MTCH', 'BMBL',
        'PTON', 'ZG', 'ZILLOW', 'REDFN', 'OPEN', 'COMP', 'RKT', 'UWMC', 'GHVI', 'IPOE',
        'CCIV', 'LCID', 'RIVN', 'F', 'GM', 'NKLA', 'HYLN', 'RIDE', 'WKHS', 'GOEV',
        'SPCE', 'ASTR', 'RKLB', 'PLL', 'BABA', 'JD', 'PDD', 'BIDU', 'BILI', 'IQ',
        'TME', 'FUTU', 'TIGR', 'YMM', 'VIPS', 'WB', 'BZUN', 'QFIN', 'LX', 'YY'
    ]
    
    company_names = [
        'Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.', 'Amazon.com Inc.', 'Tesla Inc.',
        'Meta Platforms Inc.', 'NVIDIA Corporation', 'Netflix Inc.', 'Adobe Inc.', 'Salesforce Inc.',
        'Qualcomm Inc.', 'Intel Corporation', 'Advanced Micro Devices', 'Broadcom Inc.', 'Cisco Systems',
        'Oracle Corporation', 'International Business Machines', 'Texas Instruments', 'Intuit Inc.', 'Applied Materials',
        'Lam Research Corp', 'Analog Devices Inc', 'KLA Corporation', 'Microchip Technology', 'Synopsys Inc',
        'Cadence Design Systems', 'Fortinet Inc', 'Atlassian Corporation', 'Workday Inc', 'Okta Inc',
        'Zoom Video Communications', 'DocuSign Inc', 'CrowdStrike Holdings', 'Zscaler Inc', 'Datadog Inc',
        'Snowflake Inc', 'Cloudflare Inc', 'Palantir Technologies', 'Unity Software Inc', 'UiPath Inc',
        'Coinbase Global Inc', 'Roblox Corporation', 'Robinhood Markets', 'Upstart Holdings', 'Affirm Holdings',
        'Block Inc', 'PayPal Holdings', 'Shopify Inc', 'Spotify Technology', 'Roku Inc',
        'Twilio Inc', 'Uber Technologies', 'Lyft Inc', 'DoorDash Inc', 'Airbnb Inc',
        'Pinterest Inc', 'Snap Inc', 'Twitter Inc', 'Match Group Inc', 'Bumble Inc',
        'Peloton Interactive', 'Zillow Group', 'Zillow Group A', 'Redfin Corporation', 'Opendoor Technologies',
        'Compass Inc', 'Rocket Companies', 'UWM Holdings', 'Gores Holdings VI', 'Social Capital Hedosophia',
        'Churchill Capital Corp IV', 'Lucid Group Inc', 'Rivian Automotive', 'Ford Motor Company', 'General Motors',
        'Nikola Corporation', 'Hyliion Holdings', 'Lordstown Motors', 'Workhorse Group', 'Canoo Inc',
        'Virgin Galactic Holdings', 'Astra Space Inc', 'Rocket Lab USA', 'Piedmont Lithium', 'Alibaba Group',
        'JD.com Inc', 'PDD Holdings', 'Baidu Inc', 'Bilibili Inc', 'iQIYI Inc',
        'Tencent Music Entertainment', 'Futu Holdings', 'UP Fintech Holding', 'Full Truck Alliance', 'VIPSHOP Holdings',
        'Weibo Corporation', 'Baozun Inc', 'Qifu Technology', 'Lexinfintech Holdings', 'JOYY Inc'
    ]
    
    data = []
    
    for i in range(num_stocks):
        symbol = stock_symbols[i] if i < len(stock_symbols) else f"STOCK{i:03d}"
        company_name = company_names[i] if i < len(company_names) else f"Company {i:03d} Inc."
        
        # Generate base stock data (previous day) with PREVIOUS_ prefix
        previous_close = np.random.uniform(3.0, 500.0)  # $3-500 (meets pre-filter)
        shares_outstanding = np.random.uniform(50_000_000, 10_000_000_000)  # 50M to 10B shares
        market_cap_millions = (previous_close * shares_outstanding) / 1_000_000
        
        # Ensure market cap meets pre-filter criteria (≥$50M)
        if market_cap_millions < 50:
            market_cap_millions = np.random.uniform(50, 1000)
            shares_outstanding = (market_cap_millions * 1_000_000) / previous_close
        
        # Previous day OHLCV with PREVIOUS_ prefix
        previous_open = previous_close * np.random.uniform(0.98, 1.02)
        previous_high = max(previous_open, previous_close) * np.random.uniform(1.0, 1.05)
        previous_low = min(previous_open, previous_close) * np.random.uniform(0.95, 1.0)
        previous_volume = np.random.uniform(100_000, 50_000_000)  # 100K to 50M shares
        
        # Previous day calculated fields
        previous_current_price = previous_close  # For previous day, current price = close
        previous_avg_volume = previous_volume
        previous_price_pct_change_from_open = ((previous_close - previous_open) / previous_open) * 100
        
        # Current day data progression
        # Start with some base change from previous close
        base_change_pct = np.random.uniform(-10, 70)  # Wide range
        
        # 8:37 price - this is where qualification happens
        today_price_8_37 = previous_close * (1 + base_change_pct / 100)
        
        # 8:37 volume - important for qualification
        today_volume_8_37 = previous_volume * np.random.uniform(0.8, 3.0)  # Can be different from prev day
        
        # Determine if stock qualifies at 8:37
        qualified_8_37 = False
        qualification_reason = "N/A"
        
        # Check 8:37 qualification criteria
        if today_volume_8_37 >= 1_000_000:  # Volume ≥ 1M
            change_pct = ((today_price_8_37 - previous_close) / previous_close) * 100
            if 5.0 <= change_pct <= 60.0:  # Gain 5-60%
                if today_price_8_37 > previous_open:  # Price > open
                    qualified_8_37 = True
                    qualification_reason = f"Qualified: Vol={today_volume_8_37/1_000_000:.1f}M, Gain={change_pct:.1f}%, Above open"
                else:
                    qualification_reason = f"Price not above open: ${today_price_8_37:.2f} <= ${previous_open:.2f}"
            else:
                if change_pct < 5.0:
                    qualification_reason = f"Price gain too low: {change_pct:.1f}% < 5.0%"
                else:
                    qualification_reason = f"Price gain too high: {change_pct:.1f}% > 60.0%"
        else:
            qualification_reason = f"Volume too low: {today_volume_8_37:,.0f} < 1,000,000"
        
        # 8:40 price and momentum (only for qualified stocks)
        today_price_8_40 = np.nan
        momentum_8_40 = False
        qualified_8_40 = False
        
        if qualified_8_37:
            # 8:40 price with some momentum (60% chance of maintaining/gaining)
            if np.random.random() < 0.6:
                today_price_8_40 = today_price_8_37 * np.random.uniform(1.0, 1.05)  # 0-5% gain
                momentum_8_40 = True
                qualified_8_40 = True
            else:
                today_price_8_40 = today_price_8_37 * np.random.uniform(0.95, 1.0)  # Small loss
                momentum_8_40 = False
                qualified_8_40 = False
        
        # 8:50 price and momentum (only for qualified 8:40 stocks)
        today_price_8_50 = np.nan
        momentum_8_50 = False
        qualified_8_50 = False
        
        if qualified_8_40:
            # 8:50 price with some momentum (50% chance of maintaining/gaining)
            if np.random.random() < 0.5:
                today_price_8_50 = today_price_8_40 * np.random.uniform(1.0, 1.03)  # 0-3% gain
                momentum_8_50 = True
                qualified_8_50 = True
            else:
                today_price_8_50 = today_price_8_40 * np.random.uniform(0.97, 1.0)  # Small loss
                momentum_8_50 = False
                qualified_8_50 = False
        
        # Top gainer flag (simulate some stocks being premarket gainers)
        top_gainer = np.random.random() < 0.15  # 15% chance
        
        # Create the row with prefixed column names
        row = {
            # Basic stock info
            'symbol': symbol,
            'company_name': company_name,
            'top_gainer': top_gainer,
            
            # Previous day OHLCV with PREVIOUS_ prefix
            'previous_open': round(previous_open, 2),
            'previous_high': round(previous_high, 2),
            'previous_low': round(previous_low, 2),
            'previous_close': round(previous_close, 2),
            'previous_volume': int(previous_volume),
            'previous_current_price': round(previous_current_price, 2),
            'previous_avg_volume': int(previous_avg_volume),
            'previous_price_pct_change_from_open': round(previous_price_pct_change_from_open, 2),
            
            # Market cap and shares data (not prefixed as it's static)
            'market_cap': int(market_cap_millions * 1_000_000),
            'share_class_shares_outstanding': int(shares_outstanding),
            'intraday_market_cap_millions': round(market_cap_millions, 2),
            'calculated_market_cap': round(market_cap_millions, 2),
            
            # 8:37 qualification data with TODAY_ prefix
            'today_price_8_37': round(today_price_8_37, 2),
            'today_volume_8_37': int(today_volume_8_37),
            'qualified_8_37': qualified_8_37,
            'qualification_reason': qualification_reason,
            
            # Helper columns for qualification (current_price and volume without prefix for compatibility)
            'current_price': round(today_price_8_37, 2),  # Latest available price
            'volume': int(today_volume_8_37),  # Latest available volume
            
            # 8:40 momentum data with TODAY_ prefix
            'today_price_8_40': round(today_price_8_40, 2) if not pd.isna(today_price_8_40) else np.nan,
            'momentum_8_40': momentum_8_40,
            'qualified_8_40': qualified_8_40,
            
            # 8:50 final momentum data with TODAY_ prefix
            'today_price_8_50': round(today_price_8_50, 2) if not pd.isna(today_price_8_50) else np.nan,
            'momentum_8_50': momentum_8_50,
            'qualified_8_50': qualified_8_50,
        }
        
        # Update current_price to latest available
        if not pd.isna(today_price_8_50):
            row['current_price'] = round(today_price_8_50, 2)
        elif not pd.isna(today_price_8_40):
            row['current_price'] = round(today_price_8_40, 2)
        
        data.append(row)
    
    return pd.DataFrame(data)

# Generate the dummy data
print("Generating dummy stock data with prefixed column names...")
df = generate_stock_dummy_data(100)

# Show filtering statistics
total_stocks = len(df)
qualified_837 = df['qualified_8_37'].sum()
qualified_840 = df['qualified_8_40'].sum()
qualified_850 = df['qualified_8_50'].sum()
top_gainers = df['top_gainer'].sum()

print(f"\nFILTERING PROGRESSION:")
print(f"Total pre-filtered stocks: {total_stocks}")
print(f"Premarket top gainers: {top_gainers}")
print(f"Qualified at 8:37: {qualified_837} ({qualified_837/total_stocks*100:.1f}%)")
print(f"Maintained momentum at 8:40: {qualified_840} ({qualified_840/total_stocks*100:.1f}%)")
print(f"Final buy list at 8:50: {qualified_850} ({qualified_850/total_stocks*100:.1f}%)")

# Save to CSV
current_date = datetime.now().strftime('%Y%m%d')
filename = f"dummy_filtered_raw_data_{current_date}.csv"
df.to_csv(filename, index=False)

print(f"\nDummy data exported to: {filename}")
print(f"Columns: {len(df.columns)}")
print(f"Rows: {len(df)}")

# Show column structure
print(f"\nCOLUMN STRUCTURE ({len(df.columns)} columns):")
print("\nBasic Info:")
basic_cols = [col for col in df.columns if not any(col.startswith(prefix) for prefix in ['previous_', 'today_']) and col not in ['current_price', 'volume']]
for i, col in enumerate(basic_cols, 1):
    print(f"  {i:2d}. {col}")

print(f"\nPrevious Day Data (previous_ prefix):")
prev_cols = [col for col in df.columns if col.startswith('previous_')]
for i, col in enumerate(prev_cols, 1):
    print(f"  {i:2d}. {col}")

print(f"\nToday's Data (today_ prefix):")
today_cols = [col for col in df.columns if col.startswith('today_')]
for i, col in enumerate(today_cols, 1):
    print(f"  {i:2d}. {col}")

print(f"\nQualification/Momentum Columns:")
qual_cols = [col for col in df.columns if any(word in col for word in ['qualified', 'momentum', 'qualification_reason'])]
for i, col in enumerate(qual_cols, 1):
    print(f"  {i:2d}. {col}")

print(f"\nHelper Columns (for compatibility):")
helper_cols = ['current_price', 'volume']
for i, col in enumerate(helper_cols, 1):
    print(f"  {i:2d}. {col}")

# Show sample of final buy list
buy_list = df[df['qualified_8_50'] == True]
if len(buy_list) > 0:
    print(f"\nFINAL BUY LIST ({len(buy_list)} stocks):")
    for i, (_, stock) in enumerate(buy_list.iterrows(), 1):
        symbol = stock['symbol']
        price_837 = stock['today_price_8_37']
        price_850 = stock['today_price_8_50']
        prev_close = stock['previous_close']
        gain_pct = ((price_850 - prev_close) / prev_close) * 100
        volume = stock['today_volume_8_37'] / 1_000_000
        mcap = stock['calculated_market_cap']
        
        if mcap >= 1000:
            mcap_str = f"${mcap/1000:.1f}B"
        else:
            mcap_str = f"${mcap:.0f}M"
        
        print(f"{i:2d}. {symbol}: ${price_837:.2f} → ${price_850:.2f} (+{gain_pct:.1f}%), Vol: {volume:.1f}M, MCap: {mcap_str}")
else:
    print(f"\nNo stocks in final buy list")

# Show sample data preview (first 3 rows, key columns only)
print(f"\nSAMPLE DATA PREVIEW (first 3 rows, key columns):")
key_cols = ['symbol', 'previous_close', 'today_price_8_37', 'today_price_8_40', 'today_price_8_50', 
           'qualified_8_37', 'qualified_8_40', 'qualified_8_50']
print(df[key_cols].head(3).to_string(index=False))

print(f"\nDummy data generation complete!")
print(f"Data now uses prefixed column names (previous_ and today_)")
print(f"This represents the final state of the pipeline at 8:50 AM")
print(f"Use this data to test the updated functions")