import pandas as pd
import numpy as np
import random
from datetime import datetime

# Set random seed for reproducible results
np.random.seed(42)
random.seed(42)

def generate_stock_dummy_data(num_stocks=100):
    """
    Generate dummy data representing the final state at 8:50 of the stock screening pipeline
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
        
        # Generate base stock data (previous day)
        previous_close = np.random.uniform(3.0, 500.0)  # $3-500 (meets pre-filter)
        shares_outstanding = np.random.uniform(50_000_000, 10_000_000_000)  # 50M to 10B shares
        market_cap_millions = (previous_close * shares_outstanding) / 1_000_000
        
        # Ensure market cap meets pre-filter criteria (≥$50M)
        if market_cap_millions < 50:
            market_cap_millions = np.random.uniform(50, 1000)
            shares_outstanding = (market_cap_millions * 1_000_000) / previous_close
        
        # Previous day OHLCV
        previous_open = previous_close * np.random.uniform(0.98, 1.02)
        previous_high = max(previous_open, previous_close) * np.random.uniform(1.0, 1.05)
        previous_low = min(previous_open, previous_close) * np.random.uniform(0.95, 1.0)
        previous_volume = np.random.uniform(100_000, 50_000_000)  # 100K to 50M shares
        previous_current_price = previous_close  # This is the close price from previous day
        previous_avg_volume = previous_volume
        
        # Calculate previous day percentage change
        previous_price_pct_change_from_open = ((previous_close - previous_open) / previous_open) * 100
        
        # Today's open (for current day calculations)
        todays_open = previous_close * np.random.uniform(0.95, 1.10)  # Gap up/down from previous close
        
        # Current day data progression
        # Start with some base change from today's open
        base_change_pct = np.random.uniform(-10, 70)  # Wide range
        
        # 8:37 price - this is where qualification happens
        today_price_8_37 = todays_open * (1 + base_change_pct / 100)
        
        # Calculate 8:37 percentage change (vs today's open)
        today_percentage_8_37 = ((today_price_8_37 - todays_open) / todays_open) * 100
        
        # 8:37 volume - important for qualification
        today_volume_8_37 = previous_volume * np.random.uniform(0.8, 3.0)  # Can be different from prev day
        
        # Determine if stock qualifies at 8:37
        qualified_8_37 = False
        qualification_reason = "N/A"
        
        # Check 8:37 qualification criteria
        if today_volume_8_37 >= 1_000_000:  # Volume ≥ 1M
            change_pct_from_prev_close = ((today_price_8_37 - previous_close) / previous_close) * 100
            if 5.0 <= change_pct_from_prev_close <= 60.0:  # Gain 5-60% from previous close
                if today_price_8_37 > todays_open:  # Price > today's open
                    qualified_8_37 = True
                    qualification_reason = f"Qualified: Vol={today_volume_8_37/1_000_000:.1f}M, Gain={change_pct_from_prev_close:.1f}%, Above open"
                else:
                    qualification_reason = f"Price not above open: ${today_price_8_37:.2f} <= ${todays_open:.2f}"
            else:
                if change_pct_from_prev_close < 5.0:
                    qualification_reason = f"Price gain too low: {change_pct_from_prev_close:.1f}% < 5.0%"
                else:
                    qualification_reason = f"Price gain too high: {change_pct_from_prev_close:.1f}% > 60.0%"
        else:
            qualification_reason = f"Volume too low: {today_volume_8_37:,.0f} < 1,000,000"
        
        # 8:40 price and momentum (only for qualified stocks)
        today_price_8_40 = np.nan
        today_percentage_8_40 = np.nan
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
            
            # Calculate 8:40 percentage change (vs 8:37 price)
            today_percentage_8_40 = ((today_price_8_40 - today_price_8_37) / today_price_8_37) * 100
        
        # 8:50 price and momentum (only for qualified 8:40 stocks)
        today_price_8_50 = np.nan
        today_percentage_8_50 = np.nan
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
            
            # Calculate 8:50 percentage change (vs 8:40 price)
            today_percentage_8_50 = ((today_price_8_50 - today_price_8_40) / today_price_8_40) * 100
        
        # Top gainer flag (simulate some stocks being premarket gainers)
        top_gainer = np.random.random() < 0.15  # 15% chance
        
        # Create the row with proper column naming
        row = {
            # Basic stock info
            'symbol': symbol,
            'company_name': company_name,
            'top_gainer': top_gainer,
            
            # Previous day OHLCV data
            'previous_open': round(previous_open, 2),
            'previous_high': round(previous_high, 2),
            'previous_low': round(previous_low, 2),
            'previous_close': round(previous_close, 2),
            'previous_volume': int(previous_volume),
            'previous_current_price': round(previous_current_price, 2),
            'previous_avg_volume': int(previous_avg_volume),
            'previous_price_pct_change_from_open': round(previous_price_pct_change_from_open, 2),
            
            # Market cap and share data
            'market_cap': int(market_cap_millions * 1_000_000),
            'share_class_shares_outstanding': int(shares_outstanding),
            'intraday_market_cap_millions': round(market_cap_millions, 2),
            'calculated_market_cap': round(market_cap_millions, 2),
            
            # Today's reference price (open)
            'todays_open': round(todays_open, 2),
            
            # 8:37 qualification data
            'today_price_8_37': round(today_price_8_37, 2),
            'today_percentage_8_37': round(today_percentage_8_37, 2),
            'today_volume_8_37': int(today_volume_8_37),
            'qualified_8_37': qualified_8_37,
            'qualification_reason': qualification_reason,
            
            # 8:40 momentum data
            'today_price_8_40': round(today_price_8_40, 2) if not pd.isna(today_price_8_40) else np.nan,
            'today_percentage_8_40': round(today_percentage_8_40, 2) if not pd.isna(today_percentage_8_40) else np.nan,
            'momentum_8_40': momentum_8_40,
            'qualified_8_40': qualified_8_40,
            
            # 8:50 final momentum data
            'today_price_8_50': round(today_price_8_50, 2) if not pd.isna(today_price_8_50) else np.nan,
            'today_percentage_8_50': round(today_percentage_8_50, 2) if not pd.isna(today_percentage_8_50) else np.nan,
            'momentum_8_50': momentum_8_50,
            'qualified_8_50': qualified_8_50,
        }
        
        data.append(row)
    
    return pd.DataFrame(data)

def get_expected_headers_after_8_50():
    """
    Return the expected column headers after the 8:50 pipeline completion
    """
    return [
        # Basic stock info
        'symbol',
        'company_name', 
        'top_gainer',
        
        # Previous day OHLCV data
        'previous_open',
        'previous_high',
        'previous_low', 
        'previous_close',
        'previous_volume',
        'previous_current_price',
        'previous_avg_volume',
        'previous_price_pct_change_from_open',
        
        # Market cap and share data
        'market_cap',
        'share_class_shares_outstanding',
        'intraday_market_cap_millions',
        'calculated_market_cap',
        
        # Today's reference price
        'todays_open',
        
        # 8:37 qualification data
        'today_price_8_37',
        'today_percentage_8_37',
        'today_volume_8_37', 
        'qualified_8_37',
        'qualification_reason',
        
        # 8:40 momentum data
        'today_price_8_40',
        'today_percentage_8_40',
        'momentum_8_40',
        'qualified_8_40',
        
        # 8:50 final momentum data
        'today_price_8_50',
        'today_percentage_8_50',
        'momentum_8_50',
        'qualified_8_50'
    ]

# Generate the dummy data
if __name__ == "__main__":
    print("Generating dummy stock data with updated column structure...")
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
    
    # Show final buy list
    buy_list = df[df['qualified_8_50'] == True]
    if len(buy_list) > 0:
        print(f"\nFINAL BUY LIST ({len(buy_list)} stocks):")
        for i, (_, stock) in enumerate(buy_list.iterrows(), 1):
            symbol = stock['symbol']
            price_837 = stock['today_price_8_37']
            price_850 = stock['today_price_8_50']
            pct_837 = stock['today_percentage_8_37']
            pct_850 = stock['today_percentage_8_50']
            
            print(f"{i:2d}. {symbol}: 8:37 +{pct_837:.1f}% → 8:50 +{pct_850:.1f}%")
    else:
        print(f"\nNo stocks in final buy list")
    
    # Print expected headers
    print(f"\nEXPECTED HEADERS AFTER 8:50 ({len(get_expected_headers_after_8_50())} columns):")
    for i, header in enumerate(get_expected_headers_after_8_50(), 1):
        print(f"{i:2d}. {header}")