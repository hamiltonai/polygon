# prefilter.py - Handles pre-filtering logic and criteria
import os
import pandas as pd
import logging
from config import MIN_MARKET_CAP_MILLIONS, MIN_PREVIOUS_CLOSE, S3_BUCKET
from utils import download_from_s3

logger = logging.getLogger(__name__)

class PrefilterManager:
    """Handles all pre-filtering operations and criteria"""
    
    def __init__(self):
        self.min_market_cap = MIN_MARKET_CAP_MILLIONS
        self.min_previous_close = MIN_PREVIOUS_CLOSE
        self.s3_bucket = S3_BUCKET
    
    def apply_prefilter(self, stock_data):
        """
        Apply pre-filtering criteria for 8:25 step:
        - calculated_market_cap >= MIN_MARKET_CAP_MILLIONS (default: $50M)
        - previous_close >= MIN_PREVIOUS_CLOSE (default: $3.00)
        """
        if not stock_data:
            return False
        
        # Check market cap
        calculated_market_cap = stock_data.get('calculated_market_cap')
        if calculated_market_cap is None or calculated_market_cap < self.min_market_cap:
            return False
        
        # Check previous close price
        close_price = stock_data.get('previous_close')
        if close_price is None or close_price < self.min_previous_close:
            return False
        
        return True
    
    def get_premarket_gainers_list(self, date_str):
        """Load premarket gainers list from local file or S3"""
        gainers_filename = f"premarket_top_gainers_{date_str}.csv"
        
        # Try to download from S3 if not local
        if not os.path.exists(gainers_filename) and self.s3_bucket:
            try:
                s3_key = f"stock_data/{date_str}/{gainers_filename}"
                download_from_s3(self.s3_bucket, s3_key, gainers_filename)
                logger.info(f"Downloaded {gainers_filename} from S3")
            except Exception as e:
                logger.debug(f"Could not download premarket gainers from S3: {e}")
        
        # Try to read the file
        if os.path.exists(gainers_filename):
            try:
                df = pd.read_csv(gainers_filename)
                
                # Look for ticker/symbol column
                symbol_col = None
                for col in ['ticker', 'symbol', 'Ticker', 'Symbol']:
                    if col in df.columns:
                        symbol_col = col
                        break
                
                if symbol_col:
                    gainers = set(df[symbol_col].dropna().astype(str).str.strip().str.upper())
                    logger.info(f"Loaded {len(gainers)} premarket top gainers from {gainers_filename}")
                    return gainers
                else:
                    logger.warning(f"No ticker/symbol column found in {gainers_filename}")
                    
            except Exception as e:
                logger.warning(f"Error reading premarket gainers file: {e}")
        else:
            logger.warning(f"Premarket gainers file not found: {gainers_filename}")
        
        return set()
    
    def add_top_gainer_flag(self, stock_data_list, premarket_gainers_set):
        """Add top_gainer flag to stock data"""
        for stock_data in stock_data_list:
            symbol = stock_data.get('symbol', '').upper()
            stock_data['top_gainer'] = symbol in premarket_gainers_set
        
        return stock_data_list
    
    def apply_filtering(self, stock_data_list, date_str):
        """
        Apply complete pre-filtering process:
        1. Load premarket gainers
        2. Add top_gainer flags
        3. Apply market cap and price filters
        4. Return filtered results with statistics
        """
        logger.info("Loading premarket gainers list for top_gainer flagging...")
        premarket_gainers = self.get_premarket_gainers_list(date_str)
        
        if premarket_gainers:
            logger.info(f"Found {len(premarket_gainers)} premarket top gainers")
            sample_gainers = list(premarket_gainers)[:10]
            logger.info(f"Sample premarket gainers: {', '.join(sample_gainers)}")
        else:
            logger.warning("No premarket gainers found - all stocks will have top_gainer=False")
        
        # Add top_gainer flag to all stocks
        stock_data_with_flags = self.add_top_gainer_flag(stock_data_list, premarket_gainers)
        
        # Apply pre-filtering
        original_count = len(stock_data_with_flags)
        filtered_results = []
        
        for stock_data in stock_data_with_flags:
            if self.apply_prefilter(stock_data):
                filtered_results.append(stock_data)
        
        # Count how many top gainers made it through pre-filtering
        top_gainer_count = sum(1 for stock in filtered_results if stock.get('top_gainer', False))
        
        logger.info(f"Pre-filtering: {len(filtered_results)}/{original_count} stocks passed (Market cap ≥${self.min_market_cap}M, Price ≥${self.min_previous_close})")
        logger.info(f"Top gainers in filtered dataset: {top_gainer_count}/{len(filtered_results)} stocks")
        
        return {
            'filtered_results': filtered_results,
            'original_count': original_count,
            'filtered_count': len(filtered_results),
            'top_gainer_count': top_gainer_count,
            'premarket_gainers_total': len(premarket_gainers),
            'criteria': {
                'min_market_cap': self.min_market_cap,
                'min_previous_close': self.min_previous_close
            }
        }
    
    def get_filtering_summary(self, filtering_results):
        """Generate summary text for filtering results"""
        filtered_count = filtering_results['filtered_count']
        original_count = filtering_results['original_count']
        top_gainer_count = filtering_results['top_gainer_count']
        premarket_gainers_total = filtering_results['premarket_gainers_total']
        criteria = filtering_results['criteria']
        
        return (
            f"Pre-filtered: {filtered_count}/{original_count} stocks\n"
            f"Criteria: Market cap ≥${criteria['min_market_cap']}M, Price ≥${criteria['min_previous_close']}\n"
            f"Top gainers in result: {top_gainer_count}/{filtered_count} stocks\n"
            f"Total premarket gainers found: {premarket_gainers_total}"
        )
    
    def get_top_gainers_in_filtered_results(self, filtered_results):
        """Get list of top gainer symbols that passed filtering"""
        return [stock['symbol'] for stock in filtered_results if stock.get('top_gainer', False)]
    
    def validate_filtering_criteria(self):
        """Validate that filtering criteria are reasonable"""
        errors = []
        
        if self.min_market_cap <= 0:
            errors.append(f"Invalid min_market_cap: {self.min_market_cap} (must be > 0)")
        
        if self.min_previous_close <= 0:
            errors.append(f"Invalid min_previous_close: {self.min_previous_close} (must be > 0)")
        
        if self.min_previous_close > 1000:
            errors.append(f"Warning: min_previous_close is very high: ${self.min_previous_close}")
        
        if self.min_market_cap > 10000:
            errors.append(f"Warning: min_market_cap is very high: ${self.min_market_cap}M")
        
        if errors:
            logger.warning("Filtering criteria validation issues:")
            for error in errors:
                logger.warning(f"  - {error}")
        
        return len(errors) == 0
    
    def get_filter_statistics(self, stock_data_list):
        """Get statistics about data before filtering"""
        if not stock_data_list:
            return {}
        
        market_caps = [stock.get('calculated_market_cap', 0) for stock in stock_data_list if stock.get('calculated_market_cap')]
        prev_closes = [stock.get('previous_close', 0) for stock in stock_data_list if stock.get('previous_close')]
        
        stats = {
            'total_stocks': len(stock_data_list),
            'with_market_cap': len(market_caps),
            'with_previous_close': len(prev_closes),
        }
        
        if market_caps:
            stats.update({
                'avg_market_cap': sum(market_caps) / len(market_caps),
                'min_market_cap': min(market_caps),
                'max_market_cap': max(market_caps),
                'market_cap_above_threshold': len([m for m in market_caps if m >= self.min_market_cap])
            })
        
        if prev_closes:
            stats.update({
                'avg_previous_close': sum(prev_closes) / len(prev_closes),
                'min_previous_close': min(prev_closes),
                'max_previous_close': max(prev_closes),
                'close_above_threshold': len([c for c in prev_closes if c >= self.min_previous_close])
            })
        
        return stats