# data_transformer.py - Handles data structure transformation and validation
import logging
from utils import calculate_market_cap_millions

logger = logging.getLogger(__name__)

class DataTransformer:
    """Handles all data transformation and validation operations"""
    
    def __init__(self):
        self.required_fields = [
            'previous_open', 'previous_high', 'previous_low', 
            'previous_close', 'previous_volume'
        ]
    
    def is_complete_record(self, stock_data):
        """
        Check if record has essential data using new column names
        
        Args:
            stock_data: Stock data dictionary
            
        Returns:
            bool: True if record is complete
        """
        if not stock_data:
            return False
        
        # Check essential fields
        for field in self.required_fields:
            value = stock_data.get(field)
            if value is None or value <= 0:
                return False
        
        # Sanity check: low <= open/close <= high
        low = stock_data.get('previous_low')
        high = stock_data.get('previous_high')
        open_price = stock_data.get('previous_open')
        close = stock_data.get('previous_close')
        
        if not (low <= open_price <= high and low <= close <= high):
            return False
        
        return True
    
    def calculate_previous_day_percentage(self, open_price, close_price):
        """
        Calculate percentage change for previous day
        
        Args:
            open_price: Opening price
            close_price: Closing price
            
        Returns:
            float: Percentage change
        """
        if not open_price or open_price <= 0:
            return 0.0
        
        return ((close_price - open_price) / open_price) * 100
    
    def calculate_market_cap_data(self, stock_data):
        """
        Calculate market cap related fields
        
        Args:
            stock_data: Stock data dictionary
            
        Returns:
            dict: Updated market cap data
        """
        current_price = stock_data.get('previous_close')
        shares_outstanding = stock_data.get('share_class_shares_outstanding')
        market_cap = stock_data.get('market_cap')
        
        calculated_market_cap = None
        intraday_market_cap_millions = None
        
        # Calculate using shares outstanding
        if shares_outstanding and current_price and shares_outstanding > 0:
            calculated_market_cap = calculate_market_cap_millions(current_price, shares_outstanding)
            intraday_market_cap_millions = calculated_market_cap
        # Fallback to provided market cap
        elif market_cap and current_price and current_price > 0:
            intraday_market_cap_millions = market_cap / 1_000_000
            calculated_market_cap = intraday_market_cap_millions
            # Estimate shares from market cap
            shares_outstanding = market_cap / current_price
        
        return {
            'calculated_market_cap': calculated_market_cap,
            'intraday_market_cap_millions': intraday_market_cap_millions,
            'share_class_shares_outstanding': shares_outstanding
        }
    
    def transform_single_record(self, raw_stock_data):
        """
        Transform a single stock record to the new structure
        
        Args:
            raw_stock_data: Raw stock data dictionary
            
        Returns:
            dict: Transformed stock data, or None if invalid
        """
        if not self.is_complete_record(raw_stock_data):
            return None
        
        # Start with the raw data (it should already be in the correct format)
        transformed_data = raw_stock_data.copy()
        
        # Ensure required derived fields are calculated
        open_price = transformed_data.get('previous_open')
        close_price = transformed_data.get('previous_close')
        
        # Calculate previous day percentage if not present
        if 'previous_price_pct_change_from_open' not in transformed_data:
            pct_change = self.calculate_previous_day_percentage(open_price, close_price)
            transformed_data['previous_price_pct_change_from_open'] = pct_change
        
        # Ensure previous_current_price matches previous_close
        if 'previous_current_price' not in transformed_data:
            transformed_data['previous_current_price'] = close_price
        
        # Ensure previous_avg_volume matches previous_volume
        if 'previous_avg_volume' not in transformed_data:
            transformed_data['previous_avg_volume'] = transformed_data.get('previous_volume')
        
        # Calculate market cap data
        market_cap_data = self.calculate_market_cap_data(transformed_data)
        transformed_data.update(market_cap_data)
        
        return transformed_data
    
    def transform_to_new_structure(self, raw_stock_data_list):
        """
        Transform list of stock data to new structure
        
        Args:
            raw_stock_data_list: List of raw stock data dictionaries
            
        Returns:
            list: List of transformed stock data dictionaries
        """
        transformed_results = []
        skipped_count = 0
        
        for raw_data in raw_stock_data_list:
            transformed = self.transform_single_record(raw_data)
            
            if transformed:
                transformed_results.append(transformed)
            else:
                skipped_count += 1
        
        logger.info(f"Transformed {len(transformed_results)} records, skipped {skipped_count} incomplete records")
        
        return transformed_results
    
    def validate_transformed_data(self, stock_data_list):
        """
        Validate transformed stock data
        
        Args:
            stock_data_list: List of transformed stock data
            
        Returns:
            tuple: (is_valid: bool, errors: list)
        """
        errors = []
        
        if not stock_data_list:
            errors.append("No transformed data provided")
            return False, errors
        
        # Check required fields in sample records
        sample_size = min(3, len(stock_data_list))
        required_transformed_fields = [
            'symbol', 'company_name', 'previous_close', 
            'calculated_market_cap', 'previous_price_pct_change_from_open'
        ]
        
        for i, stock_data in enumerate(stock_data_list[:sample_size]):
            for field in required_transformed_fields:
                if field not in stock_data:
                    errors.append(f"Record {i}: Missing transformed field '{field}'")
                elif stock_data[field] is None and field != 'calculated_market_cap':
                    errors.append(f"Record {i}: Transformed field '{field}' is None")
        
        return len(errors) == 0, errors
    
    def add_today_reference_price(self, stock_data_list, today_open_override=None):
        """
        Add today's opening price reference to stock data
        
        Args:
            stock_data_list: List of stock data dictionaries
            today_open_override: Optional override for today's open price
            
        Returns:
            list: Updated stock data with todays_open field
        """
        for stock_data in stock_data_list:
            if today_open_override is not None:
                stock_data['todays_open'] = today_open_override
            else:
                # Use previous close as proxy for today's open if not available
                # This will be updated with real data during qualification step
                stock_data['todays_open'] = stock_data.get('previous_close')
        
        return stock_data_list
    
    def calculate_percentage_change(self, current_price, previous_price):
        """
        Calculate percentage change between two prices
        
        Args:
            current_price: Current price
            previous_price: Previous price
            
        Returns:
            float: Percentage change, 0.0 if invalid inputs
        """
        if previous_price is None or previous_price <= 0:
            return 0.0
        if current_price is None:
            return 0.0
        
        return ((current_price - previous_price) / previous_price) * 100
    
    def add_today_percentage_fields(self, stock_data, time_step):
        """
        Add today_percentage_XX_XX field to stock data
        
        Args:
            stock_data: Stock data dictionary
            time_step: Time step (e.g., '8_37', '8_40', '8_50')
            
        Returns:
            dict: Updated stock data with percentage field
        """
        current_price_field = f"today_price_{time_step}"
        percentage_field = f"today_percentage_{time_step}"
        
        current_price = stock_data.get(current_price_field)
        
        if current_price is None:
            stock_data[percentage_field] = 0.0
            return stock_data
        
        # Determine reference price based on time step
        if time_step == '8_37':
            reference_price = stock_data.get('todays_open')
        elif time_step == '8_40':
            reference_price = stock_data.get('today_price_8_37')
        elif time_step == '8_50':
            reference_price = stock_data.get('today_price_8_40')
        else:
            reference_price = None
        
        # Calculate percentage change
        pct_change = self.calculate_percentage_change(current_price, reference_price)
        stock_data[percentage_field] = pct_change
        
        return stock_data
    
    def get_transformation_summary(self, original_count, transformed_count):
        """
        Get summary of transformation results
        
        Args:
            original_count: Original record count
            transformed_count: Transformed record count
            
        Returns:
            dict: Transformation summary
        """
        skipped_count = original_count - transformed_count
        success_rate = (transformed_count / original_count) * 100 if original_count > 0 else 0
        
        return {
            'original_count': original_count,
            'transformed_count': transformed_count,
            'skipped_count': skipped_count,
            'success_rate': success_rate
        }
    
    def clean_numeric_field(self, value, field_name, min_value=0, max_value=None):
        """
        Clean and validate a numeric field
        
        Args:
            value: Value to clean
            field_name: Field name for logging
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            
        Returns:
            float: Cleaned value, or None if invalid
        """
        try:
            if value is None:
                return None
            
            # Convert to float
            numeric_value = float(value)
            
            # Check minimum value
            if min_value is not None and numeric_value < min_value:
                logger.debug(f"Field {field_name}: value {numeric_value} below minimum {min_value}")
                return None
            
            # Check maximum value
            if max_value is not None and numeric_value > max_value:
                logger.debug(f"Field {field_name}: value {numeric_value} above maximum {max_value}")
                return None
            
            return numeric_value
            
        except (ValueError, TypeError):
            logger.debug(f"Field {field_name}: invalid numeric value {value}")
            return None
    
    def clean_stock_data_fields(self, stock_data):
        """
        Clean and validate all numeric fields in stock data
        
        Args:
            stock_data: Stock data dictionary
            
        Returns:
            dict: Cleaned stock data
        """
        cleaned_data = stock_data.copy()
        
        # Price fields (must be positive)
        price_fields = [
            'previous_open', 'previous_high', 'previous_low', 'previous_close',
            'previous_current_price', 'today_price_8_37', 'today_price_8_40', 'today_price_8_50'
        ]
        
        for field in price_fields:
            if field in cleaned_data:
                cleaned_value = self.clean_numeric_field(
                    cleaned_data[field], field, min_value=0.01, max_value=100000
                )
                cleaned_data[field] = cleaned_value
        
        # Volume fields (must be positive integers)
        volume_fields = ['previous_volume', 'previous_avg_volume', 'today_volume_8_37']
        
        for field in volume_fields:
            if field in cleaned_data:
                cleaned_value = self.clean_numeric_field(
                    cleaned_data[field], field, min_value=1, max_value=1e12
                )
                if cleaned_value is not None:
                    cleaned_data[field] = int(cleaned_value)
        
        # Market cap fields (can be large)
        market_cap_fields = ['market_cap', 'calculated_market_cap', 'intraday_market_cap_millions']
        
        for field in market_cap_fields:
            if field in cleaned_data:
                cleaned_value = self.clean_numeric_field(
                    cleaned_data[field], field, min_value=0, max_value=1e15
                )
                cleaned_data[field] = cleaned_value
        
        # Share count fields
        if 'share_class_shares_outstanding' in cleaned_data:
            cleaned_value = self.clean_numeric_field(
                cleaned_data['share_class_shares_outstanding'], 
                'share_class_shares_outstanding', 
                min_value=1, max_value=1e15
            )
            if cleaned_value is not None:
                cleaned_data['share_class_shares_outstanding'] = int(cleaned_value)
        
        return cleaned_data