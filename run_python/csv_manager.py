# csv_manager.py - Handles CSV operations, file management, and S3 uploads# csv_manager.py - Handles CSV operations, file management, and S3 uploads
import csv
import os
import logging
from config import S3_BUCKET, AWS_S3_ENABLED
from utils import upload_to_s3, format_number

logger = logging.getLogger(__name__)

class CSVManager:
    """Handles all CSV file operations and S3 management"""
    
    def __init__(self):
        self.s3_bucket = S3_BUCKET
        self.aws_enabled = AWS_S3_ENABLED
    
    def get_fieldnames_for_prefiltered_data(self):
        """Get fieldnames for pre-filtered data CSV (8:25 step)"""
        return [
            # Basic stock info
            'symbol', 
            'company_name', 
            'data_period',
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
            'calculated_market_cap'
        ]
    
    def get_fieldnames_for_regular_data(self):
        """Get fieldnames for regular data CSV (non-pre-filtered)"""
        return [
            # Basic stock info
            'symbol',
            'company_name',
            'data_period',
            
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
            'calculated_market_cap'
        ]
    
    def get_complete_fieldnames_after_8_50(self):
        """Get complete fieldnames after 8:50 pipeline completion"""
        return [
            # Basic stock info
            'symbol',
            'company_name',
            'data_period', 
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
    
    def determine_filename_and_fieldnames(self, date_str, is_prefilter_step):
        """Determine appropriate filename and fieldnames based on step type"""
        if is_prefilter_step:
            filename = f"filtered_raw_data_{date_str}.csv"
            fieldnames = self.get_fieldnames_for_prefiltered_data()
        else:
            filename = f"raw_data_{date_str}.csv"
            fieldnames = self.get_fieldnames_for_regular_data()
        
        return filename, fieldnames
    
    def save_to_csv(self, stock_data_list, date_str, is_prefilter_step=False):
        """
        Save stock data to CSV file
        
        Args:
            stock_data_list: List of stock data dictionaries
            date_str: Date string for filename
            is_prefilter_step: Whether this is the pre-filtering step
            
        Returns:
            str: Filename of saved CSV file
        """
        if not stock_data_list:
            raise ValueError("No stock data provided to save")
        
        # Determine filename and fieldnames
        filename, fieldnames = self.determine_filename_and_fieldnames(date_str, is_prefilter_step)
        
        logger.info(f"Saving {len(stock_data_list)} stocks to {filename}")
        
        try:
            # Sort by calculated market cap descending
            sorted_data = sorted(
                stock_data_list, 
                key=lambda x: -(x.get('calculated_market_cap', 0) or 0)
            )
            
            # Write CSV file
            with open(filename, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in sorted_data:
                    # Only include fields that are in our fieldnames
                    output_row = {k: row.get(k) for k in fieldnames}
                    writer.writerow(output_row)
            
            logger.info(f"Successfully saved {filename} with {len(sorted_data)} records")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving CSV file {filename}: {e}")
            raise
    
    def upload_to_s3(self, filename, date_str):
        """
        Upload CSV file to S3
        
        Args:
            filename: Local filename to upload
            date_str: Date string for S3 path
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.aws_enabled or not self.s3_bucket:
            logger.info("S3 upload disabled or no bucket configured")
            return True
        
        try:
            s3_key = f"stock_data/{date_str}/{filename}"
            
            if upload_to_s3(self.s3_bucket, s3_key, filename):
                logger.info(f"Successfully uploaded {filename} to S3: {s3_key}")
                return True
            else:
                logger.error(f"Failed to upload {filename} to S3")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading {filename} to S3: {e}")
            return False
    
    def save_and_upload(self, stock_data_list, date_str, is_prefilter_step=False):
        """
        Combined save to CSV and upload to S3 operation
        
        Args:
            stock_data_list: List of stock data dictionaries
            date_str: Date string for filename
            is_prefilter_step: Whether this is the pre-filtering step
            
        Returns:
            tuple: (success: bool, filename: str)
        """
        try:
            # Save to CSV
            filename = self.save_to_csv(stock_data_list, date_str, is_prefilter_step)
            
            # Upload to S3
            upload_success = self.upload_to_s3(filename, date_str)
            
            if not upload_success:
                logger.warning(f"CSV saved locally but S3 upload failed: {filename}")
                # Don't fail completely if S3 upload fails
            
            return True, filename
            
        except Exception as e:
            logger.error(f"Error in save_and_upload: {e}")
            return False, None
    
    def validate_stock_data_for_csv(self, stock_data_list, is_prefilter_step=False):
        """
        Validate stock data before saving to CSV
        
        Args:
            stock_data_list: List of stock data dictionaries
            is_prefilter_step: Whether this is the pre-filtering step
            
        Returns:
            tuple: (is_valid: bool, errors: list)
        """
        errors = []
        
        if not stock_data_list:
            errors.append("No stock data provided")
            return False, errors
        
        # Get expected fieldnames
        if is_prefilter_step:
            required_fields = ['symbol', 'company_name', 'previous_close', 'calculated_market_cap']
        else:
            required_fields = ['symbol', 'company_name', 'previous_close']
        
        # Check a sample of records
        sample_size = min(5, len(stock_data_list))
        for i, stock_data in enumerate(stock_data_list[:sample_size]):
            for field in required_fields:
                if field not in stock_data:
                    errors.append(f"Record {i}: Missing required field '{field}'")
                elif stock_data[field] is None:
                    errors.append(f"Record {i}: Field '{field}' is None")
        
        return len(errors) == 0, errors
    
    def get_csv_statistics(self, filename):
        """
        Get statistics about a saved CSV file
        
        Args:
            filename: CSV filename to analyze
            
        Returns:
            dict: Statistics about the CSV file
        """
        try:
            if not os.path.exists(filename):
                return {'error': 'File not found'}
            
            # Get file size
            file_size = os.path.getsize(filename)
            
            # Count rows (simple method)
            with open(filename, 'r') as f:
                line_count = sum(1 for line in f)
                row_count = line_count - 1  # Subtract header
            
            # Get column count from header
            with open(filename, 'r') as f:
                reader = csv.reader(f)
                headers = next(reader, [])
                column_count = len(headers)
            
            return {
                'filename': filename,
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'total_rows': row_count,
                'total_columns': column_count,
                'headers': headers
            }
            
        except Exception as e:
            logger.error(f"Error getting CSV statistics for {filename}: {e}")
            return {'error': str(e)}
    
    def cleanup_local_file(self, filename):
        """
        Clean up local CSV file after processing
        
        Args:
            filename: Local filename to remove
        """
        try:
            if os.path.exists(filename):
                os.remove(filename)
                logger.debug(f"Cleaned up local file: {filename}")
            else:
                logger.debug(f"File not found for cleanup: {filename}")
        except Exception as e:
            logger.warning(f"Failed to clean up {filename}: {e}")
    
    def backup_existing_file(self, filename):
        """
        Create a backup of existing file before overwriting
        
        Args:
            filename: Filename to backup
            
        Returns:
            str: Backup filename, or None if no backup created
        """
        try:
            if os.path.exists(filename):
                backup_filename = f"{filename}.backup"
                import shutil
                shutil.copy2(filename, backup_filename)
                logger.info(f"Created backup: {backup_filename}")
                return backup_filename
            return None
        except Exception as e:
            logger.warning(f"Failed to create backup of {filename}: {e}")
            return None
    
    def format_save_summary(self, filename, record_count, file_stats=None):
        """
        Format a summary message for save operations
        
        Args:
            filename: Saved filename
            record_count: Number of records saved
            file_stats: Optional file statistics
            
        Returns:
            str: Formatted summary message
        """
        summary = f"CSV File: {filename}\n"
        summary += f"Records: {format_number(record_count)}\n"
        
        if file_stats:
            summary += f"Size: {file_stats.get('file_size_mb', 'N/A')} MB\n"
            summary += f"Columns: {file_stats.get('total_columns', 'N/A')}\n"
        
        return summary