# initial_data_pull.py - Refactored main entry point using modular components
import logging
import sys
import os
from utils import get_date_str
from data_fetcher import DataFetcher
from data_transformer import DataTransformer
from prefilter import PrefilterManager
from csv_manager import CSVManager
from workflow_coordinator import WorkflowCoordinator

logger = logging.getLogger(__name__)

def run_initial_data_pull(date_str=None, max_symbols=None, force_previous_day=False):
    """
    Main data pull function using refactored modular approach
    
    Args:
        date_str: Date string for file naming (YYYYMMDD)
        max_symbols: Limit number of symbols (for testing)
        force_previous_day: Force previous day data regardless of time
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Initialize components
    fetcher = DataFetcher()
    transformer = DataTransformer()
    prefilter = PrefilterManager()
    csv_manager = CSVManager()
    coordinator = WorkflowCoordinator()
    
    try:
        # Create workflow context
        context = coordinator.create_workflow_context(date_str, max_symbols, force_previous_day)
        date_str = context['date_str']
        period_info = context['period_info']
        is_prefilter_step = context['is_prefilter_step']
        
        # Validate inputs and get symbols
        symbols = fetcher.get_symbols_with_fallback(max_symbols)
        
        is_valid, errors = coordinator.validate_inputs(symbols, period_info, date_str)
        if not is_valid:
            for error in errors:
                logger.error(f"Validation error: {error}")
            raise ValueError(f"Input validation failed: {'; '.join(errors)}")
        
        # Log workflow start
        coordinator.log_workflow_start(symbols, period_info, date_str)
        
        # Phase 1: Data Collection
        coordinator.update_progress(context['tracker'], "data_collection")
        logger.info(f"Phase 1: Collecting data for {len(symbols)} symbols...")
        
        raw_results = fetcher.fetch_all_stock_data(symbols, period_info, context['tracker']['stats'])
        
        if not raw_results:
            raise Exception("No complete records collected from data fetching")
        
        # Phase 2: Data Transformation
        coordinator.update_progress(context['tracker'], "data_transformation")
        logger.info(f"Phase 2: Transforming {len(raw_results)} records...")
        
        clean_results = transformer.transform_to_new_structure(raw_results)
        
        # Validate transformed data
        is_valid, errors = transformer.validate_transformed_data(clean_results)
        if not is_valid:
            logger.warning(f"Data transformation validation issues: {errors}")
        
        # Phase 3: Pre-filtering (if applicable)
        filtering_info = None
        if is_prefilter_step:
            coordinator.update_progress(context['tracker'], "pre_filtering")
            logger.info(f"Phase 3: Applying pre-filtering to {len(clean_results)} records...")
            
            filtering_results = prefilter.apply_filtering(clean_results, date_str)
            final_results = filtering_results['filtered_results']
            
            # Prepare filtering info for notifications
            filtering_info = filtering_results.copy()
            filtering_info['top_gainers_list'] = prefilter.get_top_gainers_in_filtered_results(final_results)
        else:
            logger.info("Phase 3: Skipping pre-filtering (not pre-filter step)")
            final_results = clean_results
        
        if not final_results:
            raise Exception("No stocks passed filtering criteria" if is_prefilter_step else "No final results after transformation")
        
        # Phase 4: Save Results
        coordinator.update_progress(context['tracker'], "saving_results")
        logger.info(f"Phase 4: Saving {len(final_results)} results...")
        
        # Validate data before saving
        is_valid, errors = csv_manager.validate_stock_data_for_csv(final_results, is_prefilter_step)
        if not is_valid:
            logger.warning(f"CSV validation issues: {errors}")
        
        # Save to CSV and upload to S3
        save_success, filename = csv_manager.save_and_upload(final_results, date_str, is_prefilter_step)
        
        if not save_success:
            raise Exception("Failed to save results to CSV or upload to S3")
        
        # Phase 5: Finalization
        coordinator.update_progress(context['tracker'], "finalization")
        logger.info("Phase 5: Finalizing workflow...")
        
        completion_info = coordinator.finalize_workflow(
            context, symbols, final_results, filename, filtering_info
        )
        
        # Log final summary
        summary = coordinator.get_workflow_summary(completion_info)
        logger.info(summary)
        
        return True
        
    except Exception as e:
        # Handle errors
        coordinator.handle_error(e, date_str or get_date_str(), "data_pull")
        return False

def run_prefiltered_data_pull(date_str=None, max_symbols=None):
    """
    Run the 8:25 pre-filtered data pull (previous day data with filtering)
    
    Args:
        date_str: Date string for file naming
        max_symbols: Limit number of symbols (for testing)
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Running 8:25 pre-filtered data pull (previous day data)")
    return run_initial_data_pull(date_str=date_str, max_symbols=max_symbols, force_previous_day=True)

def show_usage():
    """Show usage information"""
    print("Usage: python initial_data_pull.py [options]")
    print("")
    print("Options:")
    print("  --prefilter, --8:25    Run pre-filtered data pull (8:25 step)")
    print("  --help, -h            Show this help message")
    print("  [number]              Limit number of symbols for testing")
    print("")
    print("Examples:")
    print("  python initial_data_pull.py                 # Regular data pull")
    print("  python initial_data_pull.py --prefilter     # Pre-filtered data pull")
    print("  python initial_data_pull.py 100             # Test with 100 symbols")
    print("  python initial_data_pull.py --prefilter 50  # Pre-filtered test with 50 symbols")

def main():
    """Main entry point for command line usage"""
    from config import setup_logging, validate_config
    
    # Setup
    setup_logging()
    
    try:
        validate_config()
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        sys.exit(1)
    
    # Parse command line arguments
    max_symbols = None
    force_previous_day = False
    show_help = False
    
    for arg in sys.argv[1:]:
        if arg in ["--prefilter", "--8:25"]:
            force_previous_day = True
        elif arg in ["--help", "-h"]:
            show_help = True
        elif arg.isdigit():
            max_symbols = int(arg)
        else:
            print(f"Unknown argument: {arg}")
            show_help = True
    
    if show_help:
        show_usage()
        sys.exit(0)
    
    # Run the appropriate data pull
    try:
        if force_previous_day:
            success = run_prefiltered_data_pull(max_symbols=max_symbols)
            operation = "Pre-filtered data pull"
        else:
            success = run_initial_data_pull(max_symbols=max_symbols)
            operation = "Data pull"
        
        if success:
            print(f"✅ {operation} completed successfully")
            
            # Show additional info for pre-filtered pulls
            if force_previous_day:
                try:
                    import pandas as pd
                    filename = f"filtered_raw_data_{get_date_str()}.csv"
                    if os.path.exists(filename):
                        df = pd.read_csv(filename)
                        if 'top_gainer' in df.columns:
                            top_gainer_count = df['top_gainer'].sum()
                            print(f"📈 {top_gainer_count}/{len(df)} filtered stocks were premarket top gainers")
                            
                            if top_gainer_count > 0:
                                top_gainers = df[df['top_gainer'] == True]['symbol'].tolist()
                                print(f"🔥 Top gainers: {', '.join(top_gainers[:15])}")
                                if len(top_gainers) > 15:
                                    print(f"... and {len(top_gainers) - 15} more")
                except Exception:
                    pass  # Don't fail if we can't show the summary
        else:
            print(f"❌ {operation} failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⏹️ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()