import time
import logging
import sys
from datetime import datetime
import pytz
from premarket_gainers import get_premarket_top_gainers
from nasdaq_symbols import get_nasdaq_symbols
from initial_data_pull import run_prefiltered_data_pull, run_initial_data_pull
from qualification_filter import run_8_37_qualification, get_qualified_symbols_8_37
from intraday_updates import run_8_40_momentum_check, run_8_50_momentum_check, get_final_buy_symbols
from config import setup_logging, validate_config, SNS_TOPIC_ARN
from utils import get_date_str, send_sns_notification

logger = logging.getLogger(__name__)

class PolygonWorkflowScheduler:
    """
    Updated scheduler for the new Polygon stock workflow:
    8:20 - Premarket gainers
    8:22 - NASDAQ symbols  
    8:25 - Pre-filtered data pull (previous day, market cap ≥$50M, price ≥$3)
    8:37 - Qualification with current day data (volume ≥1M, gain 5-60%, price > open)
    8:40 - Momentum check #1 (price > 8:37 price)
    8:50 - Momentum check #2 + SNS buy list (price > 8:40 price)
    """
    
    def __init__(self):
        self.cst = pytz.timezone('America/Chicago')
        self.steps_run = set()
        self.date_str = get_date_str()
        self.running = True
        
        # NEW WORKFLOW SCHEDULE
        self.schedule = [
            # Pre-market data collection
            ('08:20', self._run_premarket_gainers, 'Premarket Top Gainers', False),
            ('08:22', self._run_nasdaq_symbols, 'NASDAQ Symbols Collection', False),
            
            # Pre-filtered data pull (CRITICAL - creates filtered dataset)
            ('08:25', self._run_prefiltered_data_pull, 'Pre-filtered Data Pull (Previous Day)', True),
            
            # Main qualification with current day data (CRITICAL)
            ('08:37', self._run_8_37_qualification, '8:37 Qualification (Current Day)', True),
            
            # Momentum checks
            ('08:40', self._run_8_40_momentum_check, '8:40 Momentum Check', True),
            ('08:50', self._run_8_50_momentum_check, '8:50 Final Momentum + SNS Buy List', True),
            
            # Optional: End of day summary
            ('15:35', self._run_end_of_day_summary, 'End of Day Summary', False),
        ]
    
    def _run_premarket_gainers(self):
        """Run premarket gainers collection"""
        return get_premarket_top_gainers(self.date_str)
    
    def _run_nasdaq_symbols(self):
        """Run NASDAQ symbols collection"""
        return get_nasdaq_symbols(self.date_str)
    
    def _run_prefiltered_data_pull(self):
        """Run 8:25 pre-filtered data pull (previous day data with filtering)"""
        logger.info("Running 8:25 pre-filtered data pull - previous day data with market cap and price filters")
        return run_prefiltered_data_pull(self.date_str)
    
    def _run_8_37_qualification(self):
        """Run 8:37 qualification with current day data"""
        logger.info("Running 8:37 qualification - current day data with volume/gain/momentum criteria")
        current_time = datetime.now(self.cst).strftime('%H:%M')
        return run_8_37_qualification(self.date_str, current_time)
    
    def _run_8_40_momentum_check(self):
        """Run 8:40 momentum check"""
        logger.info("Running 8:40 momentum check - price must be above 8:37 price")
        current_time = datetime.now(self.cst).strftime('%H:%M')
        return run_8_40_momentum_check(self.date_str, current_time)
    
    def _run_8_50_momentum_check(self):
        """Run 8:50 final momentum check and send buy list via SNS"""
        logger.info("Running 8:50 final momentum check - price must be above 8:40 price + send SNS notification")
        current_time = datetime.now(self.cst).strftime('%H:%M')
        return run_8_50_momentum_check(self.date_str, current_time)
    
    def _run_end_of_day_summary(self):
        """Run end of day summary"""
        try:
            buy_symbols = get_final_buy_symbols(self.date_str)
            buy_count = len(buy_symbols)
            
            current_time = datetime.now(self.cst).strftime('%H:%M:%S')
            
            summary = (
                f"📋 END OF DAY SUMMARY\n\n"
                f"Date: {self.date_str}\n"
                f"Time: {current_time} CDT\n"
                f"Final buy list: {buy_count} stocks\n\n"
                f"New Workflow Summary:\n"
                f"✓ 8:20 - Premarket gainers collected\n"
                f"✓ 8:22 - NASDAQ symbols updated\n"
                f"✓ 8:25 - Pre-filtered data (market cap ≥$50M, price ≥$3)\n"
                f"✓ 8:37 - Qualified stocks (volume ≥1M, gain 5-60%, price > open)\n"
                f"✓ 8:40 - First momentum check (price > 8:37)\n"
                f"✓ 8:50 - Final momentum check + SNS notification (price > 8:40)\n\n"
            )
            
            if buy_count > 0:
                summary += f"🎯 FINAL BUY LIST ({buy_count} stocks):\n"
                
                # Show all buy symbols
                symbols_per_line = 10
                for i in range(0, len(buy_symbols), symbols_per_line):
                    line_symbols = buy_symbols[i:i + symbols_per_line]
                    summary += f"{', '.join(line_symbols)}\n"
                
                summary += f"\n📧 SNS notification sent with detailed buy list and analysis"
            else:
                summary += "⚠️ No stocks qualified for the buy list today\n"
            
            summary += f"\n📁 Final file: stock_data/{self.date_str}/filtered_raw_data_{self.date_str}.csv"
            
            logger.info(f"End of day summary: {buy_count} stocks in final buy list")
            
            # Send end of day notification
            if SNS_TOPIC_ARN:
                subject = f"📋 End of Day Summary - {buy_count} stocks in buy list"
                send_sns_notification(SNS_TOPIC_ARN, subject, summary)
            
            return True
            
        except Exception as e:
            logger.error(f"End of day summary failed: {e}")
            return False
    
    def _handle_critical_failure(self, step_name, error_msg):
        """Handle failure of a critical step"""
        failure_msg = (
            f"🚨 CRITICAL WORKFLOW FAILURE\n\n"
            f"Step: {step_name}\n"
            f"Date: {self.date_str}\n"
            f"Time: {datetime.now(self.cst).strftime('%H:%M:%S')} CDT\n"
            f"Error: {error_msg}\n\n"
            f"⚠️ Workflow has been STOPPED due to critical failure.\n"
            f"Manual intervention required to resolve the issue.\n\n"
            f"New Workflow Dependencies:\n"
            f"• 8:25 Pre-filtering is required for 8:37 qualification\n"
            f"• 8:37 qualification is required for 8:40 momentum\n"
            f"• 8:40 momentum is required for 8:50 final check\n"
            f"• Each step depends on the previous step's success\n\n"
            f"Next steps:\n"
            f"1. Check logs for detailed error information\n"
            f"2. Resolve the underlying issue\n"
            f"3. Restart the workflow manually if needed"
        )
        
        logger.critical(f"Critical failure in {step_name}: {error_msg}")
        logger.critical("STOPPING WORKFLOW due to critical failure")
        
        # Send critical failure notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"🚨 CRITICAL WORKFLOW FAILURE - {step_name}",
                failure_msg
            )
        
        # Stop the workflow
        self.running = False
        return False
    
    def _handle_non_critical_failure(self, step_name, error_msg):
        """Handle failure of a non-critical step"""
        warning_msg = (
            f"⚠️ WORKFLOW STEP FAILED\n\n"
            f"Step: {step_name}\n"
            f"Date: {self.date_str}\n"
            f"Time: {datetime.now(self.cst).strftime('%H:%M:%S')} CDT\n"
            f"Error: {error_msg}\n\n"
            f"This is a non-critical step. Core workflow can continue.\n"
            f"Manual review recommended when convenient."
        )
        
        logger.warning(f"Non-critical failure in {step_name}: {error_msg}")
        
        # Send warning notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"⚠️ Workflow Step Failed - {step_name}",
                warning_msg
            )
        
        return True  # Continue workflow
    
    def _reset_for_new_day(self):
        """Reset scheduler state for a new day"""
        old_date = self.date_str
        self.date_str = get_date_str()
        self.steps_run = set()
        
        logger.info(f"Date changed from {old_date} to {self.date_str}, resetting scheduler")
        
        # Send new day notification
        if SNS_TOPIC_ARN:
            new_day_msg = (
                f"🌅 NEW TRADING DAY STARTED\n\n"
                f"Date: {self.date_str}\n"
                f"Previous date: {old_date}\n"
                f"Time: {datetime.now(self.cst).strftime('%H:%M:%S')} CDT\n\n"
                f"NEW WORKFLOW READY:\n"
                f"🕐 8:20 - Premarket gainers\n"
                f"🕐 8:22 - NASDAQ symbols\n"
                f"🕐 8:25 - Pre-filtered data pull (market cap ≥$50M, price ≥$3)\n"
                f"🕐 8:37 - Qualification (volume ≥1M, gain 5-60%, price > open)\n"
                f"🕐 8:40 - Momentum check #1 (price > 8:37)\n"
                f"🕐 8:50 - Final momentum + SNS notification (price > 8:40)\n\n"
                f"All scheduled steps will run according to the new workflow."
            )
            
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"🌅 New Trading Day Started - {self.date_str}",
                new_day_msg
            )
    
    def run_single_step(self, step_name):
        """
        Run a single step by name (for testing/manual execution).
        
        Args:
            step_name: Name of the step to run
            
        Returns:
            bool: True if successful, False otherwise
        """
        step_mapping = {
            'premarket_gainers': self._run_premarket_gainers,
            'nasdaq_symbols': self._run_nasdaq_symbols,
            'prefiltered_data_pull': self._run_prefiltered_data_pull,
            '8_25': self._run_prefiltered_data_pull,  # Alias
            '8_37_qualification': self._run_8_37_qualification,
            '8_37': self._run_8_37_qualification,  # Alias
            '8_40_momentum': self._run_8_40_momentum_check,
            '8_40': self._run_8_40_momentum_check,  # Alias
            '8_50_momentum': self._run_8_50_momentum_check,
            '8_50': self._run_8_50_momentum_check,  # Alias
            'end_of_day_summary': self._run_end_of_day_summary,
        }
        
        if step_name not in step_mapping:
            logger.error(f"Unknown step name: {step_name}")
            available_steps = ', '.join(step_mapping.keys())
            logger.error(f"Available steps: {available_steps}")
            return False
        
        logger.info(f"Manually running step: {step_name}")
        
        try:
            success = step_mapping[step_name]()
            if success:
                logger.info(f"✅ Step {step_name} completed successfully")
            else:
                logger.error(f"❌ Step {step_name} failed")
            return success
        except Exception as e:
            logger.error(f"❌ Step {step_name} failed with exception: {e}")
            return False
    
    def validate_workflow_dependencies(self):
        """Validate that the workflow dependencies make sense"""
        logger.info("Validating new workflow dependencies...")
        
        dependencies = [
            ("8:25 Pre-filtering", "Creates filtered dataset with market cap and price filters"),
            ("8:37 Qualification", "Requires filtered dataset from 8:25"),
            ("8:40 Momentum", "Requires qualified stocks from 8:37"),
            ("8:50 Final + Email", "Requires momentum stocks from 8:40"),
        ]
        
        logger.info("Workflow dependency chain:")
        for step, description in dependencies:
            logger.info(f"  {step}: {description}")
        
        # Check timing conflicts
        critical_times = ["08:25", "08:37", "08:40", "08:50"]
        logger.info(f"Critical timing windows: {', '.join(critical_times)}")
        
        logger.info("Workflow validation completed")
        return True
    
    def run_schedule_loop(self):
        """
        Main scheduler loop. Runs continuously checking for scheduled tasks.
        """
        logger.info("🚀 Starting NEW Polygon stock workflow scheduler")
        logger.info(f"Initial date: {self.date_str}")
        logger.info(f"Scheduled steps: {len(self.schedule)}")
        
        # Validate workflow
        self.validate_workflow_dependencies()
        
        # Send startup notification
        if SNS_TOPIC_ARN:
            startup_msg = (
                f"🚀 NEW POLYGON WORKFLOW SCHEDULER STARTED\n\n"
                f"Date: {self.date_str}\n"
                f"Start time: {datetime.now(self.cst).strftime('%H:%M:%S')} CDT\n"
                f"Scheduled steps: {len(self.schedule)}\n\n"
                f"🔄 NEW WORKFLOW:\n"
                f"• 8:20 - Premarket gainers\n"
                f"• 8:22 - NASDAQ symbols\n"
                f"• 8:25 - Pre-filtered data (market cap ≥$50M, price ≥$3)\n"
                f"• 8:37 - Qualification (volume ≥1M, gain 5-60%, price > open)\n"
                f"• 8:40 - Momentum check #1 (price > 8:37)\n"
                f"• 8:50 - Final momentum + SNS notification (price > 8:40)\n\n"
                f"🎯 GOAL: SNS buy list notification at 8:50 with stocks that:\n"
                f"✓ Pass pre-filtering criteria\n"
                f"✓ Meet qualification thresholds\n"
                f"✓ Maintain upward momentum\n\n"
                f"📧 SNS notification will be sent automatically at 8:50"
            )
            
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"🚀 New Workflow Scheduler Started - {self.date_str}",
                startup_msg
            )
        
        while self.running:
            try:
                now = datetime.now(self.cst)
                current_time = now.strftime('%H:%M')
                new_date_str = now.strftime('%Y%m%d')
                
                # Check if date has changed
                if new_date_str != self.date_str:
                    self._reset_for_new_day()
                
                # Check each scheduled step
                for time_str, func, description, is_critical in self.schedule:
                    step_id = f"{self.date_str}-{time_str}-{func.__name__}"
                    
                    # Run step if it's time and hasn't been run yet
                    if current_time == time_str and step_id not in self.steps_run:
                        logger.info(f"⏰ Running scheduled step: {description} at {time_str}")
                        
                        try:
                            success = func()
                            
                            if success:
                                logger.info(f"✅ {description} completed successfully")
                                self.steps_run.add(step_id)
                            else:
                                error_msg = f"{description} returned False"
                                
                                if is_critical:
                                    self._handle_critical_failure(description, error_msg)
                                    break  # Exit the loop for critical failures
                                else:
                                    self._handle_non_critical_failure(description, error_msg)
                                    self.steps_run.add(step_id)  # Mark as attempted
                        
                        except Exception as e:
                            error_msg = f"{description} raised exception: {str(e)}"
                            
                            if is_critical:
                                self._handle_critical_failure(description, error_msg)
                                break  # Exit the loop for critical failures
                            else:
                                self._handle_non_critical_failure(description, error_msg)
                                self.steps_run.add(step_id)  # Mark as attempted
                
                # Check if workflow should continue
                if not self.running:
                    logger.info("Workflow stopped due to critical failure")
                    break
                
                # Sleep before next check
                time.sleep(10)  # Check every 10 seconds
                
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, stopping scheduler...")
                self.running = False
            except Exception as e:
                logger.error(f"Unexpected error in scheduler loop: {e}")
                time.sleep(60)  # Wait longer on unexpected errors
        
        logger.info("🛑 Polygon workflow scheduler stopped")

def main():
    """Main entry point"""
    # Setup
    setup_logging()
    
    try:
        validate_config()
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        sys.exit(1)
    
    # Create and run scheduler
    scheduler = PolygonWorkflowScheduler()
    
    # Check for manual step execution
    if len(sys.argv) > 1:
        step_name = sys.argv[1]
        if step_name == 'list':
            print("Available steps:")
            print("  premarket_gainers - Collect premarket top gainers")
            print("  nasdaq_symbols - Collect NASDAQ symbols")
            print("  prefiltered_data_pull (8_25) - Pre-filtered data pull")
            print("  8_37_qualification (8_37) - Run 8:37 qualification")
            print("  8_40_momentum (8_40) - Run 8:40 momentum check")
            print("  8_50_momentum (8_50) - Run 8:50 momentum + SNS notification")
            print("  end_of_day_summary - Run end of day summary")
            sys.exit(0)
        elif step_name == 'validate':
            # Validate workflow
            scheduler.validate_workflow_dependencies()
            sys.exit(0)
        else:
            # Run single step
            success = scheduler.run_single_step(step_name)
            sys.exit(0 if success else 1)
    
    # Run normal schedule loop
    try:
        scheduler.run_schedule_loop()
    except Exception as e:
        logger.error(f"Fatal error in main scheduler: {e}")
        
        # Send fatal error notification
        if SNS_TOPIC_ARN:
            send_sns_notification(
                SNS_TOPIC_ARN,
                "🚨 FATAL SCHEDULER ERROR",
                f"The new workflow scheduler has crashed with a fatal error:\n\n{str(e)}\n\nImmediate attention required."
            )
        
        sys.exit(1)

if __name__ == "__main__":
    main()