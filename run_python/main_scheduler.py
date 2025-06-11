import time
import logging
import sys
from datetime import datetime
import pytz
from premarket_gainers import get_premarket_top_gainers
from nasdaq_symbols import get_nasdaq_symbols
from initial_data_pull import run_initial_data_pull
from qualification_filter import update_qualified_column
from intraday_updates import update_basic_intraday_data, update_full_intraday_data
from config import setup_logging, validate_config, SNS_TOPIC_ARN
from utils import get_date_str, send_sns_notification

logger = logging.getLogger(__name__)

class PolygonWorkflowScheduler:
    """
    Main scheduler for the Polygon stock data workflow.
    Runs scheduled tasks and handles failures with SNS notifications.
    """
    
    def __init__(self):
        self.cst = pytz.timezone('America/Chicago')
        self.steps_run = set()
        self.date_str = get_date_str()
        self.running = True
        
        # Define the schedule: (time_str, function, description, critical)
        # critical=True means if it fails, stop the program
        self.schedule = [
            ('08:24', self._run_premarket_gainers, 'Premarket Top Gainers', False),
            ('08:27', self._run_nasdaq_symbols, 'NASDAQ Symbols Collection', False),
            ('08:35', self._run_initial_data_pull, 'Initial Data Pull', True),  # Critical
            ('08:44', self._run_qualification_filter, 'Initial Qualification', True),  # Critical
            ('08:45', self._run_basic_intraday_update, 'Basic Intraday Update (08:45)', False),
            ('08:50', self._run_full_intraday_update, 'Full Intraday Update (08:50)', False),
            ('08:55', self._run_full_intraday_update, 'Full Intraday Update (08:55)', False),
            ('09:00', self._run_full_intraday_update, 'Full Intraday Update (09:00)', False),
            ('09:15', self._run_full_intraday_update, 'Full Intraday Update (09:15)', False),
            ('09:30', self._run_full_intraday_update, 'Full Intraday Update (09:30)', False),
            ('14:30', self._run_full_intraday_update, 'Full Intraday Update (14:30)', False),
        ]
    
    def _run_premarket_gainers(self):
        """Run premarket gainers collection"""
        return get_premarket_top_gainers(self.date_str)
    
    def _run_nasdaq_symbols(self):
        """Run NASDAQ symbols collection"""
        return get_nasdaq_symbols(self.date_str)
    
    def _run_initial_data_pull(self):
        """Run initial data pull"""
        return run_initial_data_pull(self.date_str)
    
    def _run_qualification_filter(self):
        """Run qualification filter"""
        return update_qualified_column(self.date_str)
    
    def _run_basic_intraday_update(self):
        """Run basic intraday update"""
        current_time = datetime.now(self.cst).strftime('%H:%M')
        return update_basic_intraday_data(self.date_str, current_time)
    
    def _run_full_intraday_update(self):
        """Run full intraday update"""
        current_time = datetime.now(self.cst).strftime('%H:%M')
        return update_full_intraday_data(self.date_str, current_time)
    
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
            f"This is a non-critical step. Workflow will continue.\n"
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
                f"🌅 NEW DAY WORKFLOW STARTED\n\n"
                f"Date: {self.date_str}\n"
                f"Previous date: {old_date}\n"
                f"Time: {datetime.now(self.cst).strftime('%H:%M:%S')} CDT\n\n"
                f"Scheduler has been reset for the new trading day.\n"
                f"All scheduled steps will run according to the daily schedule."
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
            'initial_data_pull': self._run_initial_data_pull,
            'qualification_filter': self._run_qualification_filter,
            'basic_intraday': self._run_basic_intraday_update,
            'full_intraday': self._run_full_intraday_update,
        }
        
        if step_name not in step_mapping:
            logger.error(f"Unknown step name: {step_name}")
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
    
    def run_schedule_loop(self):
        """
        Main scheduler loop. Runs continuously checking for scheduled tasks.
        """
        logger.info("🚀 Starting Polygon stock data workflow scheduler")
        logger.info(f"Initial date: {self.date_str}")
        logger.info(f"Scheduled steps: {len(self.schedule)}")
        
        # Send startup notification
        if SNS_TOPIC_ARN:
            startup_msg = (
                f"🚀 POLYGON WORKFLOW SCHEDULER STARTED\n\n"
                f"Date: {self.date_str}\n"
                f"Start time: {datetime.now(self.cst).strftime('%H:%M:%S')} CDT\n"
                f"Scheduled steps: {len(self.schedule)}\n\n"
                f"Schedule:\n"
            )
            
            for time_str, func, description, critical in self.schedule:
                critical_marker = " (CRITICAL)" if critical else ""
                startup_msg += f"• {time_str} - {description}{critical_marker}\n"
            
            send_sns_notification(
                SNS_TOPIC_ARN,
                f"🚀 Workflow Scheduler Started - {self.date_str}",
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
                time.sleep(30)  # Check every 30 seconds
                
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
            print("  initial_data_pull - Run initial data pull")
            print("  qualification_filter - Update qualification status")
            print("  basic_intraday - Run basic intraday update")
            print("  full_intraday - Run full intraday update")
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
                f"The workflow scheduler has crashed with a fatal error:\n\n{str(e)}\n\nImmediate attention required."
            )
        
        sys.exit(1)

if __name__ == "__main__":
    main()