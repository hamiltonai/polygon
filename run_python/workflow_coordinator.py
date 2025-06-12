# workflow_coordinator.py - Main orchestration and workflow coordination
import time
import logging
from datetime import datetime
import pytz
from config import SNS_TOPIC_ARN, MIN_MARKET_CAP_MILLIONS, MIN_PREVIOUS_CLOSE
from utils import (
    get_date_str, get_current_cst_time, send_sns_notification, 
    create_stats_counter, format_duration, format_number
)

logger = logging.getLogger(__name__)

class WorkflowCoordinator:
    """Coordinates the entire data pull workflow and handles notifications"""
    
    def __init__(self):
        self.sns_topic = SNS_TOPIC_ARN
        self.min_market_cap = MIN_MARKET_CAP_MILLIONS
        self.min_previous_close = MIN_PREVIOUS_CLOSE
    
    def get_period_info(self, force_previous_day=False):
        """
        Determine data period based on Chicago time or force setting
        
        Args:
            force_previous_day: Force previous day data regardless of time
            
        Returns:
            dict: Period information including data_period, time, and date
        """
        cst = pytz.timezone('America/Chicago')
        now_cst = datetime.now(cst)
        
        if force_previous_day:
            period_info = {
                'data_period': 'previous',
                'current_time_cst': now_cst.strftime('%H:%M:%S'),
                'date_str': now_cst.strftime('%Y-%m-%d'),
                'forced': True
            }
        else:
            cutoff_time = now_cst.replace(hour=8, minute=32, second=0, microsecond=0)
            is_after_cutoff = now_cst >= cutoff_time
            data_period = "today" if is_after_cutoff else "previous"
            
            period_info = {
                'data_period': data_period,
                'current_time_cst': now_cst.strftime('%H:%M:%S'),
                'date_str': now_cst.strftime('%Y-%m-%d'),
                'forced': False
            }
        
        return period_info
    
    def should_apply_prefilter(self, period_info):
        """
        Determine if pre-filtering should be applied (8:25 step)
        
        Args:
            period_info: Period information dictionary
            
        Returns:
            bool: True if pre-filtering should be applied
        """
        current_time = period_info['current_time_cst']
        is_forced_previous = period_info.get('forced', False)
        
        return is_forced_previous or current_time < "08:30"
    
    def create_progress_tracker(self):
        """Create a progress tracking object"""
        return {
            'start_time': time.time(),
            'stats': create_stats_counter(),
            'phase': 'initialization',
            'current_batch': 0,
            'total_batches': 0
        }
    
    def update_progress(self, tracker, phase, **kwargs):
        """
        Update progress tracker
        
        Args:
            tracker: Progress tracking dictionary
            phase: Current phase name
            **kwargs: Additional progress information
        """
        tracker['phase'] = phase
        tracker.update(kwargs)
        
        elapsed = time.time() - tracker['start_time']
        logger.debug(f"Progress: {phase} - Elapsed: {elapsed:.1f}s")
    
    def calculate_completion_stats(self, tracker, symbol_count, result_count):
        """
        Calculate completion statistics
        
        Args:
            tracker: Progress tracking dictionary
            symbol_count: Total symbols processed
            result_count: Final result count
            
        Returns:
            dict: Completion statistics
        """
        total_time = time.time() - tracker['start_time']
        stats = tracker['stats']
        
        complete_rate = (stats['complete_records'] / stats['processed']) * 100 if stats['processed'] > 0 else 0
        symbols_per_second = symbol_count / total_time if total_time > 0 else 0
        
        return {
            'total_time': total_time,
            'complete_rate': complete_rate,
            'symbols_per_second': symbols_per_second,
            'api_calls': stats['api_calls'],
            'processed': stats['processed'],
            'complete_records': stats['complete_records'],
            'final_results': result_count
        }
    
    def build_completion_summary(self, period_info, symbol_count, completion_stats, 
                               filename, filtering_info=None):
        """
        Build completion summary message
        
        Args:
            period_info: Period information
            symbol_count: Total symbols processed
            completion_stats: Completion statistics
            filename: Output filename
            filtering_info: Optional filtering information
            
        Returns:
            str: Formatted summary message
        """
        summary = (
            f"✅ INITIAL DATA PULL COMPLETE\n\n"
            f"Period: {period_info['data_period'].upper()}\n"
            f"Time: {period_info['current_time_cst']} CDT\n"
            f"Symbols: {format_number(symbol_count)}\n"
            f"Complete: {format_number(completion_stats['complete_records'])} "
            f"({completion_stats['complete_rate']:.1f}%)\n"
        )
        
        # Add filtering information if available
        if filtering_info:
            filter_text = (
                f"Pre-filtered: {filtering_info['filtered_count']}/{filtering_info['original_count']} stocks\n"
                f"Criteria: Market cap ≥${filtering_info['criteria']['min_market_cap']}M, "
                f"Price ≥${filtering_info['criteria']['min_previous_close']}\n"
                f"Top gainers in result: {filtering_info['top_gainer_count']}/{filtering_info['filtered_count']} stocks\n"
                f"Total premarket gainers found: {filtering_info['premarket_gainers_total']}\n"
            )
            summary += filter_text
        
        summary += (
            f"Duration: {format_duration(completion_stats['total_time'])}\n"
            f"Speed: {completion_stats['symbols_per_second']:.1f} symbols/sec\n"
            f"File: {filename}"
        )
        
        return summary
    
    def send_completion_notification(self, period_info, symbol_count, completion_stats, 
                                   filename, filtering_info=None):
        """
        Send completion notification via SNS
        
        Args:
            period_info: Period information
            symbol_count: Total symbols processed
            completion_stats: Completion statistics
            filename: Output filename
            filtering_info: Optional filtering information
        """
        if not self.sns_topic:
            logger.info("SNS notifications disabled - no topic configured")
            return
        
        try:
            # Build summary message
            summary = self.build_completion_summary(
                period_info, symbol_count, completion_stats, filename, filtering_info
            )
            
            # Determine subject and message details
            step_type = "Pre-filtered" if filtering_info else "Standard"
            final_count = filtering_info['filtered_count'] if filtering_info else completion_stats['final_results']
            
            subject = f"✅ {step_type} Data Pull Complete - {final_count} stocks ({period_info['data_period']})"
            
            # Add top gainers info to SNS if this is pre-filtering step
            sns_message = summary
            if filtering_info:
                top_gainers_in_result = filtering_info.get('top_gainers_list', [])
                if top_gainers_in_result:
                    sns_message += f"\n\n🔥 PREMARKET TOP GAINERS IN FILTERED DATASET:\n{', '.join(top_gainers_in_result[:15])}"
                    if len(top_gainers_in_result) > 15:
                        sns_message += f"\n... and {len(top_gainers_in_result) - 15} more"
                else:
                    sns_message += f"\n\n⚠️ No premarket top gainers passed the pre-filtering criteria"
            
            # Send notification
            success = send_sns_notification(self.sns_topic, subject, sns_message)
            
            if success:
                logger.info(f"Completion notification sent: {subject}")
            else:
                logger.error("Failed to send completion notification")
                
        except Exception as e:
            logger.error(f"Error sending completion notification: {e}")
    
    def handle_error(self, error, date_str, phase="data_pull"):
        """
        Handle errors and send error notifications
        
        Args:
            error: Exception or error message
            date_str: Date string for context
            phase: Phase where error occurred
        """
        error_msg = f"{phase.title()} failed: {str(error)}"
        logger.error(error_msg)
        
        # Send error notification
        if self.sns_topic:
            try:
                send_sns_notification(
                    self.sns_topic,
                    f"❌ {phase.title()} Failed",
                    f"Error: {error_msg}\nDate: {date_str}\nTime: {get_current_cst_time().strftime('%H:%M:%S')} CDT"
                )
            except Exception as e:
                logger.error(f"Failed to send error notification: {e}")
    
    def validate_inputs(self, symbols, period_info, date_str):
        """
        Validate inputs before starting workflow
        
        Args:
            symbols: List of symbols to process
            period_info: Period information
            date_str: Date string
            
        Returns:
            tuple: (is_valid: bool, errors: list)
        """
        errors = []
        
        if not symbols:
            errors.append("No symbols provided")
        elif len(symbols) == 0:
            errors.append("Empty symbols list")
        
        if not period_info:
            errors.append("No period information provided")
        elif 'data_period' not in period_info:
            errors.append("Missing data_period in period_info")
        
        if not date_str:
            errors.append("No date string provided")
        elif len(date_str) != 8 or not date_str.isdigit():
            errors.append(f"Invalid date string format: {date_str}")
        
        # Validate configuration
        if self.min_market_cap <= 0:
            errors.append(f"Invalid min_market_cap: {self.min_market_cap}")
        
        if self.min_previous_close <= 0:
            errors.append(f"Invalid min_previous_close: {self.min_previous_close}")
        
        return len(errors) == 0, errors
    
    def log_workflow_start(self, symbols, period_info, date_str):
        """
        Log workflow start information
        
        Args:
            symbols: List of symbols to process
            period_info: Period information
            date_str: Date string
        """
        logger.info(f"Starting data pull workflow")
        logger.info(f"Date: {date_str}")
        logger.info(f"Period: {period_info['data_period']}")
        logger.info(f"Time: {period_info['current_time_cst']} CDT")
        logger.info(f"Symbols: {len(symbols)}")
        logger.info(f"Pre-filter: {self.should_apply_prefilter(period_info)}")
        
        if period_info.get('forced'):
            logger.info("Previous day data forced")
    
    def create_workflow_context(self, date_str=None, max_symbols=None, force_previous_day=False):
        """
        Create complete workflow context
        
        Args:
            date_str: Optional date string override
            max_symbols: Optional limit on symbol count
            force_previous_day: Force previous day data
            
        Returns:
            dict: Complete workflow context
        """
        if not date_str:
            date_str = get_date_str()
        
        period_info = self.get_period_info(force_previous_day)
        is_prefilter_step = self.should_apply_prefilter(period_info)
        
        context = {
            'date_str': date_str,
            'period_info': period_info,
            'is_prefilter_step': is_prefilter_step,
            'max_symbols': max_symbols,
            'force_previous_day': force_previous_day,
            'tracker': self.create_progress_tracker()
        }
        
        return context
    
    def finalize_workflow(self, context, symbols, final_results, filename, filtering_info=None):
        """
        Finalize workflow with statistics and notifications
        
        Args:
            context: Workflow context
            symbols: Original symbols list
            final_results: Final processed results
            filename: Output filename
            filtering_info: Optional filtering information
            
        Returns:
            dict: Workflow completion information
        """
        # Calculate completion statistics
        completion_stats = self.calculate_completion_stats(
            context['tracker'], len(symbols), len(final_results)
        )
        
        # Log completion
        logger.info(f"Workflow completed successfully")
        logger.info(f"Final results: {len(final_results)} stocks")
        logger.info(f"Total time: {format_duration(completion_stats['total_time'])}")
        
        # Send notifications
        self.send_completion_notification(
            context['period_info'], len(symbols), completion_stats, 
            filename, filtering_info
        )
        
        # Return completion info
        return {
            'success': True,
            'filename': filename,
            'result_count': len(final_results),
            'completion_stats': completion_stats,
            'filtering_info': filtering_info
        }
    
    def get_workflow_summary(self, completion_info):
        """
        Get a brief workflow summary for logging
        
        Args:
            completion_info: Workflow completion information
            
        Returns:
            str: Brief summary message
        """
        result_count = completion_info['result_count']
        filename = completion_info['filename']
        total_time = completion_info['completion_stats']['total_time']
        
        summary = f"✅ Workflow completed: {result_count} stocks saved to {filename} in {format_duration(total_time)}"
        
        if completion_info.get('filtering_info'):
            filter_info = completion_info['filtering_info']
            summary += f" (filtered from {filter_info['original_count']})"
        
        return summary