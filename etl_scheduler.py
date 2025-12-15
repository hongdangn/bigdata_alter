"""
ETL Scheduler - Automated scheduled execution of ETL jobs
Supports daily, weekly, and monthly schedules
"""
import schedule
import time
import subprocess
import logging
from datetime import datetime, timedelta
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLScheduler:
    """
    Manages scheduled ETL job execution
    """
    
    def __init__(self, etl_script="etl_batch_job.py", python_cmd="python"):
        self.etl_script = etl_script
        self.python_cmd = python_cmd
        self.job_history = []
    
    def run_etl_job(self, mode="incremental", year=None, month=None):
        """
        Execute ETL job
        
        Args:
            mode: 'full' or 'incremental'
            year: Year for incremental processing
            month: Month for incremental processing
        """
        try:
            logger.info("=" * 80)
            logger.info(f"Starting ETL Job - Mode: {mode}")
            
            # Build command
            cmd = [self.python_cmd, self.etl_script, "--mode", mode]
            
            if mode == "incremental":
                if year and month:
                    cmd.extend(["--year", str(year), "--month", str(month)])
            
            logger.info(f"Command: {' '.join(cmd)}")
            
            # Execute
            start_time = datetime.now()
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Log result
            if result.returncode == 0:
                logger.info(f"✓ ETL Job completed successfully in {duration:.2f} seconds")
                status = "SUCCESS"
            else:
                logger.error(f"✗ ETL Job failed with return code {result.returncode}")
                logger.error(f"Error output: {result.stderr[-1000:]}")  # Last 1000 chars
                status = "FAILED"
            
            # Save to history
            self.job_history.append({
                'timestamp': start_time,
                'mode': mode,
                'year': year,
                'month': month,
                'duration': duration,
                'status': status
            })
            
            logger.info("=" * 80)
            
        except subprocess.TimeoutExpired:
            logger.error("ETL Job timed out after 1 hour")
        except Exception as e:
            logger.error(f"Error running ETL job: {e}", exc_info=True)
    
    def daily_incremental_job(self):
        """
        Daily incremental job - process current month
        """
        now = datetime.now()
        logger.info(f"Running daily incremental job for {now.year}/{now.month:02d}")
        self.run_etl_job(mode="incremental", year=now.year, month=now.month)
    
    def weekly_full_refresh(self):
        """
        Weekly full refresh - reprocess all data
        """
        logger.info("Running weekly full refresh")
        self.run_etl_job(mode="full")
    
    def monthly_historical_job(self):
        """
        Monthly job - process previous month
        """
        # Get previous month
        today = datetime.now()
        first_day_current_month = today.replace(day=1)
        last_day_prev_month = first_day_current_month - timedelta(days=1)
        
        year = last_day_prev_month.year
        month = last_day_prev_month.month
        
        logger.info(f"Running monthly historical job for {year}/{month:02d}")
        self.run_etl_job(mode="incremental", year=year, month=month)
    
    def start_scheduler(self, schedule_config):
        """
        Start the scheduler with given configuration
        
        Args:
            schedule_config: Dict with schedule settings
                {
                    'daily': True/False,
                    'daily_time': 'HH:MM',
                    'weekly': True/False,
                    'weekly_day': 'monday', 'tuesday', etc.
                    'weekly_time': 'HH:MM',
                    'monthly': True/False,
                    'monthly_day': 1-31,
                    'monthly_time': 'HH:MM'
                }
        """
        logger.info("=" * 80)
        logger.info("ETL SCHEDULER STARTING")
        logger.info("=" * 80)
        logger.info(f"Configuration: {schedule_config}")
        logger.info("=" * 80)
        
        # Daily incremental job
        if schedule_config.get('daily', True):
            daily_time = schedule_config.get('daily_time', '02:00')
            schedule.every().day.at(daily_time).do(self.daily_incremental_job)
            logger.info(f"✓ Daily incremental job scheduled at {daily_time}")
        
        # Weekly full refresh
        if schedule_config.get('weekly', True):
            weekly_day = schedule_config.get('weekly_day', 'sunday')
            weekly_time = schedule_config.get('weekly_time', '03:00')
            
            day_map = {
                'monday': schedule.every().monday,
                'tuesday': schedule.every().tuesday,
                'wednesday': schedule.every().wednesday,
                'thursday': schedule.every().thursday,
                'friday': schedule.every().friday,
                'saturday': schedule.every().saturday,
                'sunday': schedule.every().sunday
            }
            
            day_map[weekly_day].at(weekly_time).do(self.weekly_full_refresh)
            logger.info(f"✓ Weekly full refresh scheduled on {weekly_day} at {weekly_time}")
        
        # Monthly historical job
        if schedule_config.get('monthly', False):
            monthly_day = schedule_config.get('monthly_day', 1)
            monthly_time = schedule_config.get('monthly_time', '04:00')
            
            # Note: schedule library doesn't support monthly directly
            # We'll check daily and run on the specified day
            def monthly_check():
                if datetime.now().day == monthly_day:
                    self.monthly_historical_job()
            
            schedule.every().day.at(monthly_time).do(monthly_check)
            logger.info(f"✓ Monthly job scheduled on day {monthly_day} at {monthly_time}")
        
        logger.info("=" * 80)
        logger.info("Scheduler is running... Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        # Main loop
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("\n\nScheduler stopped by user")
            logger.info(f"Total jobs executed: {len(self.job_history)}")
            
            # Print summary
            if self.job_history:
                success_count = sum(1 for job in self.job_history if job['status'] == 'SUCCESS')
                logger.info(f"Success: {success_count}/{len(self.job_history)}")


def main():
    """
    Main entry point
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL Job Scheduler')
    parser.add_argument('--config', choices=['default', 'custom'],
                       default='default',
                       help='Schedule configuration preset')
    parser.add_argument('--daily', action='store_true',
                       help='Enable daily incremental job')
    parser.add_argument('--daily-time', default='02:00',
                       help='Time for daily job (HH:MM)')
    parser.add_argument('--weekly', action='store_true',
                       help='Enable weekly full refresh')
    parser.add_argument('--weekly-day', default='sunday',
                       choices=['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'],
                       help='Day for weekly job')
    parser.add_argument('--weekly-time', default='03:00',
                       help='Time for weekly job (HH:MM)')
    parser.add_argument('--test', action='store_true',
                       help='Run test job immediately')
    
    args = parser.parse_args()
    
    # Create scheduler
    scheduler = ETLScheduler()
    
    # Test mode
    if args.test:
        logger.info("Running test job...")
        scheduler.daily_incremental_job()
        return
    
    # Build configuration
    if args.config == 'default':
        config = {
            'daily': True,
            'daily_time': '02:00',  # 2 AM daily
            'weekly': True,
            'weekly_day': 'sunday',
            'weekly_time': '03:00',  # 3 AM every Sunday
            'monthly': False
        }
    else:
        config = {
            'daily': args.daily,
            'daily_time': args.daily_time,
            'weekly': args.weekly,
            'weekly_day': args.weekly_day,
            'weekly_time': args.weekly_time,
            'monthly': False
        }
    
    # Start scheduler
    scheduler.start_scheduler(config)


if __name__ == "__main__":
    main()
