"""
Batch Processing Scheduler
Schedules periodic batch jobs using APScheduler
"""

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess
import logging
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchScheduler:
    def __init__(self):
        self.scheduler = BlockingScheduler()
        
    def run_batch_job(self, job_name, script_path, args=None):
        """Execute a batch processing job"""
        
        logger.info(f"Starting batch job: {job_name}")
        start_time = datetime.now()
        
        try:
            cmd = ["python", script_path]
            if args:
                cmd.extend(args)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            
            if result.returncode == 0:
                logger.info(f"✓ Batch job '{job_name}' completed in {duration:.2f}s")
            else:
                logger.error(f"✗ Batch job '{job_name}' failed:")
                logger.error(result.stderr)
                
        except subprocess.TimeoutExpired:
            logger.error(f"✗ Batch job '{job_name}' timed out after 1 hour")
        except Exception as e:
            logger.error(f"✗ Batch job '{job_name}' error: {e}")
    
    def daily_batch_processing(self):
        """Daily batch job - process historical data"""
        self.run_batch_job(
            "daily_batch_processing",
            "spark_batch.py"
        )
    
    def hourly_statistics_update(self):
        """Hourly job - update aggregated statistics"""
        logger.info("Running hourly statistics update...")
        # Add your statistics update logic here
    
    def weekly_data_cleanup(self):
        """Weekly job - clean up old data"""
        logger.info("Running weekly data cleanup...")
        # Add cleanup logic here
    
    def setup_schedules(self):
        """Configure all scheduled jobs"""
        
        # Daily batch processing at 2 AM
        self.scheduler.add_job(
            self.daily_batch_processing,
            trigger=CronTrigger(hour=2, minute=0),
            id='daily_batch',
            name='Daily Batch Processing',
            replace_existing=True
        )
        
        # Hourly statistics update
        self.scheduler.add_job(
            self.hourly_statistics_update,
            trigger=CronTrigger(minute=0),
            id='hourly_stats',
            name='Hourly Statistics Update',
            replace_existing=True
        )
        
        # Weekly cleanup on Sunday at 3 AM
        self.scheduler.add_job(
            self.weekly_data_cleanup,
            trigger=CronTrigger(day_of_week='sun', hour=3, minute=0),
            id='weekly_cleanup',
            name='Weekly Data Cleanup',
            replace_existing=True
        )
        
        logger.info("Scheduled jobs configured:")
        for job in self.scheduler.get_jobs():
            logger.info(f"  - {job.name} (ID: {job.id})")
    
    def start(self):
        """Start the scheduler"""
        logger.info("Starting batch scheduler...")
        self.setup_schedules()
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Scheduler stopped")


def main():
    scheduler = BatchScheduler()
    scheduler.start()


if __name__ == "__main__":
    main()
