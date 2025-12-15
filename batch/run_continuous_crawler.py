#!/usr/bin/env python3
"""
Continuous crawler for real estate data
Runs the spider continuously with configurable intervals
"""
import time
import subprocess
import logging
from datetime import datetime
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ContinuousCrawler:
    """
    Manages continuous crawling with intervals
    """
    
    def __init__(
        self,
        spider_name="bds_spider",
        interval_minutes=60,
        provinces=None,
        max_page=999999,
        min_page=1,
        working_dir="bds"
    ):
        """
        Initialize continuous crawler
        
        Args:
            spider_name: Name of the spider to run
            interval_minutes: Minutes between crawl runs
            provinces: List of provinces to crawl (None = all)
            max_page: Maximum page to crawl per run
            min_page: Minimum page to start from
            working_dir: Directory containing scrapy.cfg
        """
        self.spider_name = spider_name
        self.interval_seconds = interval_minutes * 60
        self.provinces = provinces or ['ha-noi']  # Default to Ha Noi
        self.max_page = max_page
        self.min_page = min_page
        self.working_dir = working_dir
        self.run_count = 0
        
    def run_crawler(self, province):
        """
        Run the crawler for a specific province
        
        Args:
            province: Province slug to crawl
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info(f"Starting crawl for province: {province}")
        
        try:
            # Build scrapy command
            cmd = [
                'scrapy', 'crawl', self.spider_name,
                '-a', f'province={province}',
                '-a', f'max_page={self.max_page}',
                '-a', f'min_page={self.min_page}'
            ]
            
            logger.info(f"Running command: {' '.join(cmd)}")
            
            # Change to working directory
            original_dir = os.getcwd()
            os.chdir(self.working_dir)
            
            # Run scrapy
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout per province
            )
            
            # Return to original directory
            os.chdir(original_dir)
            
            if result.returncode == 0:
                logger.info(f"Successfully crawled {province}")
                
                # Extract stats from output
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if 'item_scraped_count' in line or 'finish_reason' in line:
                        logger.info(line.strip())
                
                return True
            else:
                logger.error(f"Crawler failed for {province} with return code {result.returncode}")
                logger.error(f"Error output: {result.stderr[-500:]}")  # Last 500 chars
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"Crawler timed out for {province}")
            os.chdir(original_dir)
            return False
        except Exception as e:
            logger.error(f"Error running crawler for {province}: {e}", exc_info=True)
            os.chdir(original_dir)
            return False
    
    def run_continuous(self):
        """
        Run crawler continuously with intervals
        """
        logger.info("=" * 80)
        logger.info("Starting Continuous Crawler")
        logger.info(f"Spider: {self.spider_name}")
        logger.info(f"Provinces: {', '.join(self.provinces)}")
        logger.info(f"Interval: {self.interval_seconds / 60} minutes")
        logger.info(f"Max page per run: {self.max_page}")
        logger.info("=" * 80)
        
        try:
            while True:
                self.run_count += 1
                start_time = datetime.now()
                
                logger.info(f"\n{'=' * 80}")
                logger.info(f"RUN #{self.run_count} - Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'=' * 80}\n")
                
                success_count = 0
                total_provinces = len(self.provinces)
                
                # Crawl each province
                for i, province in enumerate(self.provinces, 1):
                    logger.info(f"\n[{i}/{total_provinces}] Processing province: {province}")
                    
                    if self.run_crawler(province):
                        success_count += 1
                    
                    # Small delay between provinces
                    if i < total_provinces:
                        logger.info("Waiting 10 seconds before next province...")
                        time.sleep(10)
                
                # Summary
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                logger.info(f"\n{'=' * 80}")
                logger.info(f"RUN #{self.run_count} COMPLETED")
                logger.info(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
                logger.info(f"Success: {success_count}/{total_provinces} provinces")
                logger.info(f"Next run in {self.interval_seconds / 60} minutes")
                logger.info(f"{'=' * 80}\n")
                
                # Wait for next interval
                logger.info(f"Sleeping for {self.interval_seconds} seconds...")
                time.sleep(self.interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("\n\nReceived interrupt signal. Shutting down gracefully...")
            logger.info(f"Total runs completed: {self.run_count}")
        except Exception as e:
            logger.error(f"Fatal error in continuous crawler: {e}", exc_info=True)
            sys.exit(1)


def main():
    """
    Main entry point
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Run crawler continuously')
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Minutes between crawl runs (default: 60)'
    )
    parser.add_argument(
        '--provinces',
        nargs='+',
        default=['ha-noi'],
        help='Provinces to crawl (default: ha-noi)'
    )
    parser.add_argument(
        '--max-page',
        type=int,
        default=999999,
        help='Maximum page to crawl per run (default: 999999)'
    )
    parser.add_argument(
        '--min-page',
        type=int,
        default=1,
        help='Minimum page to start from (default: 1)'
    )
    parser.add_argument(
        '--spider',
        default='bds_spider',
        help='Spider name (default: bds_spider)'
    )
    parser.add_argument(
        '--working-dir',
        default='../bds',
        help='Working directory with scrapy.cfg (default: bds)'
    )
    
    args = parser.parse_args()
    
    # Create crawler instance
    crawler = ContinuousCrawler(
        spider_name=args.spider,
        interval_minutes=args.interval,
        provinces=args.provinces,
        max_page=args.max_page,
        min_page=args.min_page,
        working_dir=args.working_dir
    )
    
    # Run continuously
    crawler.run_continuous()


if __name__ == "__main__":
    main()
