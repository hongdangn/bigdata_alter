"""
Unified Pipeline Manager
Manages both streaming and batch processing pipelines
"""

import subprocess
import logging
import time
from multiprocessing import Process
import signal
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedPipelineManager:
    """
    Manages both streaming and batch processing pipelines
    """
    
    def __init__(self):
        self.processes = {}
        self.running = True
        
    def start_streaming_pipeline(self):
        """Start the streaming pipeline"""
        logger.info("Starting streaming pipeline...")
        
        try:
            # Start Spark Streaming
            streaming_process = subprocess.Popen(
                ["python", "spark_streaming.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.processes['streaming'] = streaming_process
            logger.info("✓ Streaming pipeline started")
            
        except Exception as e:
            logger.error(f"✗ Failed to start streaming pipeline: {e}")
    
    def start_batch_scheduler(self):
        """Start the batch processing scheduler"""
        logger.info("Starting batch scheduler...")
        
        try:
            scheduler_process = subprocess.Popen(
                ["python", "batch_scheduler.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.processes['batch_scheduler'] = scheduler_process
            logger.info("✓ Batch scheduler started")
            
        except Exception as e:
            logger.error(f"✗ Failed to start batch scheduler: {e}")
    
    def start_crawler(self, mode='streaming'):
        """
        Start Scrapy crawler
        
        Args:
            mode: 'streaming' (to Kafka) or 'batch' (to files)
        """
        logger.info(f"Starting crawler in {mode} mode...")
        
        try:
            # Change to bds directory and run crawler
            crawler_process = subprocess.Popen(
                ["scrapy", "crawl", "bds_spider"],
                cwd="./bds",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.processes['crawler'] = crawler_process
            logger.info("✓ Crawler started")
            
        except Exception as e:
            logger.error(f"✗ Failed to start crawler: {e}")
    
    def run_immediate_batch(self):
        """Run batch processing immediately (not scheduled)"""
        logger.info("Running immediate batch processing...")
        
        try:
            result = subprocess.run(
                ["python", "spark_batch.py"],
                capture_output=True,
                text=True,
                timeout=3600
            )
            
            if result.returncode == 0:
                logger.info("✓ Batch processing completed successfully")
            else:
                logger.error(f"✗ Batch processing failed:\n{result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("✗ Batch processing timed out")
        except Exception as e:
            logger.error(f"✗ Batch processing error: {e}")
    
    def check_health(self):
        """Check health of all running processes"""
        logger.info("\n=== Pipeline Health Status ===")
        
        for name, process in self.processes.items():
            if process.poll() is None:
                logger.info(f"  ✓ {name}: RUNNING")
            else:
                logger.warning(f"  ✗ {name}: STOPPED (exit code: {process.returncode})")
    
    def stop_all(self):
        """Stop all running processes"""
        logger.info("Stopping all pipelines...")
        
        for name, process in self.processes.items():
            if process.poll() is None:
                logger.info(f"  Stopping {name}...")
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                logger.info(f"  ✓ {name} stopped")
        
        self.running = False
        logger.info("All pipelines stopped")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("\nReceived shutdown signal")
        self.stop_all()
        sys.exit(0)


def main():
    """
    Main function to orchestrate the pipeline
    """
    
    manager = UnifiedPipelineManager()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, manager.signal_handler)
    signal.signal(signal.SIGTERM, manager.signal_handler)
    
    print("""
╔═══════════════════════════════════════════════════════════╗
║    Real Estate Data Pipeline - Unified Manager            ║
╚═══════════════════════════════════════════════════════════╝

Select mode:
  1. Start Streaming Pipeline (real-time)
  2. Start Batch Processing (scheduled)
  3. Run Immediate Batch Processing
  4. Start Both (Hybrid)
  5. Start Crawler Only
  6. Check Health
  7. Stop All
  8. Exit
    """)
    
    try:
        while manager.running:
            choice = input("\nEnter choice (1-8): ").strip()
            
            if choice == '1':
                manager.start_streaming_pipeline()
                manager.start_crawler(mode='streaming')
                
            elif choice == '2':
                manager.start_batch_scheduler()
                
            elif choice == '3':
                manager.run_immediate_batch()
                
            elif choice == '4':
                manager.start_streaming_pipeline()
                manager.start_batch_scheduler()
                manager.start_crawler(mode='streaming')
                logger.info("Hybrid mode activated: Both streaming and batch pipelines running")
                
            elif choice == '5':
                mode = input("Crawler mode (streaming/batch): ").strip().lower()
                if mode in ['streaming', 'batch']:
                    manager.start_crawler(mode=mode)
                else:
                    logger.error("Invalid mode. Use 'streaming' or 'batch'")
                    
            elif choice == '6':
                manager.check_health()
                
            elif choice == '7':
                manager.stop_all()
                
            elif choice == '8':
                manager.stop_all()
                break
                
            else:
                logger.warning("Invalid choice. Please enter 1-8")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\nShutdown requested")
        manager.stop_all()
    except Exception as e:
        logger.error(f"Error: {e}")
        manager.stop_all()


if __name__ == "__main__":
    main()
