"""
Kafka to MinIO Data Lake Consumer
Consumes real estate data from Kafka and stores it in MinIO as a data lake
with partitioned structure: raw/province=xxx/year=xxxx/month=xx/
"""
import json
import logging
import re
import os
import subprocess
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaError
from minio import Minio
from minio.error import S3Error
import io
import pandas as pd
from utils import normalize_province_name, get_partition_path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaToMinIOConsumer:
    """
    Consumer that reads from Kafka and writes to MinIO data lake
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="batdongsan",
        kafka_group_id="minio-consumer-group",
        minio_endpoint="localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        bucket_name="datalake",
        batch_size=100,
        batch_timeout=30
    ):
        """
        Initialize Kafka to MinIO consumer
        
        Args:
            kafka_bootstrap_servers: Kafka broker address
            kafka_topic: Kafka topic to consume from
            kafka_group_id: Consumer group ID
            minio_endpoint: MinIO server endpoint
            minio_access_key: MinIO access key
            minio_secret_key: MinIO secret key
            bucket_name: MinIO bucket name for data lake
            batch_size: Number of messages to batch before writing
            batch_timeout: Timeout in seconds for batch writes
        """
        self.kafka_topic = kafka_topic
        self.bucket_name = bucket_name
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # Initialize Kafka Consumer
        self.consumer = Consumer({
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': kafka_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
        })
        
        # Initialize MinIO Client
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False  # Set to True if using HTTPS
        )
        
        # Data buffer for batching
        self.data_buffer = {}  # Key: partition_path, Value: list of records
        self.message_count = 0
        
    def setup(self):
        """
        Setup MinIO bucket and Kafka subscription
        """
        try:
            # Create bucket if it doesn't exist
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                logger.info(f"Created MinIO bucket: {self.bucket_name}")
            else:
                logger.info(f"MinIO bucket already exists: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error creating MinIO bucket: {e}")
            raise
        
        # Subscribe to Kafka topic
        self.consumer.subscribe([self.kafka_topic])
        logger.info(f"Subscribed to Kafka topic: {self.kafka_topic}")
    
    def _parse_post_date(self, post_date_str):
        """
        Parse post_date string to datetime object
        Handles multiple Vietnamese date formats
        
        Args:
            post_date_str: Date string from crawler
            
        Returns:
            datetime: Parsed datetime or current time if parsing fails
        """
        if not post_date_str:
            return datetime.now()
        
        post_date_str = post_date_str.strip()
        
        # Try multiple date formats
        date_formats = [
            '%d/%m/%Y',      # 10/12/2025
            '%Y-%m-%d',      # 2025-12-10
            '%d-%m-%Y',      # 10-12-2025
            '%d.%m.%Y',      # 10.12.2025
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(post_date_str, fmt)
            except ValueError:
                continue
        
        # Handle relative dates in Vietnamese
        post_date_lower = post_date_str.lower()
        now = datetime.now()
        
        if 'hôm nay' in post_date_lower or 'today' in post_date_lower:
            return now
        elif 'hôm qua' in post_date_lower or 'yesterday' in post_date_lower:
            return now - timedelta(days=1)
        elif 'ngày trước' in post_date_lower or 'days ago' in post_date_lower:
            # Extract number: "2 ngày trước"
            match = re.search(r'(\d+)', post_date_str)
            if match:
                days_ago = int(match.group(1))
                return now - timedelta(days=days_ago)
        
        # If all parsing fails, log warning and use current time
        logger.warning(f"Could not parse post_date: '{post_date_str}', using current time")
        return now
        logger.warning(f"Could not parse post_date: '{post_date_str}', using current time")
        return now
    
    def process_message(self, message):
        """
        Process a single Kafka message and add to buffer
        
        Args:
            message: Kafka message object
        """
        try:
            # Parse JSON data
            data = json.loads(message.value().decode('utf-8'))
            
            # Extract province and timestamp from post_date
            province = data.get('province', 'unknown')
            post_date = data.get('post_date')
            
            # Parse post_date to get year and month
            dt = self._parse_post_date(post_date)
            year = dt.year
            month = dt.month
            
            # Get partition path
            partition_path = get_partition_path(province, year, month)
            
            # Add metadata
            data['_ingestion_timestamp'] = datetime.now().isoformat()
            data['_partition_province'] = normalize_province_name(province)
            data['_partition_year'] = year
            data['_partition_month'] = month
            data['_parsed_post_date'] = dt.isoformat()  # Add parsed date for reference
            
            # Add to buffer
            if partition_path not in self.data_buffer:
                self.data_buffer[partition_path] = []
            
            self.data_buffer[partition_path].append(data)
            self.message_count += 1
            
            logger.debug(f"Buffered message to {partition_path}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def flush_buffer(self, force=False):
        """
        Flush buffered data to MinIO
        
        Args:
            force: Force flush even if batch size not reached
        """
        for partition_path, records in list(self.data_buffer.items()):
            if force or len(records) >= self.batch_size:
                self._write_to_minio(partition_path, records)
                del self.data_buffer[partition_path]
    
    def _write_to_minio(self, partition_path, records):
        """
        Write records to MinIO in Parquet format
        
        Args:
            partition_path: Partition path in data lake
            records: List of records to write
        """
        try:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            object_name = f"{partition_path}/data_{timestamp}.parquet"
            
            # Convert records to DataFrame
            df = pd.DataFrame(records)
            
            # Write DataFrame to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(
                parquet_buffer,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            parquet_buffer.seek(0)
            
            # Upload to MinIO
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            logger.info(f"Wrote {len(records)} records to {object_name} ({parquet_buffer.getbuffer().nbytes} bytes)")
            
        except S3Error as e:
            logger.error(f"Error writing to MinIO: {e}")
        except Exception as e:
            logger.error(f"Unexpected error writing to MinIO: {e}", exc_info=True)
    
    def run(self):
        """
        Main consumer loop
        """
        logger.info("Starting Kafka to MinIO consumer...")
        
        try:
            self.setup()
            
            last_flush_time = datetime.now()
            
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Check if we should flush based on timeout
                    if (datetime.now() - last_flush_time).seconds >= self.batch_timeout:
                        self.flush_buffer(force=True)
                        last_flush_time = datetime.now()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                
                # Process message
                self.process_message(msg)
                
                # Commit offset
                self.consumer.commit(asynchronous=True)
                
                # Check if we should flush
                if self.message_count % self.batch_size == 0:
                    self.flush_buffer()
                    last_flush_time = datetime.now()
                
                # Log progress
                if self.message_count % 100 == 0:
                    logger.info(f"Processed {self.message_count} messages")
                    
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}", exc_info=True)
        finally:
            # Flush any remaining data
            logger.info("Flushing remaining data...")
            self.flush_buffer(force=True)
            
            # Close connections
            self.consumer.close()
            logger.info(f"Consumer stopped. Total messages processed: {self.message_count}")


def get_wsl_host_ip():
    """
    Get Windows host IP when running from WSL
    """
    try:
        # Get default gateway IP (Windows host IP in WSL)
        result = subprocess.run(
            ['ip', 'route', 'show', 'default'],
            capture_output=True,
            text=True,
            timeout=2
        )
        if result.returncode == 0:
            # Parse: default via 172.x.x.x dev eth0
            parts = result.stdout.strip().split()
            if len(parts) >= 3 and parts[0] == 'default' and parts[1] == 'via':
                return parts[2]
    except:
        pass
    return 'localhost'


def main():
    """
    Main entry point
    """
    # Auto-detect if running in WSL
    host = 'localhost'
    if os.path.exists('/proc/version'):
        try:
            with open('/proc/version', 'r') as f:
                if 'microsoft' in f.read().lower():
                    # Running in WSL, get Windows host IP
                    host = get_wsl_host_ip()
                    logger.info(f"Detected WSL environment, using Windows host IP: {host}")
        except:
            pass
    
    # Configuration - can be overridden by environment variables
    config = {
        'kafka_bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', f'{host}:9092'),
        'kafka_topic': os.getenv('KAFKA_TOPIC', 'batdongsan'),
        'kafka_group_id': os.getenv('KAFKA_GROUP_ID', 'minio-consumer-group'),
        'minio_endpoint': os.getenv('MINIO_ENDPOINT', f'{host}:9000'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        'bucket_name': os.getenv('MINIO_BUCKET', 'datalake'),
        'batch_size': int(os.getenv('BATCH_SIZE', '50')),
        'batch_timeout': int(os.getenv('BATCH_TIMEOUT', '30'))
    }
    
    logger.info(f"Kafka Bootstrap Servers: {config['kafka_bootstrap_servers']}")
    logger.info(f"MinIO Endpoint: {config['minio_endpoint']}")
    
    # Create and run consumer
    consumer = KafkaToMinIOConsumer(**config)
    consumer.run()


if __name__ == "__main__":
    main()
