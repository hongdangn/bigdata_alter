"""
Gold to Kafka Topics - Send ranked analytics to Kafka for Elasticsearch
Creates hot_area and luxury topics by province
"""
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from minio import Minio
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoldToKafkaPublisher:
    """
    Publishes Gold layer analytics to Kafka topics for Elasticsearch
    """
    
    def __init__(
        self,
        kafka_bootstrap_servers="localhost:9092",
        minio_endpoint="localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        bucket_name="datalake"
    ):
        """Initialize MinIO client and Kafka producer"""
        self.bucket_name = bucket_name
        
        # Connect to MinIO
        try:
            self.minio_client = Minio(
                minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=False
            )
            logger.info(f"Connected to MinIO at {minio_endpoint}")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise
        
        # Create Kafka Producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer connected to {kafka_bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def create_topics_if_not_exist(self, topics):
        """Create Kafka topics if they don't exist"""
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.producer.config['bootstrap_servers']
            )
            
            topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1) 
                          for topic in topics]
            
            try:
                admin_client.create_topics(new_topics=topic_list, validate_only=False)
                logger.info(f"Created {len(topics)} topics")
            except TopicAlreadyExistsError:
                logger.info("Topics already exist")
            
            admin_client.close()
            
        except Exception as e:
            logger.warning(f"Could not create topics (may already exist): {e}")
    
    def read_district_aggregation(self):
        """Read district_aggregation from MinIO Gold layer"""
        try:
            # List all parquet files in district_aggregation
            objects = list(self.minio_client.list_objects(
                self.bucket_name,
                prefix="gold/district_aggregation/",
                recursive=True
            ))
            
            parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
            
            if not parquet_files:
                logger.error("No parquet files found in gold/district_aggregation/")
                return None
            
            # Download and read first parquet file
            temp_file = "temp_district_agg.parquet"
            self.minio_client.fget_object(
                self.bucket_name,
                parquet_files[0].object_name,
                temp_file
            )
            
            df = pd.read_parquet(temp_file)
            logger.info(f"Loaded {len(df)} districts from Gold layer")
            
            import os
            os.remove(temp_file)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading district aggregation: {e}", exc_info=True)
            return None
        


    def read_listing_counts(self):
        """Read district_aggregation from MinIO Gold layer"""
        try:
            # List all parquet files in district_aggregation
            objects = list(self.minio_client.list_objects(
                self.bucket_name,
                prefix="gold/daily_trends/",
                recursive=True
            ))
            
            parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
            
            if not parquet_files:
                logger.error("No parquet files found in gold/daily_trends/")
                return None
            
            # Download and read first parquet file
            temp_file = "temp_daily.parquet"
            self.minio_client.fget_object(
                self.bucket_name,
                parquet_files[0].object_name,
                temp_file
            )
            
            df = pd.read_parquet(temp_file)
            logger.info(f"Loaded {len(df)} districts from Gold layer")
            
            import os
            os.remove(temp_file)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading daily counts: {e}", exc_info=True)
            return None
    
    def publish_hot_areas(self, df):
        """
        Publish hot areas (ranked by listing count) to Kafka
        
        Topics:
        - batch.hot_area.ha_noi
        - batch.hot_area.ho_chi_minh
        
        Message format:
        {
            "district": "Quận Ba Đình",
            "listing_count": 29,
            "rank": 1,
            "province": "ha_noi",
            "timestamp": "2025-12-16T10:30:00",
            "@timestamp": "2025-12-16T10:30:00.000Z"
        }
        """
        logger.info("=" * 80)
        logger.info("PUBLISHING HOT AREAS TO KAFKA")
        logger.info("=" * 80)
        
        provinces = {
            "ha_noi": "batch.hot_area.ha_noi",
            "ho_chi_minh": "batch.hot_area.ho_chi_minh"
        }
        
        # Create topics if not exist
        self.create_topics_if_not_exist(list(provinces.values()))
        
        sent_count = {province: 0 for province in provinces.keys()}
        timestamp = datetime.utcnow()
        
        for province, topic in provinces.items():
            # Filter by province
            df_province = df[df['province_clean'] == province].copy()
            
            if df_province.empty:
                logger.warning(f"No data for province: {province}")
                continue
            
            # Sort by listing_count DESC and add rank
            df_province = df_province.sort_values('listing_count', ascending=False)
            df_province['rank'] = range(1, len(df_province) + 1)
            
            # Send each district to Kafka
            for _, row in df_province.iterrows():
                message = {
                    "district": str(row['district_clean']),
                    "listing_count": int(row['listing_count']),
                    "rank": int(row['rank']),
                    "province": province,
                    "timestamp": timestamp.isoformat(),
                    "@timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z"),  # ES format
                    "category": "hot_area"
                }
                
                try:
                    future = self.producer.send(
                        topic,
                        key=str(row['district_clean']),
                        value=message
                    )
                    future.get(timeout=10)
                    sent_count[province] += 1
                except KafkaError as e:
                    logger.error(f"Failed to send to {topic}: {e}")
        
        self.producer.flush()
        
        logger.info("\n✅ Hot Areas Published:")
        for province, topic in provinces.items():
            logger.info(f"   {topic}: {sent_count[province]} districts")
    
    def publish_luxury_areas(self, df):
        """
        Publish luxury areas (ranked by avg price per m2) to Kafka
        
        Topics:
        - batch.luxury.ha_noi
        - batch.luxury.ho_chi_minh
        
        Message format:
        {
            "district": "Quận Ba Đình",
            "rank": 1,
            "avg_price_vnd": 29680689655.17,
            "avg_square_m2": 52.45,
            "avg_price_per_m2": 565919099.98,
            "province": "ha_noi",
            "timestamp": "2025-12-16T10:30:00",
            "@timestamp": "2025-12-16T10:30:00.000Z"
        }
        """
        logger.info("=" * 80)
        logger.info("PUBLISHING LUXURY AREAS TO KAFKA")
        logger.info("=" * 80)
        
        provinces = {
            "ha_noi": "batch.luxury.ha_noi",
            "ho_chi_minh": "batch.luxury.ho_chi_minh"
        }
        
        # Create topics if not exist
        self.create_topics_if_not_exist(list(provinces.values()))
        
        sent_count = {province: 0 for province in provinces.keys()}
        timestamp = datetime.utcnow()
        
        for province, topic in provinces.items():
            # Filter by province
            df_province = df[df['province_clean'] == province].copy()
            
            if df_province.empty:
                logger.warning(f"No data for province: {province}")
                continue
            
            # Sort by avg_price_per_m2 DESC and add rank
            df_province = df_province.sort_values('avg_price_per_m2', ascending=False)
            df_province['rank'] = range(1, len(df_province) + 1)
            
            # Send each district to Kafka
            for _, row in df_province.iterrows():
                message = {
                    "district": str(row['district_clean']),
                    "rank": int(row['rank']),
                    "avg_price_vnd": float(row['avg_price_vnd']) if pd.notna(row['avg_price_vnd']) else None,
                    "avg_square_m2": float(row['avg_square_m2']) if pd.notna(row['avg_square_m2']) else None,
                    "avg_price_per_m2": float(row['avg_price_per_m2']) if pd.notna(row['avg_price_per_m2']) else None,
                    "province": province,
                    "timestamp": timestamp.isoformat(),
                    "@timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z"),  # ES format
                    "category": "luxury"
                }
                
                try:
                    future = self.producer.send(
                        topic,
                        key=str(row['district_clean']),
                        value=message
                    )
                    future.get(timeout=10)
                    sent_count[province] += 1
                except KafkaError as e:
                    logger.error(f"Failed to send to {topic}: {e}")
        
        self.producer.flush()
        
        logger.info("\n✅ Luxury Areas Published:")
        for province, topic in provinces.items():
            logger.info(f"   {topic}: {sent_count[province]} districts")
    
    def publish_daily_trends(self, df):
        """
        Publish daily number of listings trends to Kafka
        
        Topics:
        - batch.statistic.ha_noi
        - batch.statistic.ho_chi_minh
        
        Message format:
        {
            "year": 2025,
            "month": 12,
            "day" : 16,
            "count": 150,
            "province": "ha_noi",
            "timestamp": "2025-12-16T10:30:00",
            "@timestamp": "2025-12-16T10:30:00.000Z"
        }
        """
        logger.info("=" * 80)
        logger.info("PUBLISHING DAILY POST COUNT TO KAFKA")
        logger.info("=" * 80)
        
        provinces = {
            "ha_noi": "batch.statistic.ha_noi",
            "ho_chi_minh": "batch.statistic.ho_chi_minh"
        }
        
        # Create topics if not exist
        self.create_topics_if_not_exist(list(provinces.values()))
        
        sent_count = {province: 0 for province in provinces.keys()}
        timestamp = datetime.now()

        # Calculate cutoff date (30 days ago)
        cutoff_date = timestamp - timedelta(days=30)
        
        # Create date column for filtering
        df['post_date'] = pd.to_datetime(df[['post_year', 'post_month', 'post_day']].rename(
            columns={'post_year': 'year', 'post_month': 'month', 'post_day': 'day'}
        ))
        
        for province, topic in provinces.items():
            # Filter by province and last 30 days
            df_province = df[
                (df['province_clean'] == province) & 
                (df['post_date'] >= cutoff_date)
            ].copy()
            
            if df_province.empty:
                logger.warning(f"No data for province: {province}")
                continue
            
            # Sort by post_day
            df_province = df_province.sort_values('post_day' , ascending = False)

            
            # Send each day count to Kafka
            for _, row in df_province.iterrows():
                message = {
                    "year": int(row['post_year']),
                    "month": int(row['post_month']),
                    "day" : int(row['post_day']),
                    "count": int(row['daily_post_count']),
                    "province": province,
                    "timestamp": timestamp.isoformat(),
                    "@timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z"),  # ES format
                    "category": "trends"
                }
                
                try:
                    future = self.producer.send(
                        topic,
                        key=str(row['post_day']),
                        value=message
                    )
                    future.get(timeout=10)
                    sent_count[province] += 1
                except KafkaError as e:
                    logger.error(f"Failed to send to {topic}: {e}")
        
        self.producer.flush()
        
        logger.info("\n✅ Daily number of listings published (last 30 days):")
        for province, topic in provinces.items():
            logger.info(f"   {topic}: {sent_count[province]} days")
    

    def publish_all(self , df, df_count):
        """Publish all analytics topics"""
        logger.info("=" * 80)
        logger.info("PUBLISHING ALL ANALYTICS TO KAFKA")
        logger.info("=" * 80)
        
        try:
            
            if df is None or df_count is None:
                logger.error("Failed to read aggregation")
                return
            
            # Publish hot areas
            self.publish_hot_areas(df)
            
            # Publish luxury areas
            self.publish_luxury_areas(df)

            # Publish daily trends
            self.publish_daily_trends(df_count)

            logger.info("\n" + "=" * 80)
            logger.info("✅ ALL ANALYTICS PUBLISHED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info("\nTopics created:")
            logger.info("  - batch.hot_area.ha_noi")
            logger.info("  - batch.hot_area.ho_chi_minh")
            logger.info("  - batch.luxury.ha_noi")
            logger.info("  - batch.luxury.ho_chi_minh")
            logger.info("  - batch.statistic.ha_noi")
            logger.info("  - batch.statistic.ho_chi_minh")
            logger.info("\nNext steps:")
            logger.info("  1. Configure Elasticsearch connector for these topics")
            logger.info("  2. Or run streaming consumer to push to Elasticsearch")
            
        except Exception as e:
            logger.error(f"Error publishing analytics: {e}", exc_info=True)
            raise
        finally:
            self.producer.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Publish Gold Analytics to Kafka Topics')
    parser.add_argument('--mode', choices=['hot', 'luxury', 'count', 'all'],
                       default='all',
                       help='Publishing mode: hot areas, luxury areas, daily counts or all')
    parser.add_argument('--kafka-bootstrap', default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--minio-endpoint', default='localhost:9000',
                       help='MinIO endpoint')
    parser.add_argument('--bucket', default='datalake',
                       help='MinIO bucket name')
    
    args = parser.parse_args()
    
    # Initialize publisher
    publisher = GoldToKafkaPublisher(
        kafka_bootstrap_servers=args.kafka_bootstrap,
        minio_endpoint=args.minio_endpoint,
        bucket_name=args.bucket
    )
    
    try:
        # Read data
        df = publisher.read_district_aggregation()
        df_count = publisher.read_listing_counts()
        
        if df is None or df_count is None:
            logger.error("Failed to read data")
            sys.exit(1)
        
        # Publish based on mode
        if args.mode == 'hot':
            publisher.publish_hot_areas(df)
        elif args.mode == 'luxury':
            publisher.publish_luxury_areas(df)
        elif args.mode == 'count':
            publisher.publish_daily_trends(df_count)
        else:
            publisher.publish_all(df, df_count)
            
    except KeyboardInterrupt:
        logger.info("\n\n❌ Cancelled by user")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        publisher.producer.close()


if __name__ == "__main__":
    main()
