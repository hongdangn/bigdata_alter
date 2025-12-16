"""
Batch ETL Job - Medallion Architecture (Bronze -> Silver -> Gold)
Processes real estate data from MinIO using PySpark

Architecture:
- Bronze (Raw): Parquet files from kafka_to_minio.py in raw/ partition
- Silver (Cleaned): Standardized, deduplicated data in silver/
- Gold (Analytics): Aggregated business metrics in gold/
"""
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, trim, lower, regexp_replace, to_date, 
    when, lit, concat_ws, avg, count, sum as spark_sum,
    year, month, dayofmonth, md5, concat, row_number
)
from pyspark.sql.types import DoubleType, FloatType, StringType
from pyspark.sql.window import Window
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RealEstateETL:
    """
    ETL Pipeline for Real Estate Data
    """
    
    def __init__(
        self,
        minio_endpoint="localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        bucket_name="datalake"
    ):
        """
        Initialize Spark session with S3/MinIO configuration
        """
        self.bucket_name = bucket_name
        
        # Create Spark Session
        self.spark = SparkSession.builder \
            .appName("RealEstateETL-Batch") \
            .config("spark.jars.packages", 
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Register UDFs
        self._register_udfs()
        
        logger.info("Spark session initialized successfully")
    
    def _register_udfs(self):
        """
        Register User Defined Functions for parsing Vietnamese text
        """
        # Parse price: "5.5 tỷ" -> 5500000000.0
        @udf(returnType=DoubleType())
        def parse_price(price_str):
            if not price_str:
                return None
            
            try:
                price_str = price_str.lower().strip()
                
                # Remove commas and spaces
                price_str = price_str.replace(',', '').replace(' ', '')
                
                # Extract number
                number_match = re.search(r'(\d+\.?\d*)', price_str)
                if not number_match:
                    return None
                
                value = float(number_match.group(1))
                
                # Handle units
                if 'tỷ' in price_str or 'ty' in price_str:
                    return value * 1_000_000_000
                elif 'triệu' in price_str or 'trieu' in price_str or 'tr' in price_str:
                    return value * 1_000_000
                elif 'nghìn' in price_str or 'nghin' in price_str:
                    return value * 1_000
                else:
                    # Assume million if no unit specified and value < 1000
                    if value < 1000:
                        return value * 1_000_000
                    return value
                    
            except Exception as e:
                return None
        
        # Parse square: "40 m2" -> 40.0
        @udf(returnType=FloatType())
        def parse_square(square_str):
            if not square_str:
                return None
            
            try:
                square_str = square_str.lower().strip()
                square_str = square_str.replace(',', '').replace(' ', '')
                
                # Extract number
                number_match = re.search(r'(\d+\.?\d*)', square_str)
                if number_match:
                    return float(number_match.group(1))
                return None
                
            except Exception as e:
                return None
        
        # Parse Vietnamese date: "Hôm nay", "10/12/2025", etc.
        @udf(returnType=StringType())
        def parse_vietnamese_date(date_str):
            if not date_str:
                return None
            
            try:
                date_str = date_str.strip().lower()
                today = datetime.now()
                
                # Handle relative dates
                if 'hôm nay' in date_str or 'hom nay' in date_str:
                    return today.strftime('%Y-%m-%d')
                elif 'hôm qua' in date_str or 'hom qua' in date_str:
                    return (today.replace(day=today.day - 1)).strftime('%Y-%m-%d')
                elif 'ngày trước' in date_str or 'ngay truoc' in date_str:
                    days_match = re.search(r'(\d+)', date_str)
                    if days_match:
                        days = int(days_match.group(1))
                        return (today.replace(day=today.day - days)).strftime('%Y-%m-%d')
                
                # Try parsing formatted dates
                date_formats = ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        return parsed_date.strftime('%Y-%m-%d')
                    except:
                        continue
                
                return None
                
            except Exception as e:
                return None
        
        self.parse_price_udf = parse_price
        self.parse_square_udf = parse_square
        self.parse_vietnamese_date_udf = parse_vietnamese_date
    
    def bronze_to_silver(self, raw_path=None, output_path=None, mode="overwrite", incremental_partition=None):
        """
        Phase 1: Transform Bronze (Raw) data to Silver (Cleaned) data
        
        Steps:
        1. Read raw Parquet files
        2. Parse and standardize fields
        3. Remove duplicates
        4. Add quality flags
        5. Write to Silver layer
        
        Args:
            raw_path: Path to raw data (default: s3a://datalake/raw/)
            output_path: Path to silver output (default: s3a://datalake/silver/)
            mode: Write mode ('overwrite' or 'append')
            incremental_partition: Filter for incremental processing (e.g., "_partition_year=2025 AND _partition_month=12")
        """
        raw_path = raw_path or f"s3a://{self.bucket_name}/raw/"
        output_path = output_path or f"s3a://{self.bucket_name}/silver/"
        
        logger.info(f"Starting Bronze -> Silver transformation")
        logger.info(f"Reading from: {raw_path}")
        
        try:
            # Read raw Parquet files with partition discovery
            df_raw = self.spark.read.parquet(raw_path)
            
            # Apply incremental filter if specified
            if incremental_partition:
                logger.info(f"Applying incremental filter: {incremental_partition}")
                df_raw = df_raw.filter(incremental_partition)
            
            initial_count = df_raw.count()
            logger.info(f"Loaded {initial_count} records from Bronze layer")
            
            # Standardization & Parsing
            df_silver = df_raw \
                .withColumn("price_vnd", self.parse_price_udf(col("price"))) \
                .withColumn("square_m2", self.parse_square_udf(col("square"))) \
                .withColumn("post_date_parsed", 
                           to_date(self.parse_vietnamese_date_udf(col("post_date")), "yyyy-MM-dd")) \
                .withColumn("title_clean", trim(lower(col("title")))) \
                .withColumn("province_clean", trim(col("province"))) \
                .withColumn("district_clean", trim(col("district"))) \
                .withColumn("ward_clean", trim(col("ward")))
            
            # Calculate price per m2
            df_silver = df_silver.withColumn(
                "price_per_m2",
                when(
                    (col("price_vnd").isNotNull()) & (col("square_m2").isNotNull()) & (col("square_m2") > 0),
                    col("price_vnd") / col("square_m2")
                ).otherwise(None)
            )
            
            # Add data quality flags
            df_silver = df_silver \
                .withColumn("has_price", col("price_vnd").isNotNull().cast("int")) \
                .withColumn("has_square", col("square_m2").isNotNull().cast("int")) \
                .withColumn("has_location", col("province_clean").isNotNull().cast("int")) \
                .withColumn("quality_score", 
                           col("has_price") + col("has_square") + col("has_location"))
            
            # Create deduplication key
            df_silver = df_silver.withColumn(
                "dedup_key",
                md5(concat_ws("||", col("link"), col("title_clean"), col("province_clean")))
            )
            
            # Remove duplicates - keep the latest record per dedup_key
            window_spec = Window.partitionBy("dedup_key").orderBy(col("_ingestion_timestamp").desc())
            df_silver = df_silver \
                .withColumn("row_num", row_number().over(window_spec)) \
                .filter(col("row_num") == 1) \
                .drop("row_num")
            
            dedup_count = df_silver.count()
            logger.info(f"After deduplication: {dedup_count} records (removed {initial_count - dedup_count} duplicates)")
            
            # Handle deduplication with existing Silver data if append mode
            if mode == "append":
                try:
                    # Read existing Silver data
                    df_existing = self.spark.read.parquet(output_path)
                    
                    # Union with new data
                    df_combined = df_existing.union(df_silver)
                    
                    # Deduplicate globally across old and new data
                    window_spec = Window.partitionBy("dedup_key").orderBy(col("_ingestion_timestamp").desc())
                    df_silver = df_combined \
                        .withColumn("row_num", row_number().over(window_spec)) \
                        .filter(col("row_num") == 1) \
                        .drop("row_num")
                    
                    logger.info(f"Merged with existing Silver data for deduplication")
                except Exception:
                    logger.info("No existing Silver data found, skipping merge")
            
            # Write to Silver layer
            logger.info(f"Writing to Silver layer: {output_path}")
            df_silver.write \
                .mode(mode) \
                .partitionBy("year", "month") \
                .parquet(output_path)
            
            logger.info(f"✓ Bronze -> Silver completed successfully")
            logger.info(f"  Total records: {dedup_count}")
            logger.info(f"  Output: {output_path}")
            
            return df_silver
            
        except Exception as e:
            logger.error(f"Error in Bronze -> Silver transformation: {e}", exc_info=True)
            raise
    
    def silver_to_gold(self, silver_path=None, output_path=None):
        """
        Phase 2: Transform Silver (Cleaned) data to Gold (Analytics) data
        
        Aggregations:
        1. Average price by district
        2. Daily posting counts
        3. Price trends by province
        4. Property type statistics
        
        Args:
            silver_path: Path to silver data (default: s3a://datalake/silver/)
            output_path: Path to gold output (default: s3a://datalake/gold/)
        """
        silver_path = silver_path or f"s3a://{self.bucket_name}/silver/"
        output_path = output_path or f"s3a://{self.bucket_name}/gold/"
        
        logger.info(f"Starting Silver -> Gold transformation")
        logger.info(f"Reading from: {silver_path}")
        
        try:
            # Read Silver data
            df_silver = self.spark.read.parquet(silver_path)
            
            total_records = df_silver.count()
            logger.info(f"Loaded {total_records} records from Silver layer")
            
            # === Analytics 1: District-level Aggregation ===
            df_district_agg = df_silver \
                .filter(col("price_vnd").isNotNull() & col("square_m2").isNotNull()) \
                .groupBy("province_clean", "district_clean") \
                .agg(
                    count("*").alias("listing_count"),
                    avg("price_vnd").alias("avg_price_vnd"),
                    avg("square_m2").alias("avg_square_m2"),
                    avg("price_per_m2").alias("avg_price_per_m2")
                )
            
            # === Analytics 2: Daily Posting Trends ===
            df_daily_trend = df_silver \
                .filter(col("post_date_parsed").isNotNull()) \
                .withColumn("post_year", year(col("post_date_parsed"))) \
                .withColumn("post_month", month(col("post_date_parsed"))) \
                .withColumn("post_day", dayofmonth(col("post_date_parsed"))) \
                .groupBy("province_clean", "post_year", "post_month", "post_day") \
                .agg(count("*").alias("daily_post_count"))
            
            # === Analytics 3: Province Summary ===
            df_province_summary = df_silver \
                .groupBy("province_clean") \
                .agg(
                    count("*").alias("total_listings"),
                    avg("price_vnd").alias("avg_price"),
                    spark_sum("has_price").alias("listings_with_price"),
                    spark_sum("has_square").alias("listings_with_square"),
                    spark_sum("has_location").alias("listings_with_location")
                )
            
            # === Analytics 4: Quality Metrics ===
            df_quality = df_silver \
                .groupBy("quality_score") \
                .agg(count("*").alias("record_count"))
            
            # Write Gold layer datasets
            logger.info("Writing Gold layer datasets...")
            
            df_district_agg.write \
                .mode("overwrite") \
                .parquet(f"{output_path}/district_aggregation/")
            logger.info(f"  ✓ District aggregation written")
            
            df_daily_trend.write \
                .mode("overwrite") \
                .parquet(f"{output_path}/daily_trends/")
            logger.info(f"  ✓ Daily trends written")
            
            df_province_summary.write \
                .mode("overwrite") \
                .parquet(f"{output_path}/province_summary/")
            logger.info(f"  ✓ Province summary written")
            
            # === Analytics 4: Data Quality Metrics ===
            df_quality = df_silver \
                .groupBy("quality_score") \
                .agg(count("*").alias("record_count"))
            
            df_quality.write \
                .mode("overwrite") \
                .parquet(f"{output_path}/quality_metrics/")
            logger.info(f"  ✓ Quality metrics written")
            
            logger.info(f"✓ Silver -> Gold completed successfully")
            logger.info(f"  Output: {output_path}")
            
            # Show sample analytics
            logger.info("\n=== Sample Analytics ===")
            logger.info("Top 5 Districts by Avg Price:")
            df_district_agg.orderBy(col("avg_price_vnd").desc()).show(5, truncate=False)
            
            logger.info("Province Summary:")
            df_province_summary.show(truncate=False)
            
        except Exception as e:
            logger.error(f"Error in Silver -> Gold transformation: {e}", exc_info=True)
            raise
    
    def run_incremental_pipeline(self, year, month):
        """
        Execute incremental ETL for a specific year/month partition
        
        Args:
            year: Year to process
            month: Month to process
        """
        logger.info("=" * 80)
        logger.info(f"STARTING INCREMENTAL ETL PIPELINE - {year}/{month:02d}")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Phase 1: Bronze -> Silver (incremental)
            logger.info("\n[PHASE 1] Bronze -> Silver (Incremental)")
            partition_filter = f"_partition_year={year} AND _partition_month={month}"
            self.bronze_to_silver(
                mode="append",
                incremental_partition=partition_filter
            )
            
            # Phase 2: Silver -> Gold (full refresh - re-aggregate all data)
            logger.info("\n[PHASE 2] Silver -> Gold (Full Refresh)")
            self.silver_to_gold(mode="overwrite")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("\n" + "=" * 80)
            logger.info(f"INCREMENTAL ETL COMPLETED - {year}/{month:02d}")
            logger.info(f"Total Duration: {duration:.2f} seconds")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Incremental ETL Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()
    
    def run_full_pipeline(self):
        """
        Execute complete ETL pipeline: Bronze -> Silver -> Gold (Full Refresh)
        """
        logger.info("="*80)
        logger.info("STARTING FULL ETL PIPELINE")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # Phase 1: Bronze -> Silver (full)
            logger.info("\n[PHASE 1] Bronze -> Silver (Full Refresh)")
            self.bronze_to_silver(mode="overwrite")
            
            # Phase 2: Silver -> Gold
            logger.info("\n[PHASE 2] Silver -> Gold")
            self.silver_to_gold()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("\n" + "="*80)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Total Duration: {duration:.2f} seconds")
            logger.info("="*80)
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()


def main():
    """
    Main entry point
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Real Estate ETL Batch Job')
    parser.add_argument('--mode', choices=['full', 'incremental'],
                       default='full', help='ETL mode: full refresh or incremental')
    parser.add_argument('--year', type=int,
                       help='Year for incremental processing')
    parser.add_argument('--month', type=int,
                       help='Month for incremental processing')
    parser.add_argument('--minio-endpoint', default='localhost:9000',
                       help='MinIO endpoint')
    parser.add_argument('--bucket', default='datalake',
                       help='MinIO bucket name')
    
    args = parser.parse_args()
    
    # Initialize ETL
    etl = RealEstateETL(
        minio_endpoint=args.minio_endpoint,
        bucket_name=args.bucket
    )
    
    # Run requested mode
    if args.mode == 'incremental':
        if not args.year or not args.month:
            # Default to current year/month
            now = datetime.now()
            year = args.year or now.year
            month = args.month or now.month
        else:
            year = args.year
            month = args.month
        
        etl.run_incremental_pipeline(year, month)
    else:
        etl.run_full_pipeline()


if __name__ == "__main__":
    main()
