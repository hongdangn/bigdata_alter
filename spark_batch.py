"""
Spark Batch Processing for Real Estate Data
Processes historical data from JSON/CSV files or database
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, regexp_replace, lower
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from pre_process import special_chars_list
import logging
import os
import regex as re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark Session for batch processing"""
    
    spark = SparkSession.builder \
        .appName("BatDongSanBatch") \
        .config("spark.jars.packages", 
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.18.8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_batch_data(spark, source_type, source_path):
    """
    Read batch data from various sources
    
    Args:
        spark: SparkSession
        source_type: 'json', 'csv', 'parquet', or 'kafka_replay'
        source_path: Path to data source
    """
    
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", StringType(), True),
        StructField("square", StringType(), True),
        StructField("province", StringType(), True),
        StructField("district", StringType(), True),
        StructField("ward", StringType(), True),
        StructField("post_date", StringType(), True),
        StructField("link", StringType(), True),
        StructField("num_bedrooms", IntegerType(), True),
        StructField("num_floors", IntegerType(), True),
        StructField("num_toilets", IntegerType(), True)
    ])
    
    if source_type == 'json':
        df = spark.read.schema(schema).json(source_path)
    elif source_type == 'csv':
        df = spark.read.schema(schema).option("header", "true").csv(source_path)
    elif source_type == 'parquet':
        df = spark.read.parquet(source_path)
    elif source_type == 'kafka_replay':
        # Read from Kafka from beginning (for reprocessing)
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "batdongsan") \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # Parse Kafka messages
        from pyspark.sql.functions import from_json
        df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
    else:
        raise ValueError(f"Unsupported source type: {source_type}")
    
    logger.info(f"Read {df.count()} records from {source_type} source")
    return df


def process_batch_data(df):
    """
    Apply same transformations as streaming
    """
    
    # Add processing timestamp
    processed_df = df.withColumn("processed_at", current_timestamp())
    
    # Text cleaning - same as streaming
    special_chars_str = "".join(special_chars_list)
    regex_pattern = f"[{re.escape(special_chars_str)}]"
    dedup_punct_pattern = r"([^\w\s])\s*\1+"
    
    # Convert to lowercase
    processed_df = processed_df.withColumn("title", lower(col("title")))
    processed_df = processed_df.withColumn("description", lower(col("description")))
    
    # Remove special characters
    processed_df = processed_df.withColumn(
        "title", 
        regexp_replace(col("title"), regex_pattern, "")
    )
    processed_df = processed_df.withColumn(
        "description", 
        regexp_replace(col("description"), regex_pattern, "")
    )
    
    # Remove duplicate punctuation
    processed_df = processed_df.withColumn(
        "title", 
        regexp_replace(col("title"), dedup_punct_pattern, "$1")
    )
    processed_df = processed_df.withColumn(
        "description", 
        regexp_replace(col("description"), dedup_punct_pattern, "$1")
    )
    
    # Remove duplicates based on link
    processed_df = processed_df.dropDuplicates(["link"])
    
    # Filter out invalid records
    processed_df = processed_df.filter(
        (col("title").isNotNull()) & 
        (col("link").isNotNull()) &
        (col("title") != "")
    )
    
    logger.info(f"Processed {processed_df.count()} valid records")
    return processed_df


def write_to_elasticsearch(df, es_host, es_index, mode="append"):
    """
    Write batch data to Elasticsearch
    
    Args:
        mode: 'append' or 'overwrite'
    """
    
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .mode(mode) \
        .option("es.nodes", es_host) \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", es_index) \
        .option("es.mapping.id", "link") \
        .option("es.write.operation", "upsert") \
        .option("es.mapping.date.rich", "false") \
        .save()
    
    logger.info(f"Written data to Elasticsearch index: {es_index}")


def write_to_parquet(df, output_path, partition_by=None):
    """
    Save processed data to Parquet for data lake
    """
    
    if partition_by:
        df.write.mode("overwrite").partitionBy(partition_by).parquet(output_path)
    else:
        df.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Saved data to Parquet: {output_path}")


def aggregate_statistics(df):
    """
    Generate aggregated statistics from batch data
    """
    
    # Statistics by province
    province_stats = df.groupBy("province").agg(
        f.count("*").alias("total_listings"),
        f.avg("num_bedrooms").alias("avg_bedrooms"),
        f.avg("num_floors").alias("avg_floors")
    )
    
    # Statistics by district
    district_stats = df.groupBy("province", "district").agg(
        f.count("*").alias("total_listings"),
        f.countDistinct("link").alias("unique_listings")
    )
    
    return {
        "province_stats": province_stats,
        "district_stats": district_stats
    }


def main():
    """
    Main batch processing pipeline
    """
    
    # Configuration
    SOURCE_TYPE = "json"  # Change to: 'json', 'csv', 'parquet', 'kafka_replay'
    SOURCE_PATH = "./data/batch_input/*.json"  # Your data path
    ES_HOST = "localhost"
    ES_INDEX = "batdongsan"
    OUTPUT_PATH = "./data/processed_batch"
    
    spark = create_spark_session()
    
    try:
        # Read batch data
        logger.info("Reading batch data...")
        df = read_batch_data(spark, SOURCE_TYPE, SOURCE_PATH)
        
        # Process data
        logger.info("Processing batch data...")
        processed_df = process_batch_data(df)
        
        # Write to Elasticsearch
        logger.info("Writing to Elasticsearch...")
        write_to_elasticsearch(processed_df, ES_HOST, ES_INDEX, mode="append")
        
        # Save to Parquet (optional - for data lake)
        logger.info("Saving to Parquet...")
        write_to_parquet(processed_df, OUTPUT_PATH, partition_by=["province"])
        
        # Generate statistics (optional)
        logger.info("Generating statistics...")
        stats = aggregate_statistics(processed_df)
        
        # Write statistics to separate index
        stats["province_stats"].write \
            .format("org.elasticsearch.spark.sql") \
            .mode("overwrite") \
            .option("es.nodes", ES_HOST) \
            .option("es.port", "9200") \
            .option("es.resource", "batdongsan_stats_province") \
            .save()
        
        logger.info("Batch processing completed successfully!")
        
        # Show sample statistics
        print("\n=== Province Statistics ===")
        stats["province_stats"].show(10, truncate=False)
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
