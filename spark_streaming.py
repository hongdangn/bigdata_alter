from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for real estate data
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", StringType(), True),
    StructField("square", StringType(), True),
    StructField("address", StringType(), True),
    StructField("post_date", StringType(), True),
    StructField("link", StringType(), True),
    StructField("num_bedrooms", StringType(), True),
    StructField("num_floors", StringType(), True),
    StructField("num_toilets", StringType(), True)
])

def create_spark_session():
    """Create Spark Session with Kafka and Elasticsearch configs"""
    spark = SparkSession.builder \
        .appName("BatDongSanStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.18.8") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream(spark, kafka_bootstrap_servers, kafka_topic, es_host, es_index):
    """Read from Kafka, process, and write to Elasticsearch"""
    
    logger.info(f"Starting stream processing from Kafka topic: {kafka_topic}")
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Add processing timestamp
    processed_df = parsed_df.withColumn("processed_at", current_timestamp())
    
    # Write to Elasticsearch
    query = processed_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", es_host) \
        .option("es.port", "9200") \
        .option("es.resource", f"{es_index}/_doc") \
        .option("es.mapping.id", "link") \
        .option("es.index.auto.create", "true") \
        .option("checkpointLocation", f"/tmp/checkpoint/{kafka_topic}") \
        .start()
    
    logger.info(f"Stream writing to Elasticsearch index: {es_index}")
    
    return query

def main():
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "batdongsan"  # Change based on your spider name
    ES_HOST = "localhost"
    ES_INDEX = "batdongsan"
    
    # Create Spark Session
    spark = create_spark_session()
    
    try:
        # Start streaming query
        query = process_stream(
            spark, 
            KAFKA_BOOTSTRAP_SERVERS, 
            KAFKA_TOPIC, 
            ES_HOST, 
            ES_INDEX
        )
        
        # Wait for termination
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in streaming: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
