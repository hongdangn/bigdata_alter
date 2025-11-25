from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType
import logging
import requests
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for real estate data
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
    StructField("num_bedrooms", StringType(), True),
    StructField("num_floors", StringType(), True),
    StructField("num_toilets", StringType(), True)
])

def create_spark_session():
    """Create Spark Session with Kafka and Elasticsearch configs"""
    # Note: Ensure the spark-sql-kafka and elasticsearch-spark versions match your Spark version
    spark = SparkSession.builder \
        .appName("BatDongSanStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.18.8") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.es.nodes.wan.only", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream(spark, kafka_bootstrap_servers, kafka_topic, es_host, es_index):
    """Read from Kafka, process, and write to Elasticsearch"""
    
    logger.info(f"Starting stream processing from Kafka topic: {kafka_topic}")
    
    # 1. Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # 2. Parse JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    processed_df = parsed_df \
        .withColumn(
            "kafka_timestamp",
            col("kafka_timestamp").cast("timestamp")
        ) \
        .withColumn(
            "processed_at",
            current_timestamp()
        ) \
        .withColumn(
            "post_timestamp",
            to_timestamp(col("post_date"), "dd/MM/yyyy")
        )
    
    query = processed_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", es_host) \
        .option("es.port", "9200") \
        .option("es.resource", f"{es_index}") \
        .option("es.mapping.id", "link") \
        .option("checkpointLocation", f"/tmp/checkpoint/{kafka_topic}") \
        .start()
    
    logger.info(f"Stream writing to Elasticsearch index: {es_index}")
    
    return query

def setup_elasticsearch_index(es_host, es_index):
    """
    Deletes the old index and creates a new one with EXPLICIT DATE MAPPINGS.
    This solves the 'type=long' issue.
    """
    es_url = f"http://{es_host}:9200/{es_index}"
    
    try:
        response = requests.delete(es_url)
        if response.status_code == 200:
            print(f"Deleted old Elasticsearch index: {es_index}")
        else:
            print(f"Index {es_index} did not exist or could not be deleted.")
    except Exception as e:
        print(f"Warning deleting index: {e}")

    mapping_body = {
        "mappings": {
            "properties": {
                "kafka_timestamp": {"type": "date"},
                "processed_at": {"type": "date"},
                "post_timestamp": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis||yyyy-MM-dd HH:mm:ss"
                }
            }
        }
    }

    try:
        response = requests.put(es_url, headers={"Content-Type": "application/json"}, data=json.dumps(mapping_body))
        if response.status_code == 200:
            print(f"Successfully created index '{es_index}' with Date Mappings.")
        else:
            print(f"Failed to create index. Status: {response.status_code}, Error: {response.text}")
    except Exception as e:
        logger.error(f"Error creating ES index: {e}")
        raise e

def main():
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "batdongsan"
    ES_HOST = "localhost"
    ES_INDEX = "batdongsan"

    setup_elasticsearch_index(ES_HOST, ES_INDEX)
    spark = create_spark_session()
    
    try:
        query = process_stream(
            spark, 
            KAFKA_BOOTSTRAP_SERVERS, 
            KAFKA_TOPIC, 
            ES_HOST, 
            ES_INDEX
        )
        
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in streaming: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()