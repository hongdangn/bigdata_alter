from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, regexp_replace
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, FloatType, StructType, StructField, BooleanType, IntegerType, ArrayType, DoubleType
from pre_process import special_chars_list, remove_special_chars, remove_duplicate_punctuation_sequence, remove_special_chars_uds
import logging
import regex as re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define schema for real estate data
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("price", StringType(), True),
    StructField("square", StringType(), True),
    # StructField("address", StringType(), True),
    StructField("province", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),

    StructField("post_date", StringType(), True),
    StructField("link", StringType(), True),
    StructField("num_bedrooms", IntegerType(), True),
    StructField("num_floors", IntegerType(), True),
    StructField("num_toilets", IntegerType(), True)
])

def create_spark_session():
    """Create Spark Session with Kafka and Elasticsearch configs"""
    spark = SparkSession.builder \
        .appName("BatDongSanStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.18.8") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream(spark, kafka_bootstrap_servers, kafka_topic, es_host, es_index):
    logger.info(f"Starting stream processing from Kafka topic: {kafka_topic}")
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    processed_df = parsed_df \
        .withColumn(
            "processed_at",
            current_timestamp()  # FIX 2: already a proper timestamp
        ) \
        
    special_chars_str = "".join(special_chars_list)
    regex_pattern = f"[{re.escape(special_chars_str)}]"
    dedup_punct_pattern = r"([^\w\s])\s*\1+"

    processed_df = processed_df.withColumn("title", f.lower(f.col("title")))
    processed_df = processed_df.withColumn("description", f.lower(f.col("description")))
        
    processed_df = processed_df.withColumn(
        "title", 
        f.regexp_replace(f.col("title"), regex_pattern, "")
    )
    processed_df = processed_df.withColumn(
        "description", 
        f.regexp_replace(f.col("description"), regex_pattern, "")
    )

    processed_df = processed_df.withColumn(
        "title", 
        f.regexp_replace(f.col("title"), dedup_punct_pattern, "$1")
    )
    processed_df = processed_df.withColumn(
        "description", 
        f.regexp_replace(f.col("description"), dedup_punct_pattern, "$1")
    )

    query = processed_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", es_host) \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "true") \
        .option("es.resource", f"{es_index}") \
        .option("es.mapping.id", "link") \
        .option("es.index.auto.create", "true") \
        .option("es.mapping.date.rich", "false") \
        .option("checkpointLocation", f"/tmp/checkpoint/{kafka_topic}") \
        .start()
    
    logger.info(f"Stream writing to Elasticsearch index: {es_index}")
    
    return query

def main():
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "batdongsan"
    ES_HOST = "localhost"
    ES_INDEX = "batdongsan"

    import requests
    try:
        requests.delete(f"http://{ES_HOST}:9200/{ES_INDEX}")
        print("Deleted old Elasticsearch index.")
    except:
        print("Index did not exist.")
    
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
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
