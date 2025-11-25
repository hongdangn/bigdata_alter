import json
from scrapy import Item, Field
from itemadapter import ItemAdapter
from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)


class BatdongsanItem(Item):
    """Define the data structure for real estate items"""
    title = Field()
    description = Field()
    price = Field()
    square = Field()
    # address = Field()
    province = Field()
    district = Field()
    ward = Field()
    post_date = Field()
    link = Field()
    num_bedrooms = Field()
    num_floors = Field()
    num_toilets = Field()


class BatdongsanPipeline:
    """
    Basic pipeline for data cleaning and validation
    """
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Clean whitespace from all text fields
        for field in adapter.field_names():
            value = adapter.get(field)
            if isinstance(value, str):
                adapter[field] = value.strip()
        
        # Validate required fields
        if not adapter.get('title'):
            logger.warning(f"Item missing title: {adapter.get('link')}")
        
        if not adapter.get('link'):
            logger.error("Item missing link - dropping item")
            return None
        
        logger.info(f"Processed item: {adapter.get('title', 'N/A')}")
        return item


class PushToKafka:
    """
    Publishes a serialized item into a Kafka topic

    :param kafka_bootstrap_servers: Kafka broker addresses
    :type kafka_bootstrap_servers: str

    The topic name is automatically derived from the spider name
    by removing '_spider' suffix (e.g., 'batdongsan_spider' -> 'batdongsan')
    """
    
    def __init__(self, kafka_bootstrap_servers):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.topic = None
        self.message_count = 0
        
    @classmethod
    def from_crawler(cls, crawler):
        """
        Factory method to create pipeline instance from crawler
        """
        return cls(
            kafka_bootstrap_servers=crawler.settings.get('KAFKA_BOOTSTRAP_SERVERS')
        )
    
    def open_spider(self, spider):
        """
        Called when spider is opened
        Initialize Kafka producer and determine topic name
        """
        # Initialize Kafka producer
        self.producer = Producer({
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'client.id': f'{spider.name}_producer',
            'linger.ms': 10,  # Wait up to 10ms to batch messages
            'compression.type': 'snappy',  # Compress messages
            'acks': 1,  # Wait for leader acknowledgment
        })
        
        # Determine topic name from spider name
        self.topic = "batdongsan"
        
        logger.info(f"Kafka producer initialized for topic: {self.topic}")
        logger.info(f"Bootstrap servers: {self.kafka_bootstrap_servers}")
    
    def close_spider(self, spider):
        """
        Called when spider is closed
        Flush remaining messages and close producer
        """
        if self.producer:
            logger.info("Flushing Kafka producer...")
            self.producer.flush(timeout=30)
            logger.info(f"Successfully sent {self.message_count} messages to Kafka")
    
    def process_item(self, item, spider):
        """
        Process item and send to Kafka
        """
        try:
            # Convert item to dict and serialize to JSON
            adapter = ItemAdapter(item)
            item_dict = dict(adapter)
            
            # Ensure all values are JSON serializable
            for key, value in item_dict.items():
                if value is None:
                    item_dict[key] = ""
            
            msg = json.dumps(item_dict, ensure_ascii=False)
            
            # Produce message to Kafka
            # Use link as key for partitioning
            key = item_dict.get('link', '').encode('utf-8')
            
            self.producer.produce(
                topic=self.topic,
                value=msg.encode('utf-8'),
                key=key,
                callback=self._delivery_report
            )
            
            # Poll to handle delivery reports
            self.producer.poll(0)
            
            self.message_count += 1
            
            # Log every 10 messages
            if self.message_count % 10 == 0:
                logger.info(f"Sent {self.message_count} messages to Kafka")
            
        except Exception as e:
            logger.error(f"Error sending item to Kafka: {e}")
            logger.error(f"Item: {item}")
        
        return item
    
    def _delivery_report(self, err, msg):
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().
        """
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
            logger.error(f'Failed message: topic={msg.topic()}, partition={msg.partition()}')
        else:
            # Only log detailed delivery for debugging
            logger.debug(
                f'Message delivered to {msg.topic()} '
                f'[partition {msg.partition()}] '
                f'at offset {msg.offset()}'
            )


class DataValidationPipeline:
    """
    Optional: Advanced data validation and enrichment pipeline
    """
    
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        # Validate and normalize price
        price = adapter.get('price', '')
        if price:
            # Remove non-numeric characters except decimal point
            import re
            numeric_price = re.sub(r'[^\d.]', '', str(price))
            if numeric_price:
                adapter['price'] = numeric_price
        
        # Validate and normalize square meters
        square = adapter.get('square', '')
        if square:
            import re
            numeric_square = re.sub(r'[^\d.]', '', str(square))
            if numeric_square:
                adapter['square'] = numeric_square
        
        # Add validation timestamp
        from datetime import datetime
        adapter['scraped_at'] = datetime.now().isoformat()
        
        return item