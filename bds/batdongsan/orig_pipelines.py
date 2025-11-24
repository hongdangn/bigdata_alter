# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import pickle
import json
import math
import sys

import numpy as np
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.metrics.pairwise import cosine_similarity
import demoji
# import underthesea
# import geoapivietnam
# from vnaddress import VNAddressStandardizer
# import pymongo
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from scrapy.utils.serialize import ScrapyJSONEncoder
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer


TFIDF_VECTORIZER_PATH = './save/ha-noi/tfidf_model.pkl'

class BatdongsanPipeline:
    def process_item(self, item, spider):
        return item
    
class PushToKafka:
    """
    Publishes a serialized item into a Kafka topic

    :param producer: The Kafka producer
    :type producer: kafka.producer.Producer

    :param topic: The Kafka topic being used
    :type topic: str or unicode
    """
    def __init__(self, kafka_bootstrap_servers):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = Producer({'bootstrap.servers': kafka_bootstrap_servers})

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            kafka_bootstrap_servers=crawler.settings.get('KAFKA_BOOTSTRAP_SERVERS')
        )
    
    def open_spider(self, spider):
        self.topic = spider.name.replace('_spider', '')
    
    def close_spider(self, spider):
        self.producer.flush()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        msg = json.dumps(adapter.asdict(), ensure_ascii=False)
        self.producer.produce(self.topic, msg, callback=self._delivery_report)
        return item
    
    def _delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

