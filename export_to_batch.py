"""
Export Scrapy data to batch files
Saves scraped data to JSON/CSV for batch processing
"""

import json
from scrapy import Item, Field
from itemadapter import ItemAdapter
import os
from datetime import datetime
import csv
import logging

logger = logging.getLogger(__name__)


class ExportToBatchFile:
    """
    Pipeline to export scraped items to batch files
    Can be used alongside or instead of Kafka pipeline
    """
    
    def __init__(self, output_dir, file_format='json', batch_size=1000):
        self.output_dir = output_dir
        self.file_format = file_format
        self.batch_size = batch_size
        self.items_buffer = []
        self.file_counter = 0
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            output_dir=crawler.settings.get('BATCH_OUTPUT_DIR', './data/batch_input'),
            file_format=crawler.settings.get('BATCH_FILE_FORMAT', 'json'),
            batch_size=crawler.settings.get('BATCH_SIZE', 1000)
        )
    
    def open_spider(self, spider):
        """Create output directory if it doesn't exist"""
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Batch export initialized: {self.output_dir}")
        logger.info(f"Format: {self.file_format}, Batch size: {self.batch_size}")
    
    def close_spider(self, spider):
        """Flush remaining items"""
        if self.items_buffer:
            self._write_batch()
        logger.info(f"Batch export completed. Files created: {self.file_counter}")
    
    def process_item(self, item, spider):
        """Add item to buffer and write when batch size reached"""
        adapter = ItemAdapter(item)
        self.items_buffer.append(dict(adapter))
        
        if len(self.items_buffer) >= self.batch_size:
            self._write_batch()
        
        return item
    
    def _write_batch(self):
        """Write buffered items to file"""
        if not self.items_buffer:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_{timestamp}_{self.file_counter}.{self.file_format}"
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            if self.file_format == 'json':
                self._write_json(filepath)
            elif self.file_format == 'csv':
                self._write_csv(filepath)
            else:
                logger.error(f"Unsupported format: {self.file_format}")
                return
            
            logger.info(f"Written {len(self.items_buffer)} items to {filename}")
            self.file_counter += 1
            self.items_buffer = []
            
        except Exception as e:
            logger.error(f"Error writing batch file: {e}")
    
    def _write_json(self, filepath):
        """Write items as JSON"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.items_buffer, f, ensure_ascii=False, indent=2)
    
    def _write_csv(self, filepath):
        """Write items as CSV"""
        if not self.items_buffer:
            return
        
        keys = self.items_buffer[0].keys()
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(self.items_buffer)


# Add this to your scrapy settings.py to enable:
"""
# Enable batch export pipeline
ITEM_PIPELINES = {
    'batdongsan.pipelines.BatdongsanPipeline': 300,
    'batdongsan.pipelines.PushToKafka': 400,  # Keep for streaming
    'export_to_batch.ExportToBatchFile': 500,  # Add for batch
}

# Batch export configuration
BATCH_OUTPUT_DIR = './data/batch_input'
BATCH_FILE_FORMAT = 'json'  # or 'csv'
BATCH_SIZE = 1000  # items per file
"""
