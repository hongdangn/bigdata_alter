BOT_NAME = 'batdongsan'

SPIDER_MODULES = ['batdongsan.spiders']
NEWSPIDER_MODULE = 'batdongsan.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Configure item pipelines
ITEM_PIPELINES = {
    'batdongsan.pipelines.BatdongsanPipeline': 300,
    'batdongsan.pipelines.PushToKafka': 400,
}

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'
FEED_EXPORT_ENCODING = 'utf-8'

# Autothrottle settings
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# Enable and configure HTTP caching
# Set to False for production to always get fresh data
# Set to True for development/testing to save bandwidth
HTTPCACHE_ENABLED = False  # Changed: Disable cache for fresh data
HTTPCACHE_EXPIRATION_SECS = 3600  # Changed: Cache expires after 1 hour (if enabled)
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = []
HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Logging
LOG_LEVEL = 'INFO'