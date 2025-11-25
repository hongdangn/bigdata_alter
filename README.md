# 🏠 Real Estate Data Pipeline

A complete end-to-end data pipeline for scraping, processing, and visualizing real estate data using Scrapy, Kafka, Spark Streaming, Elasticsearch, and Kibana.

## 📊 Architecture

```
┌─────────┐     ┌───────┐     ┌───────────────┐     ┌──────────────┐     ┌────────┐
│ Scrapy  │────▶│ Kafka │────▶│ Spark         │────▶│ Elasticsearch│────▶│ Kibana │
│ Crawler │     │       │     │ Streaming     │     │              │     │        │
└─────────┘     └───────┘     └───────────────┘     └──────────────┘     └────────┘
```

### Components:
- **Scrapy**: Web crawler for extracting real estate data
- **Kafka**: Message queue for data streaming
- **Spark Streaming**: Real-time data processing
- **Elasticsearch**: Search and analytics engine
- **Kibana**: Data visualization and dashboards

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Java 8 or 11 (for Spark)
- 8GB RAM minimum

### Installation

#### Option 1: Automated Setup (Recommended)

**On Linux/macOS:**
```bash
chmod +x quickstart.sh
./quickstart.sh
```

**On Windows:**
```cmd
quickstart.bat
```

#### Option 2: Manual Setup

1. **Clone and setup project:**
```bash
mkdir batdongsan_project && cd batdongsan_project
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Start infrastructure:**
```bash
docker-compose up -d
```

3. **Wait for services (about 60 seconds):**
```bash
# Check services
docker-compose ps
curl http://localhost:9200  # Elasticsearch
curl http://localhost:5601  # Kibana
```

4. **Create Kafka topic:**
```bash
docker exec kafka kafka-topics --create \
  --topic batdongsan \
  --bootstrap-server localhost:9093 \
  --partitions 3 \
  --replication-factor 1
```

5. **Start Spark Streaming (Terminal 1):**
```bash
source venv/bin/activate
python spark_streaming.py
```

6. **Start Scrapy crawler (Terminal 2):**
```bash
source venv/bin/activate
scrapy crawl batdongsan_spider
```

## 📁 Project Structure

```
batdongsan_project/
├── docker-compose.yml          # Infrastructure setup
├── requirements.txt            # Python dependencies
├── spark_streaming.py          # Spark Streaming application
├── monitor.py                  # Monitoring tool
├── quickstart.sh              # Linux/macOS setup script
├── quickstart.bat             # Windows setup script
├── batdongsan/
│   ├── __init__.py
│   ├── settings.py            # Scrapy settings
│   ├── items.py               # Data models (in pipelines.py)
│   ├── pipelines.py           # Scrapy pipelines + Kafka producer
│   └── spiders/
│       ├── __init__.py
│       └── batdongsan_spider.py  # Spider implementation
└── README.md
```

## 🔧 Configuration

### Scrapy Settings (`settings.py`)
```python
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
CONCURRENT_REQUESTS = 16
DOWNLOAD_DELAY = 1
```

### Spark Configuration (`spark_streaming.py`)
```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "batdongsan"
ES_HOST = "localhost"
ES_INDEX = "batdongsan"
```

## 📊 Monitoring

### Using the Monitor Tool
```bash
# Single check
python monitor.py

# Continuous monitoring (refresh every 10 seconds)
python monitor.py --loop 10
```

### Manual Checks

**Kafka Messages:**
```bash
docker exec -it kafka kafka-console-consumer \
  --topic batdongsan \
  --from-beginning \
  --bootstrap-server localhost:9093 \
  --max-messages 10
```

**Elasticsearch Data:**
```bash
# Count documents
curl http://localhost:9200/batdongsan/_count?pretty

# Get recent documents
curl http://localhost:9200/batdongsan/_search?pretty&size=5

# Index statistics
curl http://localhost:9200/_cat/indices?v
```

**Kafka Topics:**
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9093
```

**Consumer Groups:**
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 \
  --describe \
  --all-groups
```

## 📈 Kibana Dashboard Setup

1. **Access Kibana:** http://localhost:5601

2. **Create Index Pattern:**
   - Go to **Management → Stack Management → Index Patterns**
   - Click **Create index pattern**
   - Enter: `batdongsan*`
   - Select time field: `processed_at`
   - Click **Create**

3. **Create Visualizations:**

   **Price Distribution (Bar Chart):**
   - Analytics → Visualize → Create visualization → Lens
   - Add field: `price` (Average or Median)
   - Split by: `address` or custom ranges

   **Property Count by Area (Pie Chart):**
   - Create visualization → Pie
   - Slice by: `address`
   - Metric: Count

   **Time Series (Line Chart):**
   - Create visualization → Line
   - X-axis: `processed_at` (Date Histogram)
   - Y-axis: Count

   **Data Table:**
   - Create visualization → Table
   - Columns: title, price, address, num_bedrooms, square
   - Add search and filters

4. **Create Dashboard:**
   - Analytics → Dashboard → Create dashboard
   - Add your visualizations
   - Arrange and save

## 🛠️ Customization

### Scaling

**Increase Scrapy Performance:**
```python
# In settings.py
CONCURRENT_REQUESTS = 32
DOWNLOAD_DELAY = 0.25
AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0
```

**Scale Kafka Partitions:**
```bash
docker exec kafka kafka-topics --alter \
  --topic batdongsan \
  --partitions 6 \
  --bootstrap-server localhost:9093
```

**Increase Spark Resources:**
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000")
```

**Scale Elasticsearch:**
```yaml
# In docker-compose.yml
environment:
  - "ES_JAVA_OPTS=-Xms2g -Xmx2g"
```

## 🐛 Troubleshooting

### Issue: Kafka Connection Failed

**Symptoms:** Scrapy or Spark can't connect to Kafka

**Solutions:**
```bash
# Check Kafka is running
docker logs kafka

# Verify Kafka topic exists
docker exec kafka kafka-topics --list --bootstrap-server localhost:9093

# Restart Kafka
docker-compose restart kafka

# Check network connectivity
telnet localhost 9092
```

### Issue: No Data in Elasticsearch

**Check Pipeline Flow:**
```bash
# 1. Verify Scrapy is producing data
# Check Scrapy logs for "Sent X messages to Kafka"

# 2. Verify Kafka has messages
docker exec kafka kafka-console-consumer \
  --topic batdongsan \
  --from-beginning \
  --bootstrap-server localhost:9093 \
  --max-messages 5

# 3. Check Spark Streaming logs
# Look for errors in terminal where spark_streaming.py is running

# 4. Verify Elasticsearch received data
curl http://localhost:9200/batdongsan/_count
```

### Issue: Kibana Can't Connect to Elasticsearch

```bash
# Check Elasticsearch is running
curl http://localhost:9200

# Restart Kibana
docker-compose restart kibana

# Check Kibana logs
docker logs kibana
```

### Issue: Docker Containers Keep Restarting

```bash
# Check container logs
docker-compose logs [service_name]
docker stats

# Remove old volumes and restart
docker-compose down -v
docker-compose up -d
```

## 🧪 Testing

### Test Scrapy to Kafka:
```bash
# Run spider with limited items
scrapy crawl batdongsan_spider -s CLOSESPIDER_ITEMCOUNT=10

# Monitor Kafka for messages
docker exec kafka kafka-console-consumer \
  --topic batdongsan \
  --from-beginning \
  --bootstrap-server localhost:9093
```

### Test Kafka to Spark to Elasticsearch:
```bash
# 1. Send test message to Kafka
echo '{"title":"Test Property","price":"500000","link":"http://test.com"}' | \
  docker exec -i kafka kafka-console-producer \
    --topic batdongsan \
    --bootstrap-server localhost:9093

# 2. Check Elasticsearch
curl http://localhost:9200/batdongsan/_search?q=title:Test
```

### Clean Old Data:
```bash
# Delete old documents (older than 30 days)
curl -X POST "localhost:9200/batdongsan/_delete_by_query" \
  -H 'Content-Type: application/json' -d'
{
  "query": {
    "range": {
      "processed_at": {
        "lt": "now-30d"
      }
    }
  }
}'
```

### Reset Everything:
```bash
# Stop and remove all data
docker-compose down -v

# Remove checkpoint directories
rm -rf /tmp/checkpoint

# Start fresh
docker-compose up -d
```
