# Quick Start Guide: Batch Processing

## 🚀 Khởi động nhanh Batch Processing

### Bước 1: Cài đặt dependencies
```bash
pip install apscheduler
```

### Bước 2: Tạo thư mục
```bash
mkdir data
mkdir data\batch_input
mkdir data\processed_batch
```

### Bước 3: Chọn mode hoạt động

#### Option A: Chạy Batch ngay lập tức
```bash
python spark_batch.py
```

#### Option B: Chạy Batch theo lịch
```bash
python batch_scheduler.py
```

#### Option C: Sử dụng Unified Manager (Recommended)
```bash
python unified_pipeline.py
# Chọn option trong menu
```

### Bước 4: Kích hoạt Batch Export trong Scrapy (Optional)

Mở file `bds/batdongsan/settings.py` và uncomment phần batch config:

```python
# Uncomment những dòng này:
ITEM_PIPELINES = {
    'batdongsan.pipelines.BatdongsanPipeline': 300,
    'batdongsan.pipelines.PushToKafka': 400,
    'export_to_batch.ExportToBatchFile': 500,  # ← Thêm dòng này
}
BATCH_OUTPUT_DIR = '../data/batch_input'
BATCH_FILE_FORMAT = 'json'
BATCH_SIZE = 1000
```

### Bước 5: Kiểm tra kết quả

```bash
# Check batch files
dir data\batch_input

# Check processed files
dir data\processed_batch

# Check Elasticsearch
curl http://localhost:9200/batdongsan/_count
curl http://localhost:9200/batdongsan_stats_province/_search?pretty
```

---

## 📋 Workflow Examples

### Example 1: Chạy full pipeline (Streaming + Batch)

```bash
# Terminal 1: Start Docker services
docker-compose up -d

# Terminal 2: Start Streaming
python spark_streaming.py

# Terminal 3: Start Batch Scheduler
python batch_scheduler.py

# Terminal 4: Start Crawler
cd bds
scrapy crawl bds_spider
```

### Example 2: Chỉ xử lý Batch từ files có sẵn

```bash
# 1. Có data trong data/batch_input/*.json
# 2. Run batch processing
python spark_batch.py
# 3. Check results trong Elasticsearch và Parquet
```

### Example 3: Replay data từ Kafka

Sửa trong `spark_batch.py`:
```python
SOURCE_TYPE = "kafka_replay"
SOURCE_PATH = None
```

Chạy:
```bash
python spark_batch.py
```

---

## ⚡ Lệnh hữu ích

```bash
# Install dependencies
pip install -r requirements.txt

# Check Docker services
docker-compose ps

# View Elasticsearch indices
curl http://localhost:9200/_cat/indices?v

# Count documents
curl http://localhost:9200/batdongsan/_count

# View province stats
curl http://localhost:9200/batdongsan_stats_province/_search?pretty

# Monitor crawler
cd bds
scrapy crawl bds_spider --logfile=crawler.log

# Check batch scheduler logs
python batch_scheduler.py > scheduler.log 2>&1 &
tail -f scheduler.log
```

---

## 🎯 Recommended Setup

Để có trải nghiệm tốt nhất, chạy theo thứ tự:

```bash
# 1. Start infrastructure
docker-compose up -d
timeout /t 30  # Wait 30 seconds

# 2. Create Kafka topic (if not exists)
docker exec kafka kafka-topics --create --topic batdongsan --bootstrap-server localhost:9093 --partitions 3 --replication-factor 1

# 3. Start unified pipeline manager
python unified_pipeline.py

# 4. Chọn option 4: "Start Both (Hybrid)"
```

Xong! Hệ thống sẽ chạy cả streaming và batch processing đồng thời.

---

## 📊 Kiểm tra kết quả

- **Kibana Dashboard:** http://localhost:5601
- **Elasticsearch:** http://localhost:9200
- **Batch files:** `data/batch_input/`
- **Processed Parquet:** `data/processed_batch/`

Đọc thêm chi tiết trong `BATCH_PROCESSING_GUIDE.md`
