# 🔄 Batch Processing Guide

## Tổng quan

Hệ thống đã được mở rộng để hỗ trợ **cả Streaming và Batch Processing** theo kiến trúc Lambda.

```
┌──────────────────────────────────────────────────────┐
│              DATA SOURCES (Scrapy Crawler)           │
└───────────────────┬──────────────────────────────────┘
                    │
        ┌───────────┴──────────┐
        │                      │
┌───────▼────────┐    ┌────────▼────────┐
│ SPEED LAYER    │    │  BATCH LAYER    │
│ (Streaming)    │    │  (Batch)        │
│                │    │                 │
│ • Kafka        │    │ • JSON/CSV      │
│ • Spark        │    │ • Spark Batch   │
│   Streaming    │    │ • Scheduler     │
│ • Real-time    │    │ • Historical    │
└───────┬────────┘    └────────┬────────┘
        │                      │
        └───────────┬──────────┘
                    ▼
        ┌──────────────────────┐
        │   SERVING LAYER      │
        │   • Elasticsearch    │
        │   • Kibana          │
        └──────────────────────┘
```

---

## 📁 File mới được tạo

### 1. **spark_batch.py** - Spark Batch Processing
- Xử lý dữ liệu từ file JSON/CSV/Parquet
- Hỗ trợ replay data từ Kafka
- Tạo aggregated statistics
- Lưu kết quả vào Elasticsearch + Parquet

### 2. **batch_scheduler.py** - Batch Job Scheduler
- Lên lịch chạy batch jobs tự động
- Mặc định: Daily (2 AM), Hourly stats, Weekly cleanup
- Sử dụng APScheduler

### 3. **export_to_batch.py** - Export Pipeline
- Scrapy pipeline để export data ra file
- Hỗ trợ JSON và CSV
- Batch size có thể cấu hình

### 4. **unified_pipeline.py** - Unified Manager
- Quản lý tất cả pipelines từ một nơi
- Interactive menu
- Health monitoring

---

## 🚀 Cách sử dụng

### **Bước 1: Cài đặt dependencies mới**

```bash
pip install -r requirements.txt
```

### **Bước 2: Tạo thư mục cho batch data**

```bash
mkdir -p data/batch_input
mkdir -p data/processed_batch
```

### **Bước 3: Cấu hình Scrapy để export batch files**

Mở `bds/batdongsan/settings.py` và thêm:

```python
# Batch export configuration
BATCH_OUTPUT_DIR = './data/batch_input'
BATCH_FILE_FORMAT = 'json'  # hoặc 'csv'
BATCH_SIZE = 1000  # số items mỗi file
```

Cập nhật ITEM_PIPELINES:

```python
ITEM_PIPELINES = {
    'batdongsan.pipelines.BatdongsanPipeline': 300,
    'batdongsan.pipelines.PushToKafka': 400,  # Streaming
    'export_to_batch.ExportToBatchFile': 500,  # Batch (thêm dòng này)
}
```

---

## 📋 Các mode hoạt động

### **Mode 1: Chỉ Streaming (như hiện tại)**

```bash
# Terminal 1: Start Spark Streaming
python spark_streaming.py

# Terminal 2: Start Crawler
cd bds
scrapy crawl bds_spider
```

### **Mode 2: Chỉ Batch Processing**

```bash
# Chạy batch ngay lập tức
python spark_batch.py

# Hoặc chạy theo lịch
python batch_scheduler.py
```

### **Mode 3: Hybrid - Cả Streaming và Batch**

```bash
# Sử dụng Unified Manager
python unified_pipeline.py

# Chọn option 4 trong menu: "Start Both (Hybrid)"
```

### **Mode 4: Sử dụng Unified Manager (Recommended)**

```bash
python unified_pipeline.py
```

Menu:
```
1. Start Streaming Pipeline (real-time)
2. Start Batch Processing (scheduled)
3. Run Immediate Batch Processing
4. Start Both (Hybrid)
5. Start Crawler Only
6. Check Health
7. Stop All
8. Exit
```

---

## 🎯 Use Cases

### **Use Case 1: Real-time + Historical**

**Kịch bản:** Bạn muốn vừa xử lý real-time vừa phân tích historical data

```bash
# 1. Start streaming pipeline
python spark_streaming.py

# 2. Start batch scheduler (background)
python batch_scheduler.py &

# 3. Start crawler
cd bds
scrapy crawl bds_spider
```

### **Use Case 2: Reprocess Historical Data**

**Kịch bản:** Bạn đã có data trong Kafka và muốn xử lý lại

```python
# Trong spark_batch.py, đổi SOURCE_TYPE:
SOURCE_TYPE = "kafka_replay"  # Replay từ Kafka
SOURCE_PATH = None  # Không cần path

# Chạy batch
python spark_batch.py
```

### **Use Case 3: Scheduled Daily Aggregation**

**Kịch bản:** Tính toán thống kê mỗi ngày lúc 2 AM

```bash
# Chỉnh batch_scheduler.py (đã config sẵn)
python batch_scheduler.py
```

### **Use Case 4: Export to Data Lake**

**Kịch bản:** Lưu data vào Parquet để phân tích sau

```python
# spark_batch.py tự động lưu vào Parquet
# Partition theo province để query nhanh hơn
write_to_parquet(processed_df, OUTPUT_PATH, partition_by=["province"])
```

---

## ⚙️ Configuration

### **Spark Batch Processing**

Sửa trong `spark_batch.py`:

```python
# Data source
SOURCE_TYPE = "json"  # Options: 'json', 'csv', 'parquet', 'kafka_replay'
SOURCE_PATH = "./data/batch_input/*.json"

# Output
ES_INDEX = "batdongsan"
OUTPUT_PATH = "./data/processed_batch"
```

### **Batch Scheduler**

Sửa trong `batch_scheduler.py`:

```python
# Daily batch at 2 AM
self.scheduler.add_job(
    self.daily_batch_processing,
    trigger=CronTrigger(hour=2, minute=0),
    ...
)

# Hourly stats update
self.scheduler.add_job(
    self.hourly_statistics_update,
    trigger=CronTrigger(minute=0),  # Every hour
    ...
)
```

---

## 📊 Batch Processing Features

### **1. Data Deduplication**
- Tự động loại bỏ duplicates dựa trên `link`

### **2. Data Validation**
- Lọc records không hợp lệ
- Kiểm tra required fields

### **3. Statistics Generation**
- Thống kê theo province
- Thống kê theo district
- Lưu vào index riêng: `batdongsan_stats_province`

### **4. Multiple Output Formats**
- Elasticsearch (searchable)
- Parquet (data lake, analytics)
- Partitioned by province (query optimization)

---

## 🔍 Monitoring

### **Check Batch Job Status**

```bash
# Check Elasticsearch indices
curl http://localhost:9200/_cat/indices?v

# Check Parquet files
ls -lh data/processed_batch/

# Check batch input files
ls -lh data/batch_input/
```

### **View Statistics**

```bash
# Province statistics
curl http://localhost:9200/batdongsan_stats_province/_search?pretty

# Or trong Kibana:
# http://localhost:5601
```

---

## 🛠️ Troubleshooting

### **Problem: Batch job không chạy**

```bash
# Check scheduler logs
python batch_scheduler.py

# Hoặc run manual
python spark_batch.py
```

### **Problem: File không được tạo**

```bash
# Check export pipeline config
cd bds
grep -A 5 "ITEM_PIPELINES" batdongsan/settings.py

# Check thư mục tồn tại
mkdir -p data/batch_input
```

### **Problem: Spark batch chậm**

```python
# Tăng parallelism trong spark_batch.py
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    ...
```

---

## 📈 So sánh Streaming vs Batch

| Feature | Streaming | Batch |
|---------|-----------|-------|
| **Latency** | < 1 second | Minutes to hours |
| **Data Source** | Kafka (real-time) | Files, Kafka replay |
| **Use Case** | Real-time monitoring | Historical analysis |
| **Resource** | Always running | Scheduled/on-demand |
| **Complexity** | Higher | Lower |
| **Aggregations** | Limited | Rich (SQL) |

---

## 🎓 Best Practices

1. **Streaming cho real-time:** Dùng cho dashboard, alerts
2. **Batch cho analytics:** Dùng cho reports, ML training
3. **Hybrid approach:** Combine both để có best of both worlds
4. **Data Lake:** Lưu Parquet để re-process sau này
5. **Partitioning:** Partition by date/province để query nhanh

---

## 📝 Next Steps

1. **Test batch processing:**
   ```bash
   python spark_batch.py
   ```

2. **Setup scheduler:**
   ```bash
   python batch_scheduler.py
   ```

3. **Try unified manager:**
   ```bash
   python unified_pipeline.py
   ```

4. **Monitor results in Kibana:**
   ```
   http://localhost:5601
   ```

---

## 🤝 Support

Nếu gặp vấn đề:
- Check logs trong terminal
- Verify Docker services: `docker-compose ps`
- Check Elasticsearch: `curl localhost:9200/_cluster/health`
