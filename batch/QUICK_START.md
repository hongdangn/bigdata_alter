# 🚀 QUICK START - Batch Pipeline

## 📋 Yêu cầu trước khi chạy
- Docker Desktop đã cài đặt và đang chạy
- Python 3.8+ đã cài đặt
- Đã cài đặt dependencies: `pip install -r requirements.txt`

---

## 🎯 CÁCH CHẠY PIPELINE

### **Bước 1: Khởi động Docker Services**

Mở **Terminal/PowerShell**, chạy:

```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Khởi động tất cả containers (Kafka, MinIO, Elasticsearch, Kibana)
docker-compose up -d

# Đợi 30 giây để Kafka khởi động hoàn toàn
timeout /t 30

# Kiểm tra containers đang chạy
docker ps
```

**Kết quả mong đợi:**
```
CONTAINER ID   IMAGE                         STATUS    PORTS
xxx            confluentinc/cp-kafka         Up        9092-9093
xxx            confluentinc/cp-zookeeper     Up        2181
xxx            minio/minio                   Up        9000-9001
xxx            elasticsearch:8.18.8          Up        9200,9300
xxx            kibana:8.18.8                 Up        5601
```

---

### **Bước 2: Bật Crawler (Terminal 1)**

Mở **Terminal 1**, chạy:

```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Crawl nhiều tỉnh, lặp mỗi 60 phút, tối đa 500 trang/tỉnh
python run_continuous_crawler.py --provinces ha-noi ho-chi-minh da-nang --interval 60 --max-page 500
```

**Tùy chỉnh tham số:**
```bash
# Test nhanh (5 trang, 1 tỉnh)
python run_continuous_crawler.py --provinces ha-noi --interval 999 --max-page 5

# Production (nhiều tỉnh, crawl liên tục)
python run_continuous_crawler.py \
    --provinces ha-noi ho-chi-minh da-nang hai-phong can-tho \
    --interval 60 \
    --max-page 999
```

**Log mong đợi:**
```
INFO - Starting crawler for province: ha-noi
INFO - Scraped from <200 https://...>
INFO - Sent 1 messages to Kafka topic: batdongsan
```

⚠️ **Để terminal này chạy, không đóng!**

---

### **Bước 3: Bật Kafka→MinIO Consumer (Terminal 2)**

Mở **Terminal 2**, chạy:

```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Lấy data từ Kafka và lưu vào MinIO (Data Lake)
python kafka_to_minio.py
```

**Log mong đợi:**
```
INFO - Connecting to MinIO: localhost:9000
INFO - Bucket 'datalake' exists
INFO - Successfully consumed 50 messages
INFO - Saved batch to: raw/province=ha_noi/year=2025/month=12/data_20251215_143022.parquet
INFO - Batch processing time: 2.34s
```

⚠️ **Để terminal này chạy song song với crawler!**

---

### **Bước 4: Đợi Thu Thập Dữ Liệu**

**Xem tiến độ trong MinIO Console:**
1. Mở browser: http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. Vào: **Object Browser** → **datalake** → **raw/**
4. Chọn tỉnh → năm → tháng
5. Xem số lượng file `.parquet` và kích thước

**Thời gian khuyến nghị:**
- **Test nhanh:** 5-10 phút (50-100 records)
- **Dataset vừa:** 1-2 giờ (1,000-5,000 records)
- **Dataset lớn:** 5-7 ngày (100,000+ records)

**Khi nào dừng:**
- Khi đã có đủ data để test ETL
- Terminal 1 đã crawl xong các trang
- MinIO đã có nhiều file parquet

---

### **Bước 5: Dừng Crawler**

Khi đã có đủ data:

```bash
# Terminal 1 & 2: Nhấn Ctrl+C để dừng
# Đợi kafka_to_minio.py xử lý hết buffer trong Kafka
# Khi thấy: "No new messages, waiting..." → Ctrl+C
```

---

### **Bước 6: Chạy ETL Batch Processing**

Mở **Terminal 3**, chạy:

```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Option 1: Full ETL (xử lý toàn bộ data)
python etl_batch_job.py --mode full

# Option 2: Incremental ETL (chỉ xử lý tháng hiện tại)
python etl_batch_job.py --mode incremental --year 2025 --month 12
```

**Log mong đợi:**
```
INFO - ========================================
INFO - STARTING FULL ETL PIPELINE
INFO - ========================================
INFO - [PHASE 1] Bronze -> Silver (Full Refresh)
INFO - Loaded 12,450 records from Bronze layer
INFO - After deduplication: 11,823 records
INFO - ✓ Bronze -> Silver completed successfully
INFO - [PHASE 2] Silver -> Gold
INFO - Loaded 11,823 records from Silver layer
INFO - ✓ Silver -> Gold completed successfully
INFO - ========================================
INFO - ETL PIPELINE COMPLETED SUCCESSFULLY
INFO - Total Duration: 127.45 seconds
INFO - ========================================
```

**Kết quả trong MinIO:**
```
datalake/
├── raw/                    # Bronze (không đổi)
├── silver/                 # Cleaned data
│   └── year=2025/month=12/
└── gold/                   # Analytics
    ├── district_aggregation/
    ├── daily_trends/
    ├── province_summary/
    └── quality_metrics/
```

---

### **Bước 7: Xem Kết Quả**

```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Xem kết quả và export CSV
python view_results.py
```

**Output:**
- 📊 Số lượng records trong Bronze, Silver, Gold
- 📈 Statistics (giá trung bình, diện tích)
- 🏆 Top districts by price
- 🌍 Province summary
- 💾 Export CSV option

---

## � KIỂM TRA KẾT QUẢ

### **1. MinIO Console (Web UI)**
```
URL: http://localhost:9001
Login: minioadmin / minioadmin

Cấu trúc data:
datalake/
├── raw/                    # Bronze Layer
│   └── province=ha_noi/...
├── silver/                 # Silver Layer  
│   └── year=2025/month=12/
└── gold/                   # Gold Layer
    ├── district_aggregation/
    ├── daily_trends/
    ├── province_summary/
    └── quality_metrics/
```

### **2. Python Script**
```bash
# Xem data nhanh
python view_results.py

# Hoặc xem trực tiếp với pandas
python -c "
import pandas as pd
from minio import Minio
client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
# Download và xem Silver data
objects = list(client.list_objects('datalake', prefix='silver/', recursive=True))
if objects:
    client.fget_object('datalake', objects[0].object_name, 'temp.parquet')
    df = pd.read_parquet('temp.parquet')
    print(f'Records: {len(df)}')
    print(df.head())
"
```

---

## ⚡ QUICK TEST (5-10 phút)

Để test nhanh pipeline trước khi chạy production:

```bash
# Terminal 1: Start Docker
docker-compose up -d && timeout /t 30

# Terminal 2: Quick crawl (5 trang)
cd bds
scrapy crawl bds_spider -a province=ha-noi -a max_page=5

# Terminal 3: Kafka→MinIO
python kafka_to_minio.py
# Đợi thấy "Saved batch to..." rồi Ctrl+C

# Terminal 4: Run ETL
python etl_batch_job.py --mode full

# View results
python view_results.py
```

---

## 🛑 DỪNG TOÀN BỘ

```bash
# Dừng Docker containers
docker-compose down

# Hoặc chỉ stop (không xóa volumes)
docker-compose stop
```

---

## 🔧 TROUBLESHOOTING

### ❌ Lỗi: "Connection refused" khi crawler
```bash
# Kiểm tra Kafka
docker ps | grep kafka
docker logs kafka

# Restart
docker-compose restart kafka zookeeper
```

### ❌ Lỗi: MinIO "NoSuchBucket"
```bash
# Tạo bucket
docker exec -it minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec -it minio mc mb local/datalake
```

### ❌ ETL lỗi: "No such file or directory"
```bash
# Kiểm tra có file trong raw/
python -c "
from minio import Minio
client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
files = list(client.list_objects('datalake', prefix='raw/', recursive=True))
print(f'Found {len(files)} files')
"
```

### ❌ Import error: "No module named 'pyspark'"
```bash
# Cài dependencies
pip install -r requirements.txt

# Hoặc cài riêng PySpark
pip install pyspark==3.4.3
```

---

## 📚 TÀI LIỆU CHI TIẾT

- **[RUN_PIPELINE.md](RUN_PIPELINE.md)** - Hướng dẫn đầy đủ từng bước
- **[FILE_STRUCTURE.md](FILE_STRUCTURE.md)** - Giải thích tất cả các file
- **[ETL_GUIDE.md](ETL_GUIDE.md)** - Kiến trúc Medallion (Bronze/Silver/Gold)

---

## ✅ CHECKLIST

```bash
# Pre-flight check
[ ] Docker Desktop đang chạy
[ ] Python 3.8+ đã cài
[ ] pip install -r requirements.txt thành công

# Pipeline execution
[ ] docker-compose up -d → 5 containers running
[ ] Terminal 1: python run_continuous_crawler.py
[ ] Terminal 2: python kafka_to_minio.py
[ ] MinIO Console có files trong raw/
[ ] Ctrl+C cả 2 terminals khi đủ data
[ ] python etl_batch_job.py --mode full
[ ] MinIO Console có files trong silver/ và gold/
[ ] python view_results.py thành công

# Results
[ ] CSV files exported
[ ] Statistics hiển thị đúng
[ ] District aggregation có data
```

🎉 **HOÀN TẤT!**
