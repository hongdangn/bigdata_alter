# 🚀 HƯỚNG DẪN CHẠY TOÀN BỘ BATCH PIPELINE

## 📋 KIẾN TRÚC PIPELINE

```
┌─────────────┐    ┌─────────┐    ┌─────────┐    ┌──────────────┐
│   CRAWLER   │───►│  KAFKA  │───►│  MinIO  │───►│  ETL BATCH   │
│  (Scrapy)   │    │ (Queue) │    │(Bronze) │    │  (PySpark)   │
└─────────────┘    └─────────┘    └─────────┘    └──────┬───────┘
                                                          │
                                   ┌──────────────────────┼────────────┐
                                   ▼                      ▼            ▼
                              ┌─────────┐          ┌─────────┐  ┌─────────┐
                              │ BRONZE  │          │ SILVER  │  │  GOLD   │
                              │  (Raw)  │──────────►│(Cleaned)│──►│(Analytics)│
                              └─────────┘          └─────────┘  └─────────┘
```

---

## 🎯 BƯỚC 1: CHUẨN BỊ MÔI TRƯỜNG

### 1.1. Kiểm tra Docker Desktop
```powershell
# Mở Docker Desktop hoặc kiểm tra
Get-Process "Docker Desktop" -ErrorAction SilentlyContinue
```

### 1.2. Khởi động Docker Services
```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Start all containers
docker-compose up -d

# Đợi 30 giây để Kafka khởi động hoàn toàn
timeout /t 30

# Kiểm tra các container đang chạy
docker ps
```

**Kết quả mong đợi:**
```
CONTAINER ID   IMAGE                         STATUS         PORTS
xxx            confluentinc/cp-kafka         Up             9092-9093
xxx            confluentinc/cp-zookeeper     Up             2181
xxx            minio/minio                   Up             9000-9001
xxx            elasticsearch:8.18.8          Up             9200,9300
xxx            kibana:8.18.8                 Up             5601
```

### 1.3. Kiểm tra MinIO
```powershell
# Mở MinIO Console
start http://localhost:9001
# Login: minioadmin / minioadmin

# Kiểm tra bucket 'datalake' đã tồn tại chưa
# Nếu chưa, tạo bucket mới tên 'datalake'
```

### 1.4. Kiểm tra Kafka Topic
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Nếu chưa có topic 'batdongsan', tạo mới:
docker exec -it kafka kafka-topics --create \
  --topic batdongsan \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## 🕷️ BƯỚC 2: CRAWL DỮ LIỆU → KAFKA

### Option 1: Crawl Nhiều Tỉnh (Khuyến nghị)

**Terminal 1 (PowerShell):**
```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Crawl 5 tỉnh lớn, mỗi tỉnh max 500 trang, lặp mỗi 60 phút
python run_continuous_crawler.py \
  --provinces ha-noi ho-chi-minh da-nang hai-phong can-tho \
  --interval 60 \
  --max-page 500
```

### Option 2: Crawl Tuần Tự Các Tỉnh

**Terminal 1:**
```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Crawl 10 tỉnh, mỗi tỉnh 100 trang, chạy 1 lần
python run_multi_province.py --max-page 100 --continuous

# Hoặc chạy liên tục (lặp lại)
python run_multi_province.py --max-page 100 --continuous --interval 120
```

### Option 3: Crawl 1 Tỉnh Cụ Thể

**Terminal 1:**
```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter\bds

# Crawl chỉ Hà Nội
scrapy crawl bds_spider -a province=ha-noi -a max_page=999
```

**📊 Theo dõi log:**
- Bạn sẽ thấy: `Scraped from <200 https://...>`
- Mỗi listing được gửi vào Kafka topic `batdongsan`
- Log: `Sent 1 messages to Kafka`

**⚠️ Để terminal này chạy, không đóng!**

---

## 📦 BƯỚC 3: KAFKA → MinIO (BRONZE LAYER)

**Terminal 2 (PowerShell mới):**
```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Khởi động consumer lưu data vào MinIO
python kafka_to_minio.py
```

**📊 Theo dõi log:**
```
INFO - Connecting to MinIO: localhost:9000
INFO - Bucket 'datalake' exists
INFO - Successfully consumed 50 messages
INFO - Saved batch to: raw/province=ha_noi/year=2025/month=12/data_20251215_143022.parquet
INFO - Batch processing time: 2.34s
```

**Cấu trúc dữ liệu trong MinIO:**
```
datalake/
└── raw/                                    # ← BRONZE LAYER
    ├── province=ha_noi/
    │   └── year=2025/
    │       └── month=12/
    │           ├── data_20251215_100000.parquet
    │           ├── data_20251215_103000.parquet
    │           └── data_20251215_110000.parquet
    ├── province=ho_chi_minh/
    └── province=da_nang/
```

**⚠️ Để terminal này chạy song song với crawler!**

---

## ⏸️ BƯỚC 4: ĐỢI THU THẬP ĐỦ DỮ LIỆU

### Kiểm tra dữ liệu hiện có

**Option 1: MinIO Console**
```
1. Mở: http://localhost:9001
2. Login: minioadmin / minioadmin
3. Object Browser → datalake → raw
4. Xem số lượng file và kích thước
```

**Option 2: Python Script**
```bash
python monitor.py
```

### Ước lượng thời gian cần crawl

| Mục tiêu | Cấu hình | Thời gian ước tính |
|----------|----------|-------------------|
| **Test nhỏ** | 1 tỉnh, 10 trang | 5-10 phút |
| **Dataset vừa** | 3 tỉnh, 100 trang mỗi tỉnh | 1-2 giờ |
| **Dataset lớn** | 5 tỉnh, 500 trang, crawl liên tục | 5-7 ngày |

**Khuyến nghị:** Crawl ít nhất **2-3 giờ** để có đủ data demo ETL batch.

### Dừng crawling khi đủ data

```bash
# Dừng crawler (Terminal 1)
Ctrl + C

# Đợi kafka_to_minio.py xử lý hết buffer (Terminal 2)
# Khi thấy: "No new messages, waiting..."
# Có thể Ctrl + C để dừng
```

---

## ⚙️ BƯỚC 5: ETL BATCH (BRONZE → SILVER → GOLD)

### 5.1. Chạy Full ETL Pipeline (Lần đầu)

**Terminal 3 (PowerShell mới):**
```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Xử lý TOÀN BỘ dữ liệu trong Bronze → Silver → Gold
python etl_batch_job.py --mode full
```

**📊 Log mong đợi:**
```
INFO - Starting FULL ETL Pipeline...
INFO - ========================================
INFO - PHASE 1: Bronze -> Silver Transformation
INFO - ========================================
INFO - Reading raw data from: s3a://datalake/raw/
INFO - Total raw records: 12,450
INFO - After deduplication: 11,823 records
INFO - Saved to: s3a://datalake/silver/
INFO - 
INFO - ========================================
INFO - PHASE 2: Silver -> Gold Analytics
INFO - ========================================
INFO - Reading silver data from: s3a://datalake/silver/
INFO - Creating district aggregations...
INFO - Creating daily trends...
INFO - Saved to: s3a://datalake/gold/
INFO - 
INFO - ETL PIPELINE COMPLETED SUCCESSFULLY
INFO - Total Duration: 127.45 seconds
```

### 5.2. Chạy Incremental ETL (Hàng ngày)

```bash
# Chỉ xử lý data tháng 12/2025
python etl_batch_job.py --mode incremental --year 2025 --month 12
```

**Khi nào dùng Incremental?**
- ✅ Đã chạy Full ETL 1 lần
- ✅ Chỉ muốn xử lý data mới trong tháng hiện tại
- ✅ Chạy hàng ngày để update Gold layer

### 5.3. Kiểm tra kết quả trong MinIO

```
datalake/
├── raw/                      # BRONZE (không đổi)
│   └── province=.../...
│
├── silver/                   # SILVER (đã được làm sạch)
│   └── year=2025/
│       └── month=12/
│           └── part-00000-xxx.parquet
│
└── gold/                     # GOLD (aggregated)
    ├── district_aggregation/
    │   └── part-00000-xxx.parquet
    ├── daily_trends/
    │   └── year=2025/
    │       └── month=12/
    │           └── part-00000-xxx.parquet
    ├── province_summary/
    │   └── part-00000-xxx.parquet
    └── quality_metrics/
        └── part-00000-xxx.parquet
```

---

## 📊 BƯỚC 6: XEM & PHÂN TÍCH KẾT QUẢ

### 6.1. Xem Parquet files trong Python

**Tạo file `view_results.py`:**
```python
import pandas as pd
from minio import Minio

# Connect MinIO
client = Minio("localhost:9000",
               access_key="minioadmin",
               secret_key="minioadmin",
               secure=False)

# Download Silver data
objects = list(client.list_objects("datalake", prefix="silver/", recursive=True))
print(f"📄 Found {len(objects)} Silver files")

# Download file đầu tiên
obj = objects[0]
client.fget_object("datalake", obj.object_name, "temp_silver.parquet")

# Đọc và xem
df = pd.read_parquet("temp_silver.parquet")
print(f"\n✅ Silver Layer: {len(df)} records")
print(df.head(10))
print(df.columns.tolist())

# Download Gold - District Aggregation
client.fget_object("datalake", "gold/district_aggregation/part-00000-xxx.parquet", "temp_gold.parquet")
df_gold = pd.read_parquet("temp_gold.parquet")
print(f"\n✅ Gold Layer - District Agg: {len(df_gold)} records")
print(df_gold.head())
```

### 6.2. Query với PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryResults") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .getOrCreate()

# Đọc Silver
df_silver = spark.read.parquet("s3a://datalake/silver/")
df_silver.createOrReplaceTempView("listings")

# Query
spark.sql("""
    SELECT 
        province_clean,
        COUNT(*) as count,
        AVG(price_vnd) as avg_price,
        AVG(square_m2) as avg_square
    FROM listings
    WHERE price_vnd > 0 AND square_m2 > 0
    GROUP BY province_clean
    ORDER BY count DESC
""").show()
```

### 6.3. Export CSV để xem trong Excel

```python
import pandas as pd
from minio import Minio

client = Minio("localhost:9000", 
               access_key="minioadmin",
               secret_key="minioadmin",
               secure=False)

# Download Gold - District Aggregation
client.fget_object("datalake", 
                   "gold/district_aggregation/part-00000-xxx.parquet",
                   "district_agg.parquet")

df = pd.read_parquet("district_agg.parquet")
df.to_csv("district_aggregation.csv", index=False, encoding='utf-8-sig')
print("✅ Exported to: district_aggregation.csv")
```

---

## 🔄 BƯỚC 7: TỰ ĐỘNG HÓA (SCHEDULING)

### Option 1: Chạy tự động hàng ngày

```bash
cd D:\HUST\20251\IT4931_bigdata\bigdata_alter

# Schedule: Mỗi ngày 2:00 AM chạy incremental ETL
python etl_scheduler.py --daily --daily-time 02:00
```

### Option 2: Task Scheduler Windows

1. Mở **Task Scheduler** (Lập lịch tác vụ)
2. Create Basic Task:
   - Name: `ETL Batch Daily`
   - Trigger: Daily at 2:00 AM
   - Action: Start a program
   - Program: `python`
   - Arguments: `etl_batch_job.py --mode incremental`
   - Start in: `D:\HUST\20251\IT4931_bigdata\bigdata_alter`

---

## 🛠️ TROUBLESHOOTING

### ❌ Lỗi: "Connection refused" khi crawl
```bash
# Kiểm tra Kafka
docker ps | grep kafka
docker logs kafka

# Restart Kafka
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
# Kiểm tra data trong MinIO
# Đảm bảo có file .parquet trong raw/
```

### ❌ PySpark lỗi Java heap space
```bash
# Tăng memory cho Spark
export PYSPARK_DRIVER_MEMORY=4g
export PYSPARK_EXECUTOR_MEMORY=4g
```

---

## 📈 KẾT QUẢ MONG ĐỢI

Sau khi chạy xong toàn bộ pipeline, bạn sẽ có:

### Bronze Layer (Raw Data)
- ✅ File Parquet được partition theo: `province/year/month`
- ✅ Dữ liệu thô từ Kafka, chưa xử lý
- ✅ Kích thước: ~100 KB - 5 MB/file tùy batch size

### Silver Layer (Cleaned Data)
- ✅ Đã parse giá, diện tích, ngày đăng
- ✅ Đã deduplicate (loại trùng lặp)
- ✅ Đã chuẩn hóa text và địa chỉ
- ✅ Có thêm các trường: `price_vnd`, `square_m2`, `price_per_m2`, `quality_score`

### Gold Layer (Analytics)
- ✅ `district_aggregation/`: Giá trung bình theo quận/huyện
- ✅ `daily_trends/`: Số lượng tin đăng theo ngày
- ✅ `province_summary/`: Tổng hợp theo tỉnh
- ✅ `quality_metrics/`: Đánh giá chất lượng dữ liệu

---

## 🎯 QUICK START (TÓM TẮT)

```bash
# 1. Start Docker
docker-compose up -d
timeout /t 30

# 2. Start Crawler (Terminal 1)
python run_continuous_crawler.py --provinces ha-noi ho-chi-minh --interval 60 --max-page 100

# 3. Start Kafka->MinIO (Terminal 2)
python kafka_to_minio.py

# 4. Đợi 1-2 giờ, sau đó Ctrl+C cả 2 terminal

# 5. Run ETL Batch (Terminal 3)
python etl_batch_job.py --mode full

# 6. View results
python view_results.py
```

---

## 📞 SUPPORT

Nếu gặp lỗi, kiểm tra:
1. `docker ps` - Tất cả containers đang chạy
2. `docker logs kafka` - Kafka logs
3. `docker logs minio` - MinIO logs
4. `monitor.py` - Số lượng records đã crawl

**Good luck! 🚀**
