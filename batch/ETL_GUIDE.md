# ETL Batch Job - Hướng dẫn sử dụng

## Kiến trúc Medallion (Bronze -> Silver -> Gold)

```
┌──────────────┐
│   Kafka      │
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│  BRONZE (Raw Layer)                          │
│  - Format: Parquet                           │
│  - Location: s3a://datalake/raw/             │
│  - Partitioned by: province/year/month       │
│  - Data: Raw from Kafka (unprocessed)        │
└──────┬───────────────────────────────────────┘
       │ ETL Phase 1: bronze_to_silver()
       │ - Parse Vietnamese text (giá, diện tích)
       │ - Standardize dates
       │ - Deduplicate records
       │ - Add quality flags
       ▼
┌──────────────────────────────────────────────┐
│  SILVER (Cleaned Layer)                      │
│  - Format: Parquet                           │
│  - Location: s3a://datalake/silver/          │
│  - Partitioned by: year/month                │
│  - Data: Cleaned, standardized, deduplicated │
└──────┬───────────────────────────────────────┘
       │ ETL Phase 2: silver_to_gold()
       │ - Aggregate by district
       │ - Calculate metrics
       │ - Generate business analytics
       ▼
┌──────────────────────────────────────────────┐
│  GOLD (Analytics Layer)                      │
│  - Format: Parquet                           │
│  - Location: s3a://datalake/gold/            │
│  - Datasets:                                 │
│    • district_aggregation/                   │
│    • daily_trends/                           │
│    • province_summary/                       │
│    • quality_metrics/                        │
└──────────────────────────────────────────────┘
```

## Cài đặt Dependencies

```bash
pip install pyspark
```

## Chạy ETL Job

### 1. Chạy toàn bộ pipeline (Bronze -> Silver -> Gold)

```bash
python etl_batch_job.py
```

### 2. Chạy từng phase riêng lẻ

```bash
# Chỉ chạy Bronze -> Silver
python etl_batch_job.py --phase bronze-silver

# Chỉ chạy Silver -> Gold
python etl_batch_job.py --phase silver-gold
```

### 3. Với cấu hình tùy chỉnh

```bash
python etl_batch_job.py \
    --minio-endpoint localhost:9000 \
    --bucket datalake \
    --phase full
```

## Transformations Chi Tiết

### Phase 1: Bronze -> Silver

#### 1. **Parse Giá (Price)**

```
Input:  "5.5 tỷ", "500 triệu", "2.5 tỷ"
Output: 5500000000.0, 500000000.0, 2500000000.0
```

**Logic:**
- Nhận diện đơn vị: tỷ (billion), triệu (million), nghìn (thousand)
- Chuyển đổi sang VND (Double)
- Xử lý format Việt Nam: "5,5 tỷ", "5.5 ty"

#### 2. **Parse Diện Tích (Square)**

```
Input:  "40 m2", "120m2", "85.5 m²"
Output: 40.0, 120.0, 85.5
```

**Logic:**
- Extract số từ chuỗi
- Loại bỏ đơn vị (m2, m², mét vuông)
- Trả về Float

#### 3. **Parse Ngày Đăng (Post Date)**

```
Input:  "Hôm nay", "Hôm qua", "2 ngày trước", "10/12/2025"
Output: "2025-12-12", "2025-12-11", "2025-12-10", "2025-12-10"
```

**Logic:**
- Xử lý ngày tương đối (relative dates)
- Parse nhiều format: DD/MM/YYYY, YYYY-MM-DD
- Chuẩn hóa về YYYY-MM-DD

#### 4. **Deduplication**

**Strategy:** MD5 hash của (link + title + province)

```python
dedup_key = md5(concat("link", "title_clean", "province_clean"))
```

**Logic:**
- Partition by dedup_key
- Sắp xếp theo `_ingestion_timestamp` DESC
- Giữ lại record mới nhất (row_number = 1)

#### 5. **Quality Scoring**

```python
quality_score = has_price + has_square + has_location
```

**Scoring:**
- 3: Có đầy đủ giá, diện tích, địa chỉ (chất lượng cao)
- 2: Thiếu 1 trường
- 1: Thiếu 2 trường
- 0: Thiếu cả 3 (chất lượng thấp)

### Phase 2: Silver -> Gold

#### 1. **District Aggregation**

```sql
SELECT 
    province_clean,
    district_clean,
    COUNT(*) as listing_count,
    AVG(price_vnd) as avg_price_vnd,
    AVG(square_m2) as avg_square_m2,
    AVG(price_per_m2) as avg_price_per_m2
FROM silver
WHERE price_vnd IS NOT NULL AND square_m2 IS NOT NULL
GROUP BY province_clean, district_clean
```

**Output:** `gold/district_aggregation/`

#### 2. **Daily Trends**

```sql
SELECT 
    province_clean,
    YEAR(post_date_parsed) as year,
    MONTH(post_date_parsed) as month,
    DAY(post_date_parsed) as day,
    COUNT(*) as daily_post_count
FROM silver
WHERE post_date_parsed IS NOT NULL
GROUP BY province_clean, year, month, day
```

**Output:** `gold/daily_trends/` (partitioned by year, month)

#### 3. **Province Summary**

```sql
SELECT 
    province_clean,
    COUNT(*) as total_listings,
    AVG(price_vnd) as avg_price,
    AVG(square_m2) as avg_square,
    SUM(has_price) as listings_with_price,
    SUM(has_square) as listings_with_square
FROM silver
GROUP BY province_clean
```

**Output:** `gold/province_summary/`

#### 4. **Quality Metrics**

```sql
SELECT 
    quality_score,
    COUNT(*) as record_count
FROM silver
GROUP BY quality_score
```

**Output:** `gold/quality_metrics/`

## Đọc dữ liệu từ Gold Layer

### Sử dụng PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AnalyticsQuery") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read district aggregation
df_district = spark.read.parquet("s3a://datalake/gold/district_aggregation/")
df_district.show()

# Top 10 quận có giá cao nhất
df_district \
    .orderBy(col("avg_price_vnd").desc()) \
    .show(10)

# Read daily trends
df_trends = spark.read.parquet("s3a://datalake/gold/daily_trends/")

# Xu hướng đăng bài theo ngày
df_trends \
    .filter("province_clean = 'Hà Nội' AND year = 2025 AND month = 12") \
    .orderBy("day") \
    .show()
```

### Sử dụng Pandas

```python
import pandas as pd
from minio import Minio
import pyarrow.parquet as pq
import io

client = Minio("localhost:9000", "minioadmin", "minioadmin", secure=False)

# List parquet files
objects = client.list_objects("datalake", prefix="gold/district_aggregation/", recursive=True)

dfs = []
for obj in objects:
    if obj.object_name.endswith('.parquet'):
        response = client.get_object("datalake", obj.object_name)
        parquet_data = response.read()
        df = pd.read_parquet(io.BytesIO(parquet_data))
        dfs.append(df)

df_combined = pd.concat(dfs, ignore_index=True)
print(df_combined.head())

# Top 10 quận
top_districts = df_combined.nlargest(10, 'avg_price_vnd')
print(top_districts)
```

## Tích hợp với Elasticsearch (Optional)

Thêm code này vào `silver_to_gold()`:

```python
# Write to Elasticsearch
df_district_agg.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "real_estate_district_agg") \
    .mode("overwrite") \
    .save()
```

## Schedule ETL Job

### Cron (Linux/Mac)

```bash
# Chạy mỗi ngày lúc 2 giờ sáng
0 2 * * * cd /path/to/project && python etl_batch_job.py >> etl.log 2>&1
```

### Task Scheduler (Windows)

```powershell
# Tạo scheduled task
schtasks /create /tn "RealEstateETL" /tr "python D:\path\to\etl_batch_job.py" /sc daily /st 02:00
```

### Airflow DAG (Production)

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'real_estate_etl',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2025, 12, 1),
    catchup=False
)

run_etl = BashOperator(
    task_id='run_etl_batch',
    bash_command='cd /path/to/project && python etl_batch_job.py',
    dag=dag
)
```

## Monitoring & Logs

ETL job tự động log các metrics:

```
2025-12-12 10:00:00 - __main__ - INFO - Starting Bronze -> Silver transformation
2025-12-12 10:00:05 - __main__ - INFO - Loaded 1500 records from Bronze layer
2025-12-12 10:00:10 - __main__ - INFO - After deduplication: 1350 records (removed 150 duplicates)
2025-12-12 10:00:15 - __main__ - INFO - ✓ Bronze -> Silver completed successfully
2025-12-12 10:00:20 - __main__ - INFO - Starting Silver -> Gold transformation
2025-12-12 10:00:30 - __main__ - INFO - ✓ Silver -> Gold completed successfully
2025-12-12 10:00:30 - __main__ - INFO - Total Duration: 30.45 seconds
```

## Troubleshooting

### Lỗi kết nối MinIO

```
Error: Connection refused to localhost:9000
```

**Fix:**
```bash
docker ps  # Kiểm tra MinIO đang chạy
docker-compose up -d minio
```

### Lỗi Hadoop AWS JAR

```
ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem
```

**Fix:** Spark packages đã được config tự động. Nếu vẫn lỗi:
```bash
spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    etl_batch_job.py
```

### Performance tuning

```python
# Tăng parallelism
spark.conf.set("spark.default.parallelism", "200")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Memory tuning
# --driver-memory 4g --executor-memory 4g
```
