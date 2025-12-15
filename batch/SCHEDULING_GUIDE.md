# ETL Scheduling Guide - Cập nhật tự động theo lịch

## Tổng quan

Hệ thống hỗ trợ **2 chế độ ETL**:

### 1. **Full Refresh** (Xử lý lại toàn bộ)
- Xử lý lại tất cả dữ liệu từ đầu
- Overwrite Silver và Gold layers
- **Khi nào dùng:** Khi cần rebuild toàn bộ, sửa logic ETL, hoặc định kỳ hàng tuần/tháng

### 2. **Incremental** (Xử lý tăng dần)
- Chỉ xử lý partition mới (year/month)
- Append vào Silver, re-aggregate Gold
- **Khi nào dùng:** Xử lý hàng ngày cho dữ liệu mới

## Cấu trúc cập nhật

```
┌─────────────┐
│  Raw Data   │ ← Kafka consumer ghi liên tục
│  (Bronze)   │   partition theo province/year/month
└──────┬──────┘
       │
       ▼ ETL chạy theo schedule
┌──────────────┐
│  Silver      │ ← Daily: Incremental (append partition mới)
│  (Cleaned)   │   Weekly: Full refresh (overwrite all)
└──────┬───────┘
       │
       ▼ Re-aggregate
┌──────────────┐
│  Gold        │ ← Mỗi lần Silver update → Gold refresh
│  (Analytics) │
└──────────────┘
```

## Kịch bản sử dụng

### ✅ **Kịch bản 1: Cập nhật hàng ngày**

**Mục tiêu:** Xử lý dữ liệu mới crawl được mỗi ngày

**Lịch trình:**
- **02:00 AM mỗi ngày:** Chạy incremental ETL cho tháng hiện tại

**Câu lệnh:**
```bash
# Chạy incremental cho tháng hiện tại (auto-detect)
python etl_batch_job.py --mode incremental

# Hoặc chỉ định rõ year/month
python etl_batch_job.py --mode incremental --year 2025 --month 12
```

**Kết quả:**
- Silver: Append records mới từ partition year=2025/month=12
- Gold: Re-aggregate toàn bộ dữ liệu (bao gồm cả cũ + mới)

### ✅ **Kịch bản 2: Full refresh hàng tuần**

**Mục tiêu:** Đảm bảo data consistency, sửa lỗi dedup

**Lịch trình:**
- **03:00 AM Chủ nhật hàng tuần:** Full refresh toàn bộ

**Câu lệnh:**
```bash
python etl_batch_job.py --mode full
```

**Kết quả:**
- Xử lý lại toàn bộ Bronze → Silver → Gold
- Deduplicate globally
- Overwrite tất cả

### ✅ **Kịch bản 3: Xử lý lại tháng cũ**

**Mục tiêu:** Backfill dữ liệu tháng trước bị thiếu

**Câu lệnh:**
```bash
# Xử lý tháng 11/2025
python etl_batch_job.py --mode incremental --year 2025 --month 11

# Xử lý tháng 10/2025
python etl_batch_job.py --mode incremental --year 2025 --month 10
```

## Automated Scheduling

### **Option 1: Python Scheduler (Khuyến nghị cho development)**

```bash
# Cài đặt dependency
pip install schedule

# Chạy scheduler với config mặc định
python etl_scheduler.py

# Config mặc định:
# - Daily incremental: 02:00 AM
# - Weekly full refresh: 03:00 AM Sunday
```

**Custom schedule:**
```bash
python etl_scheduler.py \
    --daily \
    --daily-time 03:30 \
    --weekly \
    --weekly-day saturday \
    --weekly-time 04:00
```

**Test ngay:**
```bash
python etl_scheduler.py --test
```

### **Option 2: Cron (Linux/Mac)**

```bash
# Edit crontab
crontab -e

# Thêm các dòng sau:

# Daily incremental at 2 AM
0 2 * * * cd /path/to/project && python etl_batch_job.py --mode incremental >> etl.log 2>&1

# Weekly full refresh at 3 AM every Sunday
0 3 * * 0 cd /path/to/project && python etl_batch_job.py --mode full >> etl.log 2>&1

# Monthly backfill at 4 AM on 1st day of month
0 4 1 * * cd /path/to/project && python etl_batch_job.py --mode incremental --year $(date +\%Y) --month $(($(date +\%m)-1)) >> etl.log 2>&1
```

### **Option 3: Windows Task Scheduler**

**PowerShell script (etl_daily.ps1):**
```powershell
cd D:\path\to\project
.\bigdata\Scripts\Activate.ps1
python etl_batch_job.py --mode incremental
```

**Tạo Task:**
```powershell
# Daily incremental
schtasks /create /tn "ETL_Daily_Incremental" `
    /tr "powershell -File D:\path\to\etl_daily.ps1" `
    /sc daily /st 02:00

# Weekly full refresh
schtasks /create /tn "ETL_Weekly_Full" `
    /tr "powershell -File D:\path\to\etl_full.ps1" `
    /sc weekly /d SUN /st 03:00
```

### **Option 4: Apache Airflow (Production)**

**DAG file (dags/real_estate_etl_dag.py):**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alert@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'real_estate_etl',
    default_args=default_args,
    description='Real Estate Data ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['etl', 'real-estate'],
)

# Daily incremental job
incremental_etl = BashOperator(
    task_id='run_incremental_etl',
    bash_command='cd /opt/airflow/dags/etl && python etl_batch_job.py --mode incremental',
    dag=dag,
)

# Weekly full refresh (runs on Sunday)
dag_weekly = DAG(
    'real_estate_etl_weekly',
    default_args=default_args,
    schedule_interval='0 3 * * 0',  # Sunday at 3 AM
    start_date=datetime(2025, 12, 1),
    catchup=False,
)

full_refresh = BashOperator(
    task_id='run_full_refresh',
    bash_command='cd /opt/airflow/dags/etl && python etl_batch_job.py --mode full',
    dag=dag_weekly,
)
```

## Workflow chi tiết

### Timeline hàng ngày

```
00:00 - 01:59  Crawler chạy, đưa data vào Kafka
              ↓
              kafka_to_minio.py consume → Bronze layer
              
02:00         ETL Incremental bắt đầu
              ↓
02:00 - 02:10 Bronze → Silver (partition tháng hiện tại)
              - Parse giá, diện tích, ngày
              - Deduplicate with existing Silver
              - Append to Silver
              
02:10 - 02:15 Silver → Gold
              - Re-aggregate ALL Silver data
              - District aggregation
              - Daily trends
              - Province summary
              - Quality metrics
              
02:15         ETL hoàn thành
```

### Timeline hàng tuần (Chủ nhật)

```
03:00         ETL Full Refresh bắt đầu
              ↓
03:00 - 03:30 Bronze → Silver (ALL partitions)
              - Process toàn bộ Bronze data
              - Deduplicate globally
              - Overwrite Silver
              
03:30 - 03:45 Silver → Gold
              - Re-aggregate ALL data
              - Overwrite Gold
              
03:45         Full refresh hoàn thành
```

## Monitoring & Logs

### Log files
```bash
# Xem ETL logs
tail -f etl.log

# Xem scheduler logs
tail -f etl_scheduler.log

# Xem Spark logs
tail -f spark_logs/etl_*.log
```

### Check data freshness

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Check last processing time in Silver
df = spark.read.parquet("s3a://datalake/silver/")
df.select("silver_processing_timestamp").orderBy(col("silver_processing_timestamp").desc()).show(5)

# Count by partition
df.groupBy("_partition_year", "_partition_month").count().orderBy("_partition_year", "_partition_month").show()
```

### Metrics to monitor

```python
# District aggregation freshness
df_gold = spark.read.parquet("s3a://datalake/gold/district_aggregation/")
print(f"Total districts: {df_gold.count()}")

# Daily trends completeness
df_trends = spark.read.parquet("s3a://datalake/gold/daily_trends/")
df_trends.groupBy("year", "month").count().show()
```

## Best Practices

### ✅ DO

1. **Chạy daily incremental** để xử lý dữ liệu mới nhanh
2. **Chạy weekly full refresh** để đảm bảo consistency
3. **Monitor logs** để phát hiện lỗi sớm
4. **Backup Gold layer** trước khi full refresh
5. **Test ETL job** trước khi schedule

### ❌ DON'T

1. **Không chạy nhiều job cùng lúc** (conflict write)
2. **Không skip weekly refresh** (data drift)
3. **Không ignore failed jobs** (data gap)
4. **Không hardcode paths** (dùng parameters)

## Troubleshooting

### Lỗi: Duplicate records trong Silver

**Nguyên nhân:** Incremental mode không merge đúng

**Fix:**
```bash
# Chạy full refresh để deduplicate
python etl_batch_job.py --mode full
```

### Lỗi: Gold metrics không cập nhật

**Nguyên nhân:** Silver không có data mới

**Check:**
```python
# Kiểm tra Silver có data không
df = spark.read.parquet("s3a://datalake/silver/")
df.filter("_partition_year=2025 AND _partition_month=12").count()
```

### Lỗi: Out of memory

**Nguyên nhân:** Quá nhiều data trong 1 partition

**Fix:**
```bash
# Tăng memory cho Spark
export PYSPARK_DRIVER_MEMORY=4g
export PYSPARK_EXECUTOR_MEMORY=4g
python etl_batch_job.py --mode incremental
```

## Summary

| Schedule | Mode | Frequency | Purpose |
|----------|------|-----------|---------|
| Daily 2 AM | Incremental | Mỗi ngày | Xử lý data mới |
| Weekly 3 AM Sunday | Full | Hàng tuần | Đảm bảo consistency |
| Monthly 4 AM Day 1 | Incremental | Hàng tháng | Backfill tháng trước |

**Recommendation cho production:**
- Daily: Incremental
- Weekly: Full refresh  
- Use Airflow for orchestration
- Monitor with Prometheus + Grafana
