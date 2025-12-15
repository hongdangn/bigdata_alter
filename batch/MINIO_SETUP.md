# Kafka to MinIO Data Lake Setup

Hệ thống này tự động lưu trữ dữ liệu bất động sản từ Kafka vào MinIO data lake với cấu trúc phân vùng.

## Cấu trúc Data Lake

Dữ liệu được lưu theo cấu trúc phân vùng (partitioned):

```
datalake/
└── raw/
    ├── province=ha_noi/
    │   ├── year=2025/
    │   │   ├── month=01/
    │   │   │   ├── data_20250115_120000_123456.json
    │   │   │   └── data_20250115_130000_234567.json
    │   │   ├── month=02/
    │   │   └── month=12/
    │   └── year=2024/
    ├── province=ho_chi_minh/
    │   └── year=2025/
    │       └── month=12/
    └── province=da_nang/
        └── year=2025/
            └── month=12/
```

## Chuẩn hóa tên tỉnh

Tên tỉnh được chuẩn hóa theo quy tắc:
- Loại bỏ dấu tiếng Việt
- Chuyển về chữ thường
- Thay khoảng trắng và dấu gạch ngang bằng dấu gạch dưới

Ví dụ:
- "Hà Nội" → "ha_noi"
- "Hồ Chí Minh" → "ho_chi_minh"
- "Đà Nẵng" → "da_nang"
- "Bà Rịa - Vũng Tàu" → "ba_ria_vung_tau"

## Khởi động hệ thống

### 1. Khởi động Docker containers

```bash
docker-compose up -d
```

Dịch vụ bao gồm:
- **Zookeeper** (port 2181)
- **Kafka** (port 9092, 9093)
- **Elasticsearch** (port 9200, 9300)
- **Kibana** (port 5601)
- **MinIO** (port 9000 - API, port 9001 - Console)

### 2. Truy cập MinIO Console

Mở trình duyệt: http://localhost:9001

Đăng nhập:
- Username: `minioadmin`
- Password: `minioadmin`

### 3. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 4. Chạy crawler để gửi dữ liệu vào Kafka

```bash
cd bds
scrapy crawl bds_spider
```

### 5. Chạy consumer để lưu dữ liệu từ Kafka vào MinIO

```bash
python kafka_to_minio.py
```

## Cấu hình

### kafka_to_minio.py

Các tham số có thể điều chỉnh trong hàm `main()`:

```python
config = {
    'kafka_bootstrap_servers': 'localhost:9092',  # Kafka server
    'kafka_topic': 'batdongsan',                   # Kafka topic
    'kafka_group_id': 'minio-consumer-group',      # Consumer group
    'minio_endpoint': 'localhost:9000',            # MinIO endpoint
    'minio_access_key': 'minioadmin',              # MinIO credentials
    'minio_secret_key': 'minioadmin',
    'bucket_name': 'datalake',                     # MinIO bucket name
    'batch_size': 50,                              # Records per batch
    'batch_timeout': 30                            # Seconds before flush
}
```

## Định dạng dữ liệu

Dữ liệu được lưu ở định dạng **Parquet** (columnar format) với compression Snappy:

**Ưu điểm của Parquet:**
- Nén tốt hơn JSON (tiết kiệm 50-80% dung lượng)
- Đọc nhanh hơn với columnar storage
- Hỗ trợ schema evolution
- Tích hợp tốt với Spark, Pandas, Dask
- Hỗ trợ predicate pushdown

## Đọc dữ liệu từ MinIO

### Sử dụng MinIO Client (Python)

```python
from minio import Minio
import pandas as pd
import io

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# List objects in a partition
objects = client.list_objects(
    "datalake",
    prefix="raw/province=ha_noi/year=2025/month=12/",
    recursive=True
)

for obj in objects:
    print(obj.object_name)
    
    # Download and read Parquet file
    response = client.get_object("datalake", obj.object_name)
    parquet_data = response.read()
    
    # Read Parquet into DataFrame
    df = pd.read_parquet(io.BytesIO(parquet_data))
    print(df.head())
```

### Sử dụng PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadFromMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read partitioned Parquet data
df = spark.read.parquet("s3a://datalake/raw/province=ha_noi/year=2025/month=12/")
df.show()

# Read all data with automatic partition discovery
df_all = spark.read.parquet("s3a://datalake/raw/")
df_all.printSchema()
df_all.show()

# Query with partition filtering (very efficient!)
df_filtered = spark.read.parquet("s3a://datalake/raw/") \
    .filter("_partition_year = 2025 AND _partition_month = 12")
df_filtered.show()
```

## Monitoring

### Kiểm tra Kafka messages

```bash
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9093 \
    --topic batdongsan \
    --from-beginning
```

### Kiểm tra MinIO storage

Truy cập MinIO Console: http://localhost:9001

Hoặc sử dụng MinIO Client (mc):

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc ls local/datalake/raw/
mc tree local/datalake/raw/
```

## Tắt hệ thống

```bash
docker-compose down
```

Để xóa cả volumes (dữ liệu):

```bash
docker-compose down -v
```

## Lưu ý

1. **Batch Processing**: Consumer ghi dữ liệu theo batch (mặc định 50 records hoặc 30 giây) để tối ưu hiệu suất
2. **Partition Strategy**: Dữ liệu được phân vùng theo tỉnh/năm/tháng giúp truy vấn nhanh hơn
3. **Data Format**: JSONL format giúp dễ dàng append và đọc từng dòng
4. **Metadata**: Mỗi record được thêm metadata về thời gian ingest và partition info

## Troubleshooting

### Consumer không nhận được messages

- Kiểm tra Kafka topic có data chưa
- Xác nhận consumer group offset
- Kiểm tra network connectivity

### Không tạo được bucket trong MinIO

- Kiểm tra MinIO đã khởi động chưa: `docker ps`
- Kiểm tra credentials
- Xem logs: `docker logs minio`

### Lỗi khi ghi vào MinIO

- Kiểm tra disk space
- Xác nhận bucket permissions
- Kiểm tra MinIO logs
