# Hướng dẫn Crawl liên tục

## Vấn đề: Tại sao crawler tự động dừng?

### Nguyên nhân chính:

1. **HTTP Cache được bật vĩnh viễn** (`HTTPCACHE_EXPIRATION_SECS = 0`)
   - Tất cả requests đều được serve từ cache
   - Không có request thực sự nào được gửi đi
   - Spider nhanh chóng "hoàn thành" vì chỉ đọc cache

2. **Spider thiết kế để chạy một lần**
   - Khi hết trang hoặc hết link → dừng
   - Không có cơ chế crawl định kỳ

### Đã fix:

✅ Tắt HTTP cache trong production (`HTTPCACHE_ENABLED = False`)  
✅ Thêm cache expiration time nếu bật lại  
✅ Tạo scripts để chạy crawler liên tục

---

## Cách sử dụng

### 🚀 Option 1: Xóa cache và chạy lại (đơn giản nhất)

```bash
# Xóa cache cũ
rm -rf bds/.scrapy/httpcache/

# Hoặc trên Windows
# Remove-Item -Recurse -Force bds\.scrapy\httpcache\

# Chạy crawler
cd bds
scrapy crawl bds_spider -a province=ha-noi -a max_page=100
```

### 🔄 Option 2: Chạy nhiều tỉnh (script đơn giản)

```bash
# Chạy một lần cho Hà Nội, max 100 trang
python run_multi_province.py --provinces ha-noi --max-page 100

# Chạy nhiều tỉnh
python run_multi_province.py --provinces ha-noi ho-chi-minh da-nang --max-page 50

# Chạy liên tục, mỗi 60 phút một lần
python run_multi_province.py --provinces ha-noi ho-chi-minh --continuous --interval 60

# Chạy tất cả tỉnh lớn (15 tỉnh)
python run_multi_province.py --max-page 100
```

### ⚙️ Option 3: Crawler nâng cao với nhiều tính năng

```bash
# Chạy liên tục cho Hà Nội, interval 30 phút
python run_continuous_crawler.py --provinces ha-noi --interval 30 --max-page 100

# Nhiều tỉnh
python run_continuous_crawler.py \
    --provinces ha-noi ho-chi-minh da-nang hai-phong \
    --interval 60 \
    --max-page 100
```

### 🐳 Option 4: Chạy với Docker (khuyến nghị cho production)

Tạo file `docker-compose.crawler.yml`:

```yaml
version: '3.8'

services:
  crawler:
    build: 
      context: .
      dockerfile: Dockerfile.crawler
    container_name: crawler
    depends_on:
      - kafka
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9093
      - CRAWL_INTERVAL=60
      - MAX_PAGE=100
      - PROVINCES=ha-noi,ho-chi-minh,da-nang
    networks:
      - batdongsan-network
    restart: unless-stopped
```

---

## Cấu hình HTTP Cache

### Trong `bds/batdongsan/settings.py`:

```python
# Development: Bật cache để tiết kiệm bandwidth
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600  # 1 giờ

# Production: Tắt cache để lấy dữ liệu mới
HTTPCACHE_ENABLED = False
HTTPCACHE_EXPIRATION_SECS = 3600  # Chỉ dùng khi bật
```

### Xóa cache:

```bash
# Linux/Mac
rm -rf bds/.scrapy/httpcache/

# Windows PowerShell
Remove-Item -Recurse -Force bds\.scrapy\httpcache\

# Windows CMD
rmdir /s /q bds\.scrapy\httpcache\
```

---

## Kiểm tra logs

### Dấu hiệu crawl từ cache:

```
'httpcache/hit': 1624,  # ← Tất cả từ cache
'downloader/response_count': 1624,
'elapsed_time_seconds': 58.16,  # ← Quá nhanh, chỉ ~1 phút
```

### Dấu hiệu crawl thực sự:

```
'downloader/response_bytes': 15814634,  # ← Có download
'httpcache/miss': 1200,  # ← Có requests mới
'httpcache/hit': 424,
'elapsed_time_seconds': 600.45,  # ← Mất thời gian hợp lý
```

---

## Tham số Spider

```bash
scrapy crawl bds_spider \
    -a province=ha-noi \        # Tỉnh cần crawl
    -a min_page=1 \             # Trang bắt đầu
    -a max_page=100 \           # Trang kết thúc
    -a jump_to_page=50          # Nhảy đến trang cụ thể (optional)
```

---

## Danh sách tỉnh phổ biến

```python
PROVINCES = [
    'ha-noi',
    'ho-chi-minh',
    'da-nang',
    'hai-phong',
    'can-tho',
    'bien-hoa',
    'vung-tau',
    'nha-trang',
    'hue',
    'hai-duong',
    'nam-dinh',
    'thai-nguyen',
    'vinh',
    'quy-nhon',
    'da-lat',
]
```

---

## Luồng dữ liệu hoàn chỉnh

```
┌─────────────┐
│   Crawler   │ ──> Scrapy crawl website
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Kafka    │ ──> Queue messages
└──────┬──────┘
       │
       ├──> kafka_to_minio.py ──> MinIO Data Lake
       │                          (raw/province=xxx/year=xxxx/month=xx/)
       │
       └──> spark_streaming.py ──> Elasticsearch
                                    (Kibana visualization)
```

---

## Monitoring

### Kiểm tra Kafka có data không:

```bash
docker exec -it kafka kafka-console-consumer \
    --bootstrap-server localhost:9093 \
    --topic batdongsan \
    --from-beginning \
    --max-messages 10
```

### Kiểm tra MinIO có data không:

```python
from minio import Minio

client = Minio("localhost:9000", "minioadmin", "minioadmin", secure=False)
objects = list(client.list_objects("datalake", prefix="raw/", recursive=True))
print(f"Total objects: {len(objects)}")
for obj in objects[:10]:
    print(f"  - {obj.object_name} ({obj.size} bytes)")
```

### Kiểm tra Elasticsearch:

```bash
curl http://localhost:9200/batdongsan/_count
```

---

## Tips

1. **Crawl thử với max_page nhỏ** trước (10-20 trang) để test
2. **Tắt cache** khi muốn dữ liệu mới nhất
3. **Dùng continuous mode** cho production
4. **Monitor Kafka lag** để đảm bảo consumer theo kịp
5. **Backup MinIO** định kỳ

---

## Troubleshooting

### Crawler vẫn dừng nhanh?

1. Xóa cache: `rm -rf bds/.scrapy/httpcache/`
2. Kiểm tra `HTTPCACHE_ENABLED = False`
3. Restart từ đầu

### Không có data trong Kafka?

1. Kiểm tra Kafka đang chạy: `docker ps`
2. Xem crawler logs có lỗi không
3. Test Kafka connection

### MinIO không nhận data?

1. Chạy `kafka_to_minio.py`
2. Kiểm tra MinIO credentials
3. Xem logs consumer
