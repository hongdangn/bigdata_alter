# Hướng dẫn Crawl liên tục



## Cách sử dụng


### ⚙️ Crawler nâng cao với nhiều tính năng

```bash
# Chạy liên tục cho Hà Nội, interval 120 phút rồi lặp lại quá trình crawl (mục đích: trong 120 phút đó, có thể có các dữ liệu mới, vậy thì lặp lại để crawl tiếp -> Lưu vào trong minio để Spark xử lý, loại bỏ các subject bị crawl 2 lần )

# Crawl cho Hà Nội (khoảng 10 tiếng)

```bash
python run_continuous_crawler.py \
    --provinces ha-noi \
    --interval 99999 \
    --max-page 1050
```

```bash
python kafka_to_minio.py
```



# Crawl cho Hồ Chí Minh (khoảng 10 tiếng)
```bash
python run_continuous_crawler.py \
    --provinces ho-chi-minh \
    --interval 99999 \
    --max-page 910
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
