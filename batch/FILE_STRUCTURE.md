# 📁 CÁC FILE QUAN TRỌNG TRONG PROJECT

## 🎯 SCRIPTS TỰ ĐỘNG (Khuyến nghị dùng)

| File | Mô tả | Khi nào dùng |
|------|-------|--------------|
| **START_PIPELINE.bat** | 🚀 Chạy tất cả pipeline 1 lần | Lần đầu setup, hoặc muốn chạy nhanh |
| **1_start_services.bat** | Khởi động Docker services | Bắt đầu session mới |
| **2_start_crawler.bat** | Bật crawler (Terminal 1) | Crawl data từ web |
| **3_start_kafka_to_minio.bat** | Bật Kafka→MinIO (Terminal 2) | Lưu data vào data lake |
| **4_run_etl_batch.bat** | Chạy ETL batch (Bronze→Silver→Gold) | Sau khi có đủ data |
| **5_view_results.bat** | Xem kết quả ETL | Xem data sau ETL |
| **9_stop_all.bat** | Dừng tất cả services | Khi xong hoặc muốn tắt máy |

---

## 📖 TÀI LIỆU HƯỚNG DẪN

| File | Nội dung |
|------|----------|
| **QUICK_START.md** | 🚀 Hướng dẫn nhanh cho người mới |
| **RUN_PIPELINE.md** | 📚 Hướng dẫn chi tiết từng bước |
| **ETL_GUIDE.md** | Kiến trúc Medallion (Bronze/Silver/Gold) |
| **MINIO_SETUP.md** | Setup MinIO data lake |
| **CRAWLER_GUIDE.md** | Hướng dẫn crawler Scrapy |
| **SCHEDULING_GUIDE.md** | Lập lịch tự động |

---

## 🐍 PYTHON SCRIPTS CHÍNH

### 1. Thu thập dữ liệu (Crawling)
| File | Chức năng |
|------|-----------|
| `run_continuous_crawler.py` | Crawler liên tục nhiều tỉnh |
| `run_multi_province.py` | Crawler tuần tự các tỉnh |
| `bds/batdongsan/spiders/bds_spider.py` | Scrapy spider chính |

### 2. Data Pipeline
| File | Chức năng |
|------|-----------|
| `kafka_to_minio.py` | 📦 Kafka → MinIO (Bronze layer) |
| `spark_streaming.py` | ⚡ Spark Streaming (real-time) |
| `etl_batch_job.py` | 🎯 **ETL BATCH CHÍNH** (Bronze→Silver→Gold) |
| `etl_scheduler.py` | 📅 Lập lịch ETL tự động |

### 3. Tiện ích
| File | Chức năng |
|------|-----------|
| `view_results.py` | 👁️ Xem kết quả ETL |
| `monitor.py` | 📊 Giám sát pipeline |
| `pre_process.py` | 🧹 Xử lý text tiếng Việt |
| `utils.py` | 🔧 Hàm tiện ích chung |

---

## ⚙️ CẤU HÌNH

| File | Mô tả |
|------|-------|
| `docker-compose.yml` | 🐳 Cấu hình Kafka, MinIO, ES, Kibana |
| `requirements.txt` | 📦 Python dependencies |
| `.env` | 🔐 Biến môi trường (nếu có) |
| `bds/scrapy.cfg` | Cấu hình Scrapy project |
| `bds/batdongsan/settings.py` | Settings Scrapy spider |
| `bds/batdongsan/pipelines.py` | Pipeline gửi Kafka |

---

## 🗂️ CẤU TRÚC THỨ MỤC

```
bigdata_alter/
├── 📁 bds/                          # Scrapy project
│   ├── scrapy.cfg
│   ├── run_crawler.sh
│   └── batdongsan/
│       ├── spiders/
│       │   └── bds_spider.py        # Spider chính
│       ├── pipelines.py             # Kafka pipeline
│       └── settings.py
│
├── 🎯 SCRIPTS CHẠY PIPELINE
│   ├── START_PIPELINE.bat           # ⭐ Chạy tất cả 1 lần
│   ├── 1_start_services.bat
│   ├── 2_start_crawler.bat
│   ├── 3_start_kafka_to_minio.bat
│   ├── 4_run_etl_batch.bat
│   ├── 5_view_results.bat
│   └── 9_stop_all.bat
│
├── 📚 TÀI LIỆU
│   ├── QUICK_START.md               # ⭐ Bắt đầu từ đây
│   ├── RUN_PIPELINE.md              # Hướng dẫn chi tiết
│   ├── ETL_GUIDE.md
│   ├── MINIO_SETUP.md
│   ├── CRAWLER_GUIDE.md
│   └── SCHEDULING_GUIDE.md
│
├── 🐍 PYTHON SCRIPTS
│   ├── run_continuous_crawler.py    # Crawler liên tục
│   ├── run_multi_province.py        # Crawler nhiều tỉnh
│   ├── kafka_to_minio.py            # Kafka → MinIO
│   ├── spark_streaming.py           # Spark Streaming
│   ├── etl_batch_job.py             # ⭐ ETL BATCH CHÍNH
│   ├── etl_scheduler.py             # Scheduler
│   ├── view_results.py              # Xem kết quả
│   ├── monitor.py
│   ├── pre_process.py
│   └── utils.py
│
├── ⚙️ CẤU HÌNH
│   ├── docker-compose.yml           # Docker services
│   ├── requirements.txt             # Python deps
│   └── README.md
│
└── 📂 bigdata/                      # Python virtualenv
    └── ...
```

---

## 🎯 LỘ TRÌNH HỌC TẬP

### 1️⃣ Người mới bắt đầu
```
1. Đọc: QUICK_START.md
2. Chạy: START_PIPELINE.bat
3. Đợi 1-2 giờ
4. Chạy: 4_run_etl_batch.bat
5. Chạy: 5_view_results.bat
```

### 2️⃣ Hiểu rõ từng bước
```
1. Đọc: RUN_PIPELINE.md
2. Chạy từng script thủ công:
   - 1_start_services.bat
   - 2_start_crawler.bat
   - 3_start_kafka_to_minio.bat
   - 4_run_etl_batch.bat
3. Xem code trong từng .py file
4. Đọc: ETL_GUIDE.md để hiểu Medallion Architecture
```

### 3️⃣ Nâng cao - Custom pipeline
```
1. Đọc code: etl_batch_job.py
2. Hiểu logic: Bronze → Silver → Gold
3. Thay đổi transformation logic
4. Đọc: SCHEDULING_GUIDE.md để tự động hóa
5. Deploy production
```

---

## 🚀 CÁCH DÙNG NHANH NHẤT

### Lần đầu chạy
```bash
# 1. Double-click:
START_PIPELINE.bat

# 2. Đợi 1-2 giờ (xem MinIO: http://localhost:9001)

# 3. Double-click:
4_run_etl_batch.bat

# 4. Double-click:
5_view_results.bat
```

### Lần sau (đã có data)
```bash
# 1. Khởi động services
1_start_services.bat

# 2. Chạy ETL
4_run_etl_batch.bat

# 3. Xem kết quả
5_view_results.bat
```

---

## ❓ CÂU HỎI THƯỜNG GẶP

### Q: File nào chạy đầu tiên?
**A:** `START_PIPELINE.bat` hoặc đọc `QUICK_START.md`

### Q: ETL batch ở đâu?
**A:** `etl_batch_job.py` - chạy bằng `4_run_etl_batch.bat`

### Q: Xem dữ liệu ở đâu?
**A:** 
- MinIO Console: http://localhost:9001
- Hoặc chạy: `5_view_results.bat`

### Q: Lỗi "Docker not running"?
**A:** Mở Docker Desktop trước khi chạy

### Q: Crawl bao lâu?
**A:** Tối thiểu 1-2 giờ để có đủ data demo

### Q: Data lưu ở đâu?
**A:** 
- Docker volume: MinIO data
- Thực tế: `D:\minio\data\datalake\`

---

## 📞 SUPPORT

Nếu gặp lỗi, kiểm tra:
1. Docker Desktop đã chạy chưa
2. `docker ps` - Xem containers
3. Logs trong các terminal
4. File log: `crawler.log`, `minio.log`

---

## 🎉 HAPPY CODING!

Project này implement **Medallion Architecture** với:
- 🥉 **Bronze:** Raw data từ Kafka
- 🥈 **Silver:** Cleaned, deduplicated, standardized
- 🥇 **Gold:** Analytics, aggregations, metrics

Powered by: Scrapy + Kafka + MinIO + PySpark 🚀
