# 🔄 Architecture Comparison: Streaming vs Batch vs Hybrid

## 📊 Kiến trúc Lambda Architecture

```
                    ┌─────────────────────────┐
                    │   DATA SOURCE LAYER     │
                    │   (Scrapy Crawler)      │
                    │   - bds.com.vn          │
                    └──────────┬──────────────┘
                               │
                ┏━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━┓
                ┃                              ┃
    ┌───────────▼──────────┐      ┌───────────▼──────────┐
    │   SPEED LAYER        │      │   BATCH LAYER        │
    │   (Real-time)        │      │   (Historical)       │
    ├──────────────────────┤      ├──────────────────────┤
    │ • Kafka Queue        │      │ • JSON/CSV Files     │
    │ • Spark Streaming    │      │ • Spark Batch        │
    │ • < 1s latency       │      │ • Scheduler          │
    │ • Continuous         │      │ • On-demand/Cron     │
    │                      │      │ • Aggregations       │
    └───────────┬──────────┘      └───────────┬──────────┘
                │                              │
                └───────────┬──────────────────┘
                            ▼
                ┌──────────────────────────┐
                │   SERVING LAYER          │
                ├──────────────────────────┤
                │ • Elasticsearch (Search) │
                │ • Kibana (Visualization) │
                │ • Parquet (Data Lake)    │
                └──────────────────────────┘
```

---

## 🎯 So sánh 3 Architecture Modes

### **Mode 1: Pure Streaming (Hiện tại)**

```
Scrapy → Kafka → Spark Streaming → Elasticsearch → Kibana
```

**Ưu điểm:**
- ✅ Latency thấp (< 1 giây)
- ✅ Real-time monitoring
- ✅ Phù hợp cho alerts, dashboards

**Nhược điểm:**
- ❌ Khó làm complex aggregations
- ❌ Không xử lý historical data tốt
- ❌ Phải running 24/7

**Use cases:**
- Real-time price monitoring
- Instant search updates
- Live dashboard

---

### **Mode 2: Pure Batch**

```
Scrapy → Files → Spark Batch → Elasticsearch/Parquet
```

**Ưu điểm:**
- ✅ Complex analytics dễ dàng
- ✅ Xử lý historical data hiệu quả
- ✅ Resource-efficient (chỉ chạy khi cần)
- ✅ Có thể reprocess data

**Nhược điểm:**
- ❌ Latency cao (phút → giờ)
- ❌ Không real-time

**Use cases:**
- Daily/weekly reports
- Historical trend analysis
- ML model training
- Data warehouse updates

---

### **Mode 3: Hybrid (Lambda Architecture) ⭐ RECOMMENDED**

```
              Scrapy
                │
        ┌───────┴───────┐
        ▼               ▼
     Kafka            Files
        ▼               ▼
   Streaming         Batch
        └───────┬───────┘
                ▼
         Elasticsearch
```

**Ưu điểm:**
- ✅ Best of both worlds
- ✅ Real-time + historical analytics
- ✅ Flexible query patterns
- ✅ Data redundancy

**Nhược điểm:**
- ❌ Complexity cao hơn
- ❌ Cần maintain 2 pipelines

**Use cases:**
- Production systems
- Khi cần cả real-time và analytics
- Compliance requirements

---

## 📁 File Structure Mới

```
bigdata_alter/
├── spark_streaming.py          # Speed Layer (existing)
├── spark_batch.py              # Batch Layer (NEW)
├── batch_scheduler.py          # Scheduler (NEW)
├── export_to_batch.py          # Batch Export Pipeline (NEW)
├── unified_pipeline.py         # Unified Manager (NEW)
│
├── data/                       # (NEW)
│   ├── batch_input/           # Raw batch files
│   └── processed_batch/       # Processed Parquet files
│
├── BATCH_PROCESSING_GUIDE.md  # Full guide (NEW)
├── QUICKSTART_BATCH.md        # Quick start (NEW)
├── setup_batch.bat            # Windows setup (NEW)
└── setup_batch.sh             # Linux setup (NEW)
```

---

## 🚀 Quick Start for Each Mode

### **Streaming Only**
```bash
# Terminal 1
python spark_streaming.py

# Terminal 2
cd bds
scrapy crawl bds_spider
```

### **Batch Only**
```bash
# One-time run
python spark_batch.py

# Or scheduled
python batch_scheduler.py
```

### **Hybrid (Recommended)**
```bash
# Easy way
python unified_pipeline.py
# Choose option 4: "Start Both"

# Manual way
# Terminal 1: Streaming
python spark_streaming.py

# Terminal 2: Batch scheduler
python batch_scheduler.py

# Terminal 3: Crawler
cd bds
scrapy crawl bds_spider
```

---

## 📊 Data Flow Comparison

### **Streaming Flow**
```
1. Scrapy scrapes → item
2. PushToKafka pipeline → Kafka
3. Spark Streaming reads → process
4. Write to Elasticsearch → immediate
⏱️ Total: < 2 seconds
```

### **Batch Flow**
```
1. Scrapy scrapes → item
2. ExportToBatchFile pipeline → JSON file
3. [Wait until scheduled time or manual trigger]
4. Spark Batch reads → process → aggregate
5. Write to Elasticsearch + Parquet
⏱️ Total: Scheduled (e.g., daily)
```

### **Hybrid Flow**
```
1. Scrapy scrapes → item
2. Split to:
   - PushToKafka → Streaming → ES (real-time)
   - ExportToBatchFile → Files → Batch → ES + Parquet (scheduled)
3. Serving layer merges both views
⏱️ Real-time + Historical
```

---

## 🎯 When to Use What?

| Requirement | Streaming | Batch | Hybrid |
|-------------|-----------|-------|--------|
| Real-time dashboard | ✅ | ❌ | ✅ |
| Daily reports | ❌ | ✅ | ✅ |
| Complex aggregations | ⚠️ | ✅ | ✅ |
| Historical analysis | ❌ | ✅ | ✅ |
| Cost optimization | ❌ | ✅ | ⚠️ |
| Data reprocessing | ❌ | ✅ | ✅ |
| Low latency | ✅ | ❌ | ✅ |
| Simpler maintenance | ✅ | ✅ | ❌ |

---

## 💡 Recommended Scenarios

### **Scenario 1: Student Project / Demo**
→ **Use Streaming Only**
- Đơn giản, dễ demo
- Real-time impressive cho presentation
- Ít phức tạp

### **Scenario 2: Production System**
→ **Use Hybrid**
- Reliable với data backup
- Flexible analytics
- Compliance ready

### **Scenario 3: Research / Analytics Focus**
→ **Use Batch**
- Focus vào data quality
- Complex statistics
- ML model training

### **Scenario 4: Real Estate Agency App**
→ **Use Hybrid**
- Users cần real-time search
- Business cần daily reports
- Marketing cần trend analysis

---

## 🔧 Configuration Matrix

### **Scrapy Pipeline Config**

**Streaming only:**
```python
ITEM_PIPELINES = {
    'batdongsan.pipelines.BatdongsanPipeline': 300,
    'batdongsan.pipelines.PushToKafka': 400,
}
```

**Batch only:**
```python
ITEM_PIPELINES = {
    'batdongsan.pipelines.BatdongsanPipeline': 300,
    'export_to_batch.ExportToBatchFile': 500,
}
```

**Hybrid:**
```python
ITEM_PIPELINES = {
    'batdongsan.pipelines.BatdongsanPipeline': 300,
    'batdongsan.pipelines.PushToKafka': 400,
    'export_to_batch.ExportToBatchFile': 500,
}
```

---

## 📈 Performance Characteristics

### **Throughput**
- **Streaming:** ~1000 items/second
- **Batch:** ~10000 items/second (với optimization)

### **Latency**
- **Streaming:** < 1 second
- **Batch:** Minutes to hours (scheduled)

### **Resource Usage**
- **Streaming:** Continuous CPU/Memory
- **Batch:** Burst CPU/Memory, idle otherwise

### **Data Quality**
- **Streaming:** Basic validation
- **Batch:** Advanced validation + deduplication

---

## 🎓 Learning Path

**Beginner:** Start with Streaming
1. Understand the basic flow
2. See real-time updates in Kibana
3. Simple to debug

**Intermediate:** Add Batch
1. Learn Spark batch processing
2. Understand scheduling
3. Data lake concepts

**Advanced:** Implement Hybrid
1. Unified architecture
2. Handle both pipelines
3. Production-ready system

---

## 🔗 Next Steps

1. **Read guides:**
   - `BATCH_PROCESSING_GUIDE.md` - Chi tiết đầy đủ
   - `QUICKSTART_BATCH.md` - Bắt đầu nhanh

2. **Run setup:**
   ```bash
   # Windows
   setup_batch.bat
   
   # Linux/macOS
   chmod +x setup_batch.sh
   ./setup_batch.sh
   ```

3. **Try unified manager:**
   ```bash
   python unified_pipeline.py
   ```

4. **Experiment:**
   - Chạy từng mode
   - So sánh performance
   - Chọn phù hợp với use case của bạn

---

## 📞 Troubleshooting

**Q: Nên chọn mode nào?**
A: Nếu học → Streaming. Nếu production → Hybrid.

**Q: Cả 2 pipelines có conflict?**
A: Không, chúng independent. Elasticsearch tự merge dựa trên `link` ID.

**Q: Performance có ảnh hưởng?**
A: Minimal. Batch chỉ chạy theo lịch, không ảnh hưởng streaming.

**Q: Làm sao để test?**
A: Dùng `unified_pipeline.py` để dễ dàng switch giữa các modes.
