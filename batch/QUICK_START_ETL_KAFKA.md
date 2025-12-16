# Quick Start Guide - ETL + Kafka Publishing

## Manual Run (Step by Step)

### Option 1: Complete Pipeline (Recommended)
```bash
# One command - runs both ETL and Kafka publishing
scripts\4_run_etl_and_publish.bat
```

**Flow:**
1. ✅ ETL: Bronze → Silver → Gold (creates statistics)
2. ✅ Publish: Gold → Kafka topics (creates rankings)

---

### Option 2: Separate Steps

**Step 1: Run ETL only**
```bash
cd batch
python etl_batch_job.py --mode full
```

**Step 2: Publish to Kafka**
```bash
python gold_to_kafka_topics.py --mode all
```

---

## Automated Scheduler

**Start scheduler for automatic daily/weekly runs:**
```bash
cd batch
python etl_scheduler.py --config default
```

**Schedule:**
- **Daily 2 AM**: Incremental ETL + Kafka publish
- **Sunday 3 AM**: Full refresh + Kafka publish

**Test without waiting:**
```bash
python etl_scheduler.py --test
```

---

## Verification

**Check Gold layer in MinIO:**
```bash
python view_results.py
```

**Check Kafka topics:**
```bash
# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092 | findstr batch

# Read messages
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic batch/hot_area/ha-noi --from-beginning --max-messages 5
```

**Test Kafka publisher:**
```bash
python test_kafka_publisher.py
```

---

## Scripts Overview

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `4_run_etl_batch.bat` | ETL only | Testing ETL without Kafka |
| `4_run_etl_and_publish.bat` | ETL + Kafka | **Main workflow** |
| `etl_scheduler.py` | Auto scheduling | Production daily runs |
| `gold_to_kafka_topics.py` | Kafka only | Re-publish after ETL |
| `test_kafka_publisher.py` | Test connections | Before first run |

---

## Troubleshooting

**Problem: "No parquet files in gold/district_aggregation"**
```bash
# Solution: Run ETL first
python etl_batch_job.py --mode full
```

**Problem: Kafka connection failed**
```bash
# Check Kafka is running
docker ps | findstr kafka

# Restart Kafka
docker-compose restart kafka
```

**Problem: ETL succeeds but Kafka fails**
```bash
# Just re-run Kafka publishing
python gold_to_kafka_topics.py --mode all
```

---

## Expected Output

```
================================================================================
[STEP 1/2] RUNNING ETL BATCH JOB
================================================================================
Starting Bronze -> Silver transformation
✓ Bronze -> Silver completed successfully
Starting Silver -> Gold transformation
✓ Silver -> Gold completed successfully

================================================================================
[STEP 2/2] PUBLISHING GOLD ANALYTICS TO KAFKA
================================================================================
Connected to MinIO at localhost:9000
Kafka producer connected to localhost:9092
✅ Hot Areas Published:
   batch/hot_area/ha-noi: 36 districts
   batch/hot_area/ho-chi-minh: 12 districts
✅ Luxury Areas Published:
   batch/luxury/ha-noi: 36 districts
   batch/luxury/ho-chi-minh: 12 districts

SUCCESS! COMPLETE PIPELINE FINISHED
```
