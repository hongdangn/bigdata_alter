#!/bin/bash
# Setup Batch Processing Environment
# For Linux/macOS

echo "================================================"
echo "  Batch Processing Setup"
echo "================================================"
echo ""

# Create directories
echo "[1/4] Creating directories..."
mkdir -p data/batch_input
mkdir -p data/processed_batch
echo "  ✓ Created data directories"
echo ""

# Install dependencies
echo "[2/4] Installing Python dependencies..."
pip install apscheduler -q
echo "  ✓ Installed apscheduler"
echo ""

# Check Docker services
echo "[3/4] Checking Docker services..."
docker-compose ps
echo ""

# Create Kafka topic if needed
echo "[4/4] Creating Kafka topic (if not exists)..."
docker exec kafka kafka-topics --create \
    --topic batdongsan \
    --bootstrap-server localhost:9093 \
    --partitions 3 \
    --replication-factor 1 2>/dev/null

if [ $? -eq 0 ]; then
    echo "  ✓ Kafka topic created"
else
    echo "  ℹ Kafka topic already exists or Docker not running"
fi
echo ""

echo "================================================"
echo "  Setup Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo "  1. Start streaming: python spark_streaming.py"
echo "  2. Start batch: python spark_batch.py"
echo "  3. Or use unified manager: python unified_pipeline.py"
echo ""
