@echo off
REM Setup Batch Processing Environment
REM For Windows

echo ================================================
echo   Batch Processing Setup
echo ================================================
echo.

REM Create directories
echo [1/4] Creating directories...
if not exist "data" mkdir data
if not exist "data\batch_input" mkdir data\batch_input
if not exist "data\processed_batch" mkdir data\processed_batch
echo   - Created data directories
echo.

REM Install dependencies
echo [2/4] Installing Python dependencies...
pip install apscheduler -q
echo   - Installed apscheduler
echo.

REM Check Docker services
echo [3/4] Checking Docker services...
docker-compose ps
echo.

REM Create Kafka topic if needed
echo [4/4] Creating Kafka topic (if not exists)...
docker exec kafka kafka-topics --create --topic batdongsan --bootstrap-server localhost:9093 --partitions 3 --replication-factor 1 2>nul
if %errorlevel% equ 0 (
    echo   - Kafka topic created
) else (
    echo   - Kafka topic already exists or Docker not running
)
echo.

echo ================================================
echo   Setup Complete!
echo ================================================
echo.
echo Next steps:
echo   1. Start streaming: python spark_streaming.py
echo   2. Start batch: python spark_batch.py
echo   3. Or use unified manager: python unified_pipeline.py
echo.
pause
