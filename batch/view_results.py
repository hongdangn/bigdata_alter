#!/usr/bin/env python3
"""
View and analyze ETL results from MinIO
"""
import pandas as pd
from minio import Minio
from minio.error import S3Error
import sys
import os
import shutil
from tabulate import tabulate

def connect_minio():
    """Connect to MinIO"""
    try:
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        return client
    except Exception as e:
        print(f"❌ Failed to connect to MinIO: {e}")
        print("\nMake sure:")
        print("  1. Docker is running")
        print("  2. MinIO container is up: docker ps | grep minio")
        sys.exit(1)

def list_files(client, bucket, prefix):
    """List files in MinIO bucket"""
    try:
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        return objects
    except S3Error as e:
        print(f"❌ Error listing files: {e}")
        return []

def download_and_view(client, bucket, object_name, output_file):
    """Download Parquet file and view content"""
    try:
        client.fget_object(bucket, object_name, output_file)
        df = pd.read_parquet(output_file)
        return df
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None

def download_and_view_partitioned(client, bucket, prefix, output_dir="temp_parquet"):
    """Download all parquet files from a partitioned folder and read as single DataFrame"""
    try:
        # Clean and create temp directory
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(output_dir)
        
        # Download all parquet files
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
        
        if not parquet_files:
            return None
        
        for obj in parquet_files:
            file_name = obj.object_name.replace('/', '_')
            local_path = os.path.join(output_dir, file_name)
            client.fget_object(bucket, obj.object_name, local_path)
        
        # Read all parquet files as one DataFrame
        df = pd.read_parquet(output_dir)
        
        # Cleanup
        shutil.rmtree(output_dir)
        
        return df
    except Exception as e:
        print(f"❌ Error reading partitioned data: {e}")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        return None

def main():
    print("=" * 80)
    print("📊 ETL RESULTS VIEWER")
    print("=" * 80)
    print()
    
    # Connect to MinIO
    print("🔗 Connecting to MinIO...")
    client = connect_minio()
    bucket = "datalake"
    
    # Check bucket exists
    if not client.bucket_exists(bucket):
        print(f"❌ Bucket '{bucket}' does not exist!")
        sys.exit(1)
    print(f"✅ Connected to bucket: {bucket}")
    print()
    
    # === BRONZE LAYER ===
    print("=" * 80)
    print("🥉 BRONZE LAYER (Raw Data)")
    print("=" * 80)
    bronze_files = list_files(client, bucket, "raw/")
    print(f"📁 Total files: {len(bronze_files)}")
    
    total_size = sum(obj.size for obj in bronze_files)
    print(f"💾 Total size: {total_size / 1024 / 1024:.2f} MB")
    
    # Group by province
    provinces = {}
    for obj in bronze_files:
        if "province=" in obj.object_name:
            province = obj.object_name.split("province=")[1].split("/")[0]
            provinces[province] = provinces.get(province, 0) + 1
    
    print(f"🌍 Provinces: {len(provinces)}")
    for province, count in sorted(provinces.items(), key=lambda x: x[1], reverse=True):
        print(f"   - {province}: {count} files")
    print()
    
    # === SILVER LAYER ===
    print("=" * 80)
    print("🥈 SILVER LAYER (Cleaned Data)")
    print("=" * 80)
    silver_files = list_files(client, bucket, "silver/")
    print(f"📁 Total files: {len(silver_files)}")
    
    # Filter out _SUCCESS files
    silver_parquet_files = [f for f in silver_files if f.object_name.endswith('.parquet')]
    
    if silver_parquet_files:
        # Download and preview first file
        first_file = silver_parquet_files[0]
        print(f"📄 Previewing: {first_file.object_name}")
        
        df_silver = download_and_view(client, bucket, first_file.object_name, "temp_silver.parquet")
        
        if df_silver is not None:
            print(f"\n✅ Records: {len(df_silver):,}")
            print(f"📋 Columns: {len(df_silver.columns)}")
            print(f"🔑 Columns: {', '.join(df_silver.columns[:10])}")
            
            if len(df_silver.columns) > 10:
                print(f"           ... and {len(df_silver.columns) - 10} more")
            
            print("\n📊 Sample data (first 5 rows):")
            print(tabulate(df_silver.head(), headers='keys', tablefmt='pretty', showindex=False))
            
            # Statistics
            if 'price_vnd' in df_silver.columns:
                print(f"\n💰 Price Statistics:")
                print(f"   - Average: {df_silver['price_vnd'].mean():,.0f} VND")
                print(f"   - Median:  {df_silver['price_vnd'].median():,.0f} VND")
                print(f"   - Min:     {df_silver['price_vnd'].min():,.0f} VND")
                print(f"   - Max:     {df_silver['price_vnd'].max():,.0f} VND")
            
            if 'square_m2' in df_silver.columns:
                print(f"\n📐 Square Statistics:")
                print(f"   - Average: {df_silver['square_m2'].mean():.1f} m²")
                print(f"   - Median:  {df_silver['square_m2'].median():.1f} m²")
    else:
        print("⚠️  No Silver files found. Run ETL batch job first!")
    print()
    
    # === GOLD LAYER ===
    print("=" * 80)
    print("🥇 GOLD LAYER (Analytics)")
    print("=" * 80)
    gold_files = list_files(client, bucket, "gold/")
    print(f"📁 Total files: {len(gold_files)}")
    
    if gold_files:
        # Group by analytics type
        analytics = {}
        for obj in gold_files:
            parts = obj.object_name.split("/")
            if len(parts) >= 2:
                analysis_type = parts[1]
                analytics[analysis_type] = analytics.get(analysis_type, 0) + 1
        
        print(f"📈 Analytics types: {len(analytics)}")
        for atype, count in analytics.items():
            print(f"   - {atype}: {count} files")
        
        # Preview District Aggregation
        district_files = [obj for obj in gold_files if "district_aggregation" in obj.object_name and obj.object_name.endswith('.parquet')]
        if district_files:
            print(f"\n📊 District Aggregation Preview:")
            df_district = download_and_view(client, bucket, district_files[0].object_name, "temp_district.parquet")
            
            if df_district is not None:
                print(f"✅ Districts analyzed: {len(df_district)}")
                print("\nTop 10 districts by average price:")
                top_districts = df_district.nlargest(10, 'avg_price_vnd')[
                    ['province_clean', 'district_clean', 'listing_count', 'avg_price_vnd', 'avg_price_per_m2']
                ]
                print(tabulate(top_districts, headers='keys', tablefmt='pretty', showindex=False))
        
        # Preview Province Summary
        province_files = [obj for obj in gold_files if "province_summary" in obj.object_name and obj.object_name.endswith('.parquet')]
        if province_files:
            print(f"\n🌍 Province Summary Preview:")
            df_province = download_and_view(client, bucket, province_files[0].object_name, "temp_province.parquet")
            
            if df_province is not None:
                print(f"✅ Provinces analyzed: {len(df_province)}")
                print("\nProvince statistics:")
                print(tabulate(df_province, headers='keys', tablefmt='pretty', showindex=False))
        
        # Preview Daily Trends
        daily_files = [obj for obj in gold_files if "daily_trends" in obj.object_name and obj.object_name.endswith('.parquet')]
        if daily_files:
            print(f"\n📅 Daily Trends Preview:")
            # Read first parquet file (no longer partitioned)
            df_daily = download_and_view(client, bucket, daily_files[0].object_name, "temp_daily.parquet")
            
            if df_daily is not None:
                print(f"✅ Daily records: {len(df_daily)}")
                print("\nTop 10 days by posting count:")
                top_days = df_daily.nlargest(10, 'daily_post_count')[
                    ['province_clean', 'post_year', 'post_month', 'post_day', 'daily_post_count']
                ]
                print(tabulate(top_days, headers='keys', tablefmt='pretty', showindex=False))
        
        # Preview Quality Metrics
        quality_files = [obj for obj in gold_files if "quality_metrics" in obj.object_name and obj.object_name.endswith('.parquet')]
        if quality_files:
            print(f"\n✅ Quality Metrics Preview:")
            df_quality = download_and_view(client, bucket, quality_files[0].object_name, "temp_quality.parquet")
            
            if df_quality is not None:
                print(f"✅ Quality scores tracked: {len(df_quality)}")
                print("\nData quality distribution:")
                print(tabulate(df_quality.sort_values('quality_score'), headers='keys', tablefmt='pretty', showindex=False))
    else:
        print("⚠️  No Gold files found. Run ETL batch job first!")
    print()
    
    # === EXPORT OPTIONS ===
    print("=" * 80)
    print("💾 EXPORT OPTIONS")
    print("=" * 80)
    
    # Check if we have data to export
    has_data = False
    if silver_parquet_files and 'df_silver' in locals() and df_silver is not None:
        has_data = True
    if district_files and 'df_district' in locals() and df_district is not None:
        has_data = True
    if province_files and 'df_province' in locals() and df_province is not None:
        has_data = True
    if daily_files and 'df_daily' in locals() and df_daily is not None:
        has_data = True
    if quality_files and 'df_quality' in locals() and df_quality is not None:
        has_data = True
    
    if not has_data:
        print("⚠️  No data available to export.")
        print("   Please run ETL batch job first: python etl_batch_job.py --mode full")
    else:
        print("Would you like to export data to CSV? (y/n)")
        choice = input("> ").strip().lower()
        
        if choice == 'y':
            print("\n📤 Exporting data to CSV...")
            exported_count = 0
            
            # Export Silver
            if silver_parquet_files and 'df_silver' in locals() and df_silver is not None:
                df_silver.to_csv("sample/silver_data.csv", index=False, encoding='utf-8-sig')
                print("✅ Exported: silver_data.csv")
                exported_count += 1
            
            # Export Gold District
            if district_files and 'df_district' in locals() and df_district is not None:
                df_district.to_csv("sample/district_aggregation.csv", index=False, encoding='utf-8-sig')
                print("✅ Exported: district_aggregation.csv")
                exported_count += 1
            
            # Export Gold Province
            if province_files and 'df_province' in locals() and df_province is not None:
                df_province.to_csv("sample/province_summary.csv", index=False, encoding='utf-8-sig')
                print("✅ Exported: province_summary.csv")
                exported_count += 1
            
            # Export Gold Daily Trends
            if daily_files and 'df_daily' in locals() and df_daily is not None:
                df_daily.to_csv("sample/daily_trends.csv", index=False, encoding='utf-8-sig')
                print("✅ Exported: daily_trends.csv")
                exported_count += 1
            
            # Export Gold Quality Metrics
            if quality_files and 'df_quality' in locals() and df_quality is not None:
                df_quality.to_csv("sample/quality_metrics.csv", index=False, encoding='utf-8-sig')
                print("✅ Exported: quality_metrics.csv")
                exported_count += 1
            
            if exported_count > 0:
                print(f"\n📂 Exported {exported_count} file(s). Open with Excel or your favorite tool!")
            else:
                print("\n⚠️  No data was exported.")
    
    print()
    print("=" * 80)
    print("✅ DONE!")
    print("=" * 80)
    print()
    print("Next steps:")
    print("  - View data in MinIO Console: http://localhost:9001")
    print("  - Run more ETL: 4_run_etl_batch.bat")
    print("  - Schedule daily ETL: python etl_scheduler.py")
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
