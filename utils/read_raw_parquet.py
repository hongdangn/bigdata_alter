#!/usr/bin/env python3
"""
Đọc và xem Parquet files từ MinIO raw layer
"""
import pandas as pd
from minio import Minio
from minio.error import S3Error

def read_raw_parquet():
    """Đọc Parquet files từ MinIO raw/"""
    
    # Connect MinIO
    client = Minio("localhost:9000",
                   access_key="minioadmin",
                   secret_key="minioadmin",
                   secure=False)
    
    # List files trong raw/
    print("📁 Files trong datalake/raw/:\n")
    objects = list(client.list_objects("datalake", prefix="raw/", recursive=True))
    
    if not objects:
        print("⚠️  Không có file nào trong raw/")
        print("   Hãy chạy crawler và kafka_to_minio.py trước!")
        return
    
    # Hiển thị danh sách files
    for i, obj in enumerate(objects, 1):
        size_mb = obj.size / 1024 / 1024
        print(f"{i}. {obj.object_name}")
        print(f"   Size: {size_mb:.2f} MB | Modified: {obj.last_modified}")
    
    # Chọn file để xem
    print("\n" + "="*80)
    choice = input(f"Chọn file để xem (1-{len(objects)}) hoặc Enter để xem file đầu tiên: ").strip()
    
    if not choice:
        selected_obj = objects[0]
    else:
        try:
            selected_obj = objects[int(choice) - 1]
        except (ValueError, IndexError):
            print("❌ Lựa chọn không hợp lệ!")
            return
    
    # Download và đọc file
    print(f"\n📥 Đang đọc: {selected_obj.object_name}")
    temp_file = "temp_raw.parquet"
    
    try:
        client.fget_object("datalake", selected_obj.object_name, temp_file)
        
        # Đọc Parquet
        df = pd.read_parquet(temp_file)
        
        print(f"\n✅ Đã đọc thành công!")
        print(f"📊 Số lượng records: {len(df)}")
        print(f"📋 Số lượng columns: {len(df.columns)}")
        print(f"\n🔑 Columns: {', '.join(df.columns.tolist())}\n")
        
        # Hiển thị sample data
        print("="*80)
        print("📄 Sample data (10 dòng đầu):")
        print("="*80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        print(df.head(10))
        
        # Statistics
        print("\n" + "="*80)
        print("📈 Statistics:")
        print("="*80)
        print(df.describe())
        
        # Export option
        print("\n" + "="*80)
        export = input("💾 Export to CSV? (y/n): ").strip().lower()
        if export == 'y':
            csv_file = selected_obj.object_name.replace('/', '_').replace('.parquet', '.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"✅ Exported to: {csv_file}")
        
        # Cleanup
        import os
        os.remove(temp_file)
        
    except Exception as e:
        print(f"❌ Lỗi khi đọc file: {e}")

if __name__ == "__main__":
    read_raw_parquet()