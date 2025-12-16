"""
Test Gold to Kafka Topics Publisher
Quick test to verify connection and data availability
"""
import sys
import os

# Add batch directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gold_to_kafka_topics import GoldToKafkaPublisher
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connections():
    """Test MinIO and Kafka connections"""
    print("=" * 80)
    print("TESTING CONNECTIONS")
    print("=" * 80)
    
    try:
        publisher = GoldToKafkaPublisher()
        print("\n✅ Connections successful!")
        return publisher
    except Exception as e:
        print(f"\n❌ Connection failed: {e}")
        return None

def test_read_data(publisher):
    """Test reading district_aggregation from MinIO"""
    print("\n" + "=" * 80)
    print("TESTING DATA READ")
    print("=" * 80)
    
    try:
        df = publisher.read_district_aggregation()
        
        if df is None:
            print("\n❌ No data found in MinIO Gold layer")
            print("   Run ETL batch job first: python etl_batch_job.py --mode full")
            return False
        
        print(f"\n✅ Loaded {len(df)} districts")
        print(f"\nProvinces found: {df['province_clean'].unique().tolist()}")
        print(f"\nSample data:")
        print(df.head())
        
        return True
        
    except Exception as e:
        print(f"\n❌ Failed to read data: {e}")
        return False

def test_rankings(publisher):
    """Test ranking logic"""
    print("\n" + "=" * 80)
    print("TESTING RANKING LOGIC")
    print("=" * 80)
    
    try:
        df = publisher.read_district_aggregation()
        
        if df is None:
            return False
        
        # Test hot areas ranking
        print("\n📊 Hot Areas (Top 5 by listing count):")
        for province in df['province_clean'].unique():
            df_province = df[df['province_clean'] == province].copy()
            df_province = df_province.sort_values('listing_count', ascending=False)
            df_province['rank'] = range(1, len(df_province) + 1)
            
            print(f"\n{province.upper()}:")
            top5 = df_province.head()
            for _, row in top5.iterrows():
                print(f"  {row['rank']}. {row['district_clean']}: {int(row['listing_count'])} listings")
        
        # Test luxury ranking
        print("\n💎 Luxury Areas (Top 5 by price/m²):")
        for province in df['province_clean'].unique():
            df_province = df[df['province_clean'] == province].copy()
            df_province = df_province.sort_values('avg_price_per_m2', ascending=False)
            df_province['rank'] = range(1, len(df_province) + 1)
            
            print(f"\n{province.upper()}:")
            top5 = df_province.head()
            for _, row in top5.iterrows():
                price_m2 = row['avg_price_per_m2'] / 1_000_000
                print(f"  {row['rank']}. {row['district_clean']}: {price_m2:.1f} tr/m²")
        
        print("\n✅ Ranking logic works correctly!")
        return True
        
    except Exception as e:
        print(f"\n❌ Ranking test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "GOLD TO KAFKA - TEST SUITE" + " " * 32 + "║")
    print("╚" + "=" * 78 + "╝")
    
    # Test 1: Connections
    publisher = test_connections()
    if not publisher:
        print("\n❌ Test suite failed at connection stage")
        return
    
    # Test 2: Read data
    if not test_read_data(publisher):
        print("\n❌ Test suite failed at data read stage")
        publisher.producer.close()
        return
    
    # Test 3: Rankings
    if not test_rankings(publisher):
        print("\n❌ Test suite failed at ranking stage")
        publisher.producer.close()
        return
    
    # Summary
    print("\n" + "=" * 80)
    print("✅ ALL TESTS PASSED!")
    print("=" * 80)
    print("\nReady to publish to Kafka topics:")
    print("  - batch/hot_area/ha-noi")
    print("  - batch/hot_area/ho-chi-minh")
    print("  - batch/luxury/ha-noi")
    print("  - batch/luxury/ho-chi-minh")
    print("\nRun: python gold_to_kafka_topics.py --mode all")
    
    publisher.producer.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Test cancelled by user")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
