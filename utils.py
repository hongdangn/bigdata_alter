"""
Utility functions for data processing and normalization
"""
from unidecode import unidecode
import re


def normalize_province_name(province_name):
    """
    Normalize province name to a standard format for partitioning
    - Remove Vietnamese accents
    - Convert to lowercase
    - Replace spaces with underscores
    - Remove special characters
    
    Examples:
        "Hà Nội" -> "ha_noi"
        "Hồ Chí Minh" -> "ho_chi_minh"
        "Đà Nẵng" -> "da_nang"
    
    Args:
        province_name (str): Original province name
        
    Returns:
        str: Normalized province name
    """
    if not province_name:
        return "unknown"
    
    # Remove accents
    normalized = unidecode(province_name)
    
    # Convert to lowercase
    normalized = normalized.lower()
    
    # Remove special characters except spaces and hyphens
    normalized = re.sub(r'[^a-z0-9\s-]', '', normalized)
    
    # Replace spaces and hyphens with underscores
    normalized = re.sub(r'[\s-]+', '_', normalized)
    
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    
    return normalized if normalized else "unknown"


def get_partition_path(province, year, month, base_path="raw"):
    """
    Generate partition path for data lake storage
    
    Args:
        province (str): Province name (will be normalized)
        year (int or str): Year
        month (int or str): Month
        base_path (str): Base directory name (default: "raw")
        
    Returns:
        str: Partition path in format "raw/province=xxx/year=xxxx/month=xx"
    """
    normalized_province = normalize_province_name(province)
    
    # Ensure month is zero-padded
    month_str = str(month).zfill(2)
    
    return f"{base_path}/province={normalized_province}/year={year}/month={month_str}"


if __name__ == "__main__":
    # Test cases
    test_provinces = [
        "Hà Nội",
        "Hồ Chí Minh",
        "Đà Nẵng",
        "Hải Phòng",
        "Cần Thơ",
        "Bà Rịa - Vũng Tàu",
        "Thừa Thiên Huế"
    ]
    
    print("Province normalization tests:")
    print("-" * 50)
    for province in test_provinces:
        normalized = normalize_province_name(province)
        path = get_partition_path(province, 2025, 12)
        print(f"{province:20} -> {normalized:20} -> {path}")
    
    print("\n" + "=" * 50)
    print("Empty/None handling:")
    print(f"None -> {normalize_province_name(None)}")
    print(f"Empty -> {normalize_province_name('')}")
