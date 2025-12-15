#!/usr/bin/env python3
"""
Run crawler for multiple provinces sequentially or continuously
Simpler alternative to run_continuous_crawler.py
"""
import subprocess
import sys
import os
import time
from datetime import datetime

# Common provinces in Vietnam
PROVINCES = [
    'ha-noi',
    'ho-chi-minh',
    'da-nang',
    'hai-phong',
    'can-tho',
    'bien-hoa',
    'vung-tau',
    'nha-trang',
    'hue',
    'hai-duong',
    'nam-dinh',
    'thai-nguyen',
    'vinh',
    'quy-nhon',
    'da-lat',
    'vinh-phuc'
]


def run_crawler_for_province(province, max_page=100):
    """
    Run crawler for a single province
    
    Args:
        province: Province slug
        max_page: Maximum page to crawl
    """
    print(f"\n{'='*60}")
    print(f"Crawling province: {province}")
    print(f"Max pages: {max_page}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    cmd = f"scrapy crawl bds_spider -a province={province} -a max_page={max_page}"
    
    try:
        subprocess.run(cmd, shell=True, cwd="bds", check=True)
        print(f"\n✓ Successfully crawled {province}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Failed to crawl {province}: {e}")
        return False


def run_all_provinces(provinces, max_page=100, continuous=False, interval_minutes=60):
    """
    Run crawler for all specified provinces
    
    Args:
        provinces: List of province slugs
        max_page: Maximum page per province
        continuous: If True, run in a loop
        interval_minutes: Minutes between runs (if continuous)
    """
    run_count = 0
    
    try:
        while True:
            run_count += 1
            
            print(f"\n{'#'*80}")
            print(f"# RUN #{run_count}")
            print(f"# Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"# Provinces: {len(provinces)}")
            print(f"{'#'*80}\n")
            
            success = 0
            failed = 0
            
            for i, province in enumerate(provinces, 1):
                print(f"\n[{i}/{len(provinces)}] Province: {province}")
                
                if run_crawler_for_province(province, max_page):
                    success += 1
                else:
                    failed += 1
                
                # Small delay between provinces
                if i < len(provinces):
                    print("\nWaiting 5 seconds before next province...")
                    time.sleep(5)
            
            # Summary
            print(f"\n{'='*80}")
            print(f"RUN #{run_count} SUMMARY:")
            print(f"  Success: {success}")
            print(f"  Failed: {failed}")
            print(f"  Total: {len(provinces)}")
            print(f"{'='*80}\n")
            
            if not continuous:
                break
            
            # Wait for next run
            print(f"\nWaiting {interval_minutes} minutes until next run...")
            print(f"Next run at: {datetime.now().replace(minute=datetime.now().minute + interval_minutes).strftime('%Y-%m-%d %H:%M:%S')}")
            print("Press Ctrl+C to stop\n")
            time.sleep(interval_minutes * 60)
            
    except KeyboardInterrupt:
        print("\n\nStopped by user")
        print(f"Total runs completed: {run_count}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run crawler for provinces')
    parser.add_argument(
        '--provinces',
        nargs='+',
        help='Specific provinces to crawl (default: all major provinces)'
    )
    parser.add_argument(
        '--max-page',
        type=int,
        default=100,
        help='Maximum pages per province (default: 100)'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuously in a loop'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Minutes between runs when continuous (default: 60)'
    )
    
    args = parser.parse_args()
    
    # Determine which provinces to crawl
    provinces_to_crawl = args.provinces if args.provinces else PROVINCES
    
    print(f"\n{'='*80}")
    print(f"CRAWLER CONFIGURATION")
    print(f"{'='*80}")
    print(f"Provinces: {', '.join(provinces_to_crawl)}")
    print(f"Max pages per province: {args.max_page}")
    print(f"Continuous mode: {args.continuous}")
    if args.continuous:
        print(f"Interval: {args.interval} minutes")
    print(f"{'='*80}\n")
    
    # Run
    run_all_provinces(
        provinces_to_crawl,
        max_page=args.max_page,
        continuous=args.continuous,
        interval_minutes=args.interval
    )
