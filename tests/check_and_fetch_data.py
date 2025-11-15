#!/usr/bin/env python3
"""
Smart Data Fetcher
Checks Diamond bucket, identifies missing dates, and triggers fetches
Can be run manually or via Airflow
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'plugins'))

from utils.date_checker import print_date_report, get_dates_to_fetch
from utils.dag_trigger import trigger_multiple_dates


def main(days=5, auto_trigger=False):
    """
    Main function to check and optionally trigger missing date fetches
    
    Args:
        days: Number of days to check
        auto_trigger: Automatically trigger missing dates (without prompt)
    """
    print("\n" + "🔍 SMART DATA COMPLETENESS CHECK" + "\n")
    
    # Check dates
    result = print_date_report(days)
    
    # Get missing dates
    missing = get_dates_to_fetch(days)
    
    if not missing:
        print("\n✅ All data is complete! Ready for analytics.")
        return True
    
    print(f"\n⚠️ Missing {len(missing)} dates:")
    for item in missing:
        print(f"   • {item['date']} ({item['days_ago']} days ago)")
    
    # Ask user if they want to trigger
    if not auto_trigger:
        response = input(f"\n🤔 Trigger wiki_trending_pipeline for these {len(missing)} dates? (y/n): ").strip().lower()
        
        if response != 'y':
            print("❌ Skipping data fetch. Run analytics manually when ready.")
            return False
    
    # Trigger fetches
    print(f"\n🚀 Triggering data fetches for {len(missing)} missing dates...")
    
    dates_to_trigger = [item['date'] for item in missing]
    results = trigger_multiple_dates(dates_to_trigger, wait_between_runs=30)
    
    # Check success
    success_count = sum(1 for r in results if r.get('success', False))
    
    if success_count == len(results):
        print("\n✅ All dates fetched successfully!")
        print("📊 You can now run analytics with complete data.")
        return True
    else:
        print(f"\n⚠️ Only {success_count}/{len(results)} dates fetched successfully")
        print("💡 Check Airflow UI for failed runs and retry manually")
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Check Diamond bucket for missing dates and optionally trigger fetches'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=5,
        help='Number of days to check (default: 5)'
    )
    parser.add_argument(
        '--auto',
        action='store_true',
        help='Automatically trigger missing fetches without prompting'
    )
    
    args = parser.parse_args()
    
    success = main(days=args.days, auto_trigger=args.auto)
    
    sys.exit(0 if success else 1)
