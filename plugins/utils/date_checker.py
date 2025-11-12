"""
Date Checker Utility
Checks Diamond bucket for available dates and identifies gaps
"""

import boto3
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, DIAMOND_BUCKET


def get_available_dates_in_diamond():
    """
    Scan Diamond bucket and return all available dates
    
    Returns:
        list: [datetime objects] of dates with data
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )
    
    try:
        response = s3_client.list_objects_v2(Bucket=DIAMOND_BUCKET)
    except Exception as e:
        print(f"⚠️ Error accessing Diamond bucket: {e}")
        return []
    
    if 'Contents' not in response:
        print("⚠️ Diamond bucket is empty")
        return []
    
    # Extract dates from filenames: diamond_final_YYYY_MM_DD.csv
    available_dates = set()
    
    for obj in response['Contents']:
        key = obj['Key']
        if key.startswith('diamond_final_') and '/' in key:
            try:
                # Extract date: diamond_final_2025_11_10.csv/part-00000.csv -> 2025_11_10
                filename = key.split('/')[0]  # Get directory name
                date_part = filename.replace('diamond_final_', '').replace('.csv', '')
                date_obj = datetime.strptime(date_part, '%Y_%m_%d')
                available_dates.add(date_obj)
            except ValueError:
                continue
    
    return sorted(list(available_dates))


def find_missing_dates(n_days=5, end_date=None):
    """
    Find dates in last N days that are missing from Diamond bucket
    
    Args:
        n_days: Number of days to check (default 5)
        end_date: End date to check from (default today)
    
    Returns:
        dict: {
            'available': [datetime objects],
            'missing': [datetime objects],
            'expected': [datetime objects],
            'coverage_pct': float
        }
    """
    if end_date is None:
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Get available dates from Diamond
    available_dates = get_available_dates_in_diamond()
    
    # Calculate expected date range
    # Note: Wikipedia API has 1-3 day delay, so check from 3 days ago
    # This ensures we only look for dates that should have data available
    expected_dates = []
    for i in range(3, n_days + 3):  # Start from 3 days ago to account for API delay
        date = end_date - timedelta(days=i)
        expected_dates.append(date)
    
    # Find missing dates
    available_set = set(available_dates)
    expected_set = set(expected_dates)
    missing_dates = sorted(list(expected_set - available_set))
    
    # Calculate coverage
    coverage_pct = (len(available_set & expected_set) / len(expected_set) * 100) if expected_set else 0
    
    return {
        'available': sorted(list(available_set & expected_set)),
        'missing': missing_dates,
        'expected': expected_dates,
        'coverage_pct': coverage_pct,
        'total_in_bucket': len(available_dates)
    }


def print_date_report(n_days=5):
    """
    Print formatted report of date availability
    """
    print("\n" + "="*70)
    print("📅 DIAMOND BUCKET DATE AVAILABILITY REPORT")
    print("="*70)
    
    result = find_missing_dates(n_days)
    
    print(f"\n📊 Analysis for last {n_days} days (accounting for 3-day API delay):")
    print(f"   • Total files in Diamond: {result['total_in_bucket']}")
    print(f"   • Expected dates: {len(result['expected'])}")
    print(f"   • Available dates: {len(result['available'])}")
    print(f"   • Missing dates: {len(result['missing'])}")
    print(f"   • Coverage: {result['coverage_pct']:.1f}%")
    
    if result['available']:
        print(f"\n✅ Available Dates ({len(result['available'])}):")
        for date in result['available']:
            print(f"   • {date.strftime('%Y-%m-%d (%A)')}")
    
    if result['missing']:
        print(f"\n⚠️ Missing Dates ({len(result['missing'])}):")
        for date in result['missing']:
            print(f"   • {date.strftime('%Y-%m-%d (%A)')}")
        print(f"\n💡 These dates need to be fetched from Wikipedia API")
    else:
        print(f"\n✅ All expected dates are available!")
    
    print("\n" + "="*70)
    
    return result


def get_dates_to_fetch(n_days=5):
    """
    Get list of dates that need to be fetched
    Returns dates in format suitable for triggering DAG runs
    
    Returns:
        list: [{'date': 'YYYY-MM-DD', 'date_obj': datetime}, ...]
    """
    result = find_missing_dates(n_days)
    
    return [
        {
            'date': date.strftime('%Y-%m-%d'),
            'date_formatted': date.strftime('%Y_%m_%d'),
            'date_obj': date,
            'days_ago': (datetime.now() - date).days
        }
        for date in result['missing']
    ]


if __name__ == "__main__":
    # Test the date checker
    import sys
    
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    result = print_date_report(days)
    
    missing = get_dates_to_fetch(days)
    if missing:
        print(f"\n🔄 Need to fetch {len(missing)} dates:")
        for item in missing:
            print(f"   • {item['date']} ({item['days_ago']} days ago)")
