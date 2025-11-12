"""
Task to fetch Wikipedia top pages
"""

from datetime import datetime, timedelta
import requests
import pandas as pd
from io import StringIO
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BRONZE_BUCKET, create_s3_client, ensure_bucket_exists


def fetch_top_pages(**context):
    """Fetch top Wikipedia pages and save to Bronze layer (MinIO)"""
    print("🔄 Starting Wikipedia top pages fetch...")
    
    # Check if a specific target date was provided (from orchestrator)
    dag_run = context.get('dag_run')
    target_date_str = None
    
    if dag_run and dag_run.conf:
        target_date_str = dag_run.conf.get('target_date')
    
    if target_date_str:
        # Specific date provided - use it directly
        try:
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
            print(f"🎯 Target date specified: {target_date_str}")
            dates_to_try = [target_date]  # Only try the specified date
        except ValueError:
            print(f"⚠️ Invalid target_date format: {target_date_str}, using default fallback")
            # Fall back to default behavior
            dates_to_try = [
                datetime.now() - timedelta(days=1),  # Yesterday
                datetime.now() - timedelta(days=2),  # 2 days ago
                datetime.now() - timedelta(days=3),  # 3 days ago
            ]
    else:
        # No specific date - use default fallback (yesterday, 2 days ago, 3 days ago)
        print("📅 No target date specified, using default fallback logic")
        dates_to_try = [
            datetime.now() - timedelta(days=1),  # Yesterday
            datetime.now() - timedelta(days=2),  # 2 days ago
            datetime.now() - timedelta(days=3),  # 3 days ago
        ]
    
    successful_date = None  # Track which date worked
    
    for attempt_date in dates_to_try:
        date_str = attempt_date.strftime("%Y/%m/%d")
        print(f"� Attempting to fetch data for: {date_str}")
        
        # Make API request
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{date_str}"
        headers = {
            'User-Agent': 'Wikipedia Top Pages Analyzer/1.0 (Educational Project)',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 404:
                print(f"⚠️ Data not available for {date_str} (404 - Not Found)")
                continue  # Try next date
            
            if response.status_code != 200:
                print(f"⚠️ API returned status {response.status_code} for {date_str}")
                continue  # Try next date
            
            # Parse response
            data = response.json()
            
            # Validate response structure
            if 'items' not in data or not data['items']:
                print(f"⚠️ Invalid response structure for {date_str}")
                continue  # Try next date
            
            if 'articles' not in data['items'][0]:
                print(f"⚠️ No articles found in response for {date_str}")
                continue  # Try next date
            
            # Success! Save the successful date and break
            successful_date = attempt_date
            print(f"✅ Successfully fetched data for {date_str}")
            break
            
        except requests.RequestException as e:
            print(f"⚠️ Request failed for {date_str}: {str(e)}")
            continue  # Try next date
    
    else:
        # All dates failed
        attempted_dates = [d.strftime("%Y/%m/%d") for d in dates_to_try]
        raise Exception(f"Failed to fetch Wikipedia data. Tried dates: {', '.join(attempted_dates)}")
    
    # Generate filename based on the ACTUAL date that was successfully fetched
    filename = f"top_pages_{successful_date.strftime('%Y_%m_%d')}.csv"
    
    # Validate final response
    if 'items' not in data or not data['items']:
        raise Exception(f"Invalid API response structure. Response: {data}")
    
    if 'articles' not in data['items'][0]:
        raise Exception(f"No articles found in API response. Response: {data}")
    
    print(f"✓ Fetched {len(data.get('items', []))} items from API")
    
    # Extract articles data
    articles = data['items'][0]['articles']
    print(f"✓ Found {len(articles)} articles for {successful_date.strftime('%Y-%m-%d')}")
    
    # Convert to DataFrame
    df = pd.DataFrame(articles)
    print(f"✓ Converted to DataFrame with {len(df)} records")
    
    # Ensure Bronze bucket exists
    ensure_bucket_exists(BRONZE_BUCKET)
    
    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Upload to MinIO Bronze bucket
    s3_client = create_s3_client()
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=filename,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    print(f"✅ Uploaded to MinIO: s3://{BRONZE_BUCKET}/{filename}")
    print(f"   • Records: {len(df)}")
    print(f"   • Columns: {list(df.columns)}")
    print(f"   • Date fetched: {successful_date.strftime('%Y-%m-%d')}")
    
    # Return just filename for backward compatibility with downstream tasks
    return filename
