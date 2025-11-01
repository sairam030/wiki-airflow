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
    """
    Fetch Wikipedia top pages from API and upload to MinIO Bronze bucket as CSV
    (No categories - that's done in parallel by separate DAG)
    """
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y/%m/%d")
    filename = f"top_pages_{yesterday.strftime('%Y_%m_%d')}.csv"
    
    print(f"📥 Fetching Wikipedia data for {date_str}")
    
    # API request
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{date_str}"
    headers = {"User-Agent": "Wiki-Trends-Pipeline/1.0"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")

    data = response.json()
    print(f"✓ Fetched {len(data.get('items', []))} items from API")
    
    # Extract articles data
    articles = data['items'][0]['articles']
    
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
    
    # Return filename for next task
    return filename