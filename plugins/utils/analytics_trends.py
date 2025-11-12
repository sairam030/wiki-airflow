"""
Analytics Module - Trend Analysis for Wikipedia Pages
Analyzes last 5 days of data from Diamond bucket
Tracks article growth/decline and creates analytics tables
"""

import pandas as pd
import boto3
from datetime import datetime, timedelta
import psycopg2
from io import StringIO
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, DIAMOND_BUCKET


def get_last_n_days_files(n_days=5):
    """
    Get list of Diamond bucket files from last N days
    
    Returns:
        list: [(date_str, filename), ...] sorted by date descending
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )
    
    # List all files in Diamond bucket
    response = s3_client.list_objects_v2(Bucket=DIAMOND_BUCKET)
    
    if 'Contents' not in response:
        print("⚠️ No files found in Diamond bucket")
        return []
    
    # Extract date from filename: diamond_final_YYYY_MM_DD.csv
    files_with_dates = []
    for obj in response['Contents']:
        key = obj['Key']
        if key.startswith('diamond_final_') and key.endswith('.csv') and '/' in key:
            # Extract actual CSV file (not directory)
            actual_file = key.split('/')[-1]
            if actual_file.endswith('.csv'):
                try:
                    # Extract date: diamond_final_2025_11_10.csv -> 2025_11_10
                    date_part = actual_file.replace('diamond_final_', '').replace('.csv', '')
                    date_obj = datetime.strptime(date_part, '%Y_%m_%d')
                    files_with_dates.append((date_obj, key, obj['LastModified']))
                except ValueError:
                    continue
    
    # Sort by date descending (most recent first)
    files_with_dates.sort(reverse=True, key=lambda x: x[0])
    
    # Take last N days
    result = [(d.strftime('%Y-%m-%d'), key) for d, key, _ in files_with_dates[:n_days]]
    
    print(f"📅 Found {len(result)} files from last {n_days} days:")
    for date_str, key in result:
        print(f"   • {date_str}: {key}")
    
    return result


def load_daily_data(date_str, file_key):
    """
    Load data for a specific date from Diamond bucket
    
    Returns:
        DataFrame with added 'date' column
    """
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )
    
    try:
        obj = s3_client.get_object(Bucket=DIAMOND_BUCKET, Key=file_key)
        df = pd.read_csv(obj['Body'])
        
        # Add date column
        df['date'] = date_str
        
        print(f"✓ Loaded {len(df)} records for {date_str}")
        return df
        
    except Exception as e:
        print(f"❌ Error loading {file_key}: {e}")
        return None


def analyze_trends(days=5):
    """
    Main analytics function - analyzes last N days of data
    
    Returns:
        dict: {
            'all_data': DataFrame with all days combined,
            'common_articles': DataFrame with articles present in all days,
            'growth_metrics': DataFrame with growth/decline stats,
            'date_range': (start_date, end_date)
        }
    """
    print("\n" + "="*70)
    print("📊 WIKIPEDIA TRENDS ANALYTICS")
    print("="*70)
    
    # Get files from last N days
    daily_files = get_last_n_days_files(days)
    
    if len(daily_files) < 2:
        print("⚠️ Need at least 2 days of data for trend analysis")
        return None
    
    # Load all daily data
    all_dataframes = []
    for date_str, file_key in daily_files:
        df = load_daily_data(date_str, file_key)
        if df is not None:
            all_dataframes.append(df)
    
    if not all_dataframes:
        print("❌ No data loaded successfully")
        return None
    
    # Combine all data
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"\n✓ Combined data: {len(combined_df)} total records across {len(all_dataframes)} days")
    
    # Find common articles (present in ALL days)
    print("\n🔍 Finding articles present in all days...")
    
    article_date_counts = combined_df.groupby('page')['date'].nunique()
    common_articles = article_date_counts[article_date_counts == len(all_dataframes)].index.tolist()
    
    print(f"✓ Found {len(common_articles)} articles present in all {len(all_dataframes)} days")
    
    # Filter to common articles only
    common_df = combined_df[combined_df['page'].isin(common_articles)].copy()
    
    # Calculate growth metrics for common articles
    print("\n📈 Calculating growth metrics...")
    
    growth_metrics = []
    for article in common_articles:
        article_data = common_df[common_df['page'] == article].sort_values('date')
        
        if len(article_data) < 2:
            continue
        
        # Get first and last day data
        first_day = article_data.iloc[0]
        last_day = article_data.iloc[-1]
        
        # Calculate metrics
        views_change = last_day['views'] - first_day['views']
        views_change_pct = (views_change / first_day['views'] * 100) if first_day['views'] > 0 else 0
        rank_change = first_day['rank'] - last_day['rank']  # Positive = improved rank
        
        # Average views across all days
        avg_views = article_data['views'].mean()
        max_views = article_data['views'].max()
        min_views = article_data['views'].min()
        
        growth_metrics.append({
            'page': article,
            'page_display': last_day['page_display'],
            'category': last_day['category'],
            'location': last_day.get('location', 'N/A'),
            'first_date': first_day['date'],
            'last_date': last_day['date'],
            'first_views': int(first_day['views']),
            'last_views': int(last_day['views']),
            'views_change': int(views_change),
            'views_change_pct': round(views_change_pct, 2),
            'first_rank': int(first_day['rank']),
            'last_rank': int(last_day['rank']),
            'rank_change': int(rank_change),
            'avg_views': int(avg_views),
            'max_views': int(max_views),
            'min_views': int(min_views),
            'volatility': int(max_views - min_views),
            'days_tracked': len(article_data)
        })
    
    growth_df = pd.DataFrame(growth_metrics)
    
    # Sort by absolute views change
    growth_df = growth_df.sort_values('views_change', ascending=False)
    
    print(f"✓ Calculated metrics for {len(growth_df)} articles")
    
    # Summary statistics
    print("\n📊 TREND ANALYSIS SUMMARY")
    print("-" * 70)
    print(f"Date Range: {daily_files[-1][0]} to {daily_files[0][0]}")
    print(f"Total Articles Tracked: {len(common_articles)}")
    print(f"\nTop Growing Articles:")
    print(growth_df.nlargest(5, 'views_change')[['page_display', 'views_change', 'views_change_pct']])
    print(f"\nTop Declining Articles:")
    print(growth_df.nsmallest(5, 'views_change')[['page_display', 'views_change', 'views_change_pct']])
    
    return {
        'all_data': combined_df,
        'common_articles': common_df,
        'growth_metrics': growth_df,
        'date_range': (daily_files[-1][0], daily_files[0][0]),
        'days_analyzed': len(all_dataframes)
    }


def create_analytics_tables():
    """
    Create PostgreSQL tables for analytics data
    Separate from main wiki_diamond table
    """
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    print("\n🗄️ Creating analytics tables...")
    
    # Table 1: Time series data (all days combined)
    cursor.execute("DROP TABLE IF EXISTS wiki_trends_timeseries CASCADE;")
    cursor.execute("""
        CREATE TABLE wiki_trends_timeseries (
            id SERIAL PRIMARY KEY,
            page TEXT,
            page_display TEXT,
            date DATE,
            views INT,
            rank INT,
            category TEXT,
            location_type TEXT,
            location TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            UNIQUE(page, date)
        );
        
        CREATE INDEX idx_trends_page ON wiki_trends_timeseries(page);
        CREATE INDEX idx_trends_date ON wiki_trends_timeseries(date);
        CREATE INDEX idx_trends_category ON wiki_trends_timeseries(category);
    """)
    
    # Table 2: Common articles (present in all days)
    cursor.execute("DROP TABLE IF EXISTS wiki_trends_common CASCADE;")
    cursor.execute("""
        CREATE TABLE wiki_trends_common (
            id SERIAL PRIMARY KEY,
            page TEXT,
            page_display TEXT,
            date DATE,
            views INT,
            rank INT,
            category TEXT,
            location TEXT,
            UNIQUE(page, date)
        );
        
        CREATE INDEX idx_common_page ON wiki_trends_common(page);
        CREATE INDEX idx_common_date ON wiki_trends_common(date);
    """)
    
    # Table 3: Growth metrics
    cursor.execute("DROP TABLE IF EXISTS wiki_trends_growth CASCADE;")
    cursor.execute("""
        CREATE TABLE wiki_trends_growth (
            id SERIAL PRIMARY KEY,
            page TEXT UNIQUE,
            page_display TEXT,
            category TEXT,
            location TEXT,
            first_date DATE,
            last_date DATE,
            first_views INT,
            last_views INT,
            views_change INT,
            views_change_pct DECIMAL(10,2),
            first_rank INT,
            last_rank INT,
            rank_change INT,
            avg_views INT,
            max_views INT,
            min_views INT,
            volatility INT,
            days_tracked INT
        );
        
        CREATE INDEX idx_growth_views_change ON wiki_trends_growth(views_change DESC);
        CREATE INDEX idx_growth_category ON wiki_trends_growth(category);
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✅ Analytics tables created successfully")


def load_analytics_to_postgres(analytics_data):
    """
    Load analytics data into PostgreSQL tables
    """
    if not analytics_data:
        print("❌ No analytics data to load")
        return
    
    print("\n💾 Loading analytics data to PostgreSQL...")
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    # Load timeseries data
    print("   • Loading timeseries data...")
    timeseries_df = analytics_data['all_data']
    for _, row in timeseries_df.iterrows():
        cursor.execute("""
            INSERT INTO wiki_trends_timeseries 
            (page, page_display, date, views, rank, category, location_type, location, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (page, date) DO UPDATE SET
                views = EXCLUDED.views,
                rank = EXCLUDED.rank
        """, (
            row['page'],
            row.get('page_display', row['page'].replace('_', ' ')),
            row['date'],
            row['views'],
            row['rank'],
            row['category'],
            row.get('location_type'),
            row.get('location'),
            row.get('latitude'),
            row.get('longitude')
        ))
    
    # Load common articles
    print("   • Loading common articles data...")
    common_df = analytics_data['common_articles']
    for _, row in common_df.iterrows():
        cursor.execute("""
            INSERT INTO wiki_trends_common 
            (page, page_display, date, views, rank, category, location)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (page, date) DO UPDATE SET
                views = EXCLUDED.views,
                rank = EXCLUDED.rank
        """, (
            row['page'],
            row.get('page_display', row['page'].replace('_', ' ')),
            row['date'],
            row['views'],
            row['rank'],
            row['category'],
            row.get('location')
        ))
    
    # Load growth metrics
    print("   • Loading growth metrics...")
    growth_df = analytics_data['growth_metrics']
    for _, row in growth_df.iterrows():
        cursor.execute("""
            INSERT INTO wiki_trends_growth 
            (page, page_display, category, location, first_date, last_date,
             first_views, last_views, views_change, views_change_pct,
             first_rank, last_rank, rank_change, avg_views, max_views, min_views,
             volatility, days_tracked)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (page) DO UPDATE SET
                last_views = EXCLUDED.last_views,
                views_change = EXCLUDED.views_change,
                views_change_pct = EXCLUDED.views_change_pct,
                last_rank = EXCLUDED.last_rank,
                rank_change = EXCLUDED.rank_change
        """, (
            row['page'],
            row['page_display'],
            row['category'],
            row['location'],
            row['first_date'],
            row['last_date'],
            row['first_views'],
            row['last_views'],
            row['views_change'],
            row['views_change_pct'],
            row['first_rank'],
            row['last_rank'],
            row['rank_change'],
            row['avg_views'],
            row['max_views'],
            row['min_views'],
            row['volatility'],
            row['days_tracked']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {len(timeseries_df)} timeseries records")
    print(f"✅ Loaded {len(common_df)} common article records")
    print(f"✅ Loaded {len(growth_df)} growth metrics")


def run_analytics(days=5):
    """
    Main function to run complete analytics pipeline
    """
    print("\n" + "🚀 STARTING ANALYTICS PIPELINE" + "\n")
    
    # Step 1: Create tables
    create_analytics_tables()
    
    # Step 2: Analyze trends
    analytics_data = analyze_trends(days)
    
    if not analytics_data:
        print("❌ Analytics failed - insufficient data")
        return None
    
    # Step 3: Load to PostgreSQL
    load_analytics_to_postgres(analytics_data)
    
    print("\n" + "="*70)
    print("✅ ANALYTICS PIPELINE COMPLETE")
    print("="*70)
    print(f"\n📊 You can now query these tables:")
    print("   • wiki_trends_timeseries - All data with dates")
    print("   • wiki_trends_common - Articles present in all days")
    print("   • wiki_trends_growth - Growth/decline metrics")
    print(f"\n📅 Date Range: {analytics_data['date_range'][0]} to {analytics_data['date_range'][1]}")
    print(f"📈 Days Analyzed: {analytics_data['days_analyzed']}")
    
    return analytics_data


if __name__ == "__main__":
    # Run analytics for last 5 days
    run_analytics(days=5)
