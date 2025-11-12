"""
Smart Analytics Orchestrator DAG
Automatically ensures data completeness and loads to PostgreSQL

Flow:
1. Check Diamond bucket for last N days
2. Identify missing dates
3. Trigger wiki_trending_pipeline for each missing date
4. Wait for all data to be ready
5. Load last N days to PostgreSQL (wiki_timeseries table with date column)
6. Find common articles present in ALL days (wiki_common_articles table)

This is a SEPARATE orchestrator - does not modify existing DAGs
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from utils.date_checker import find_missing_dates, get_dates_to_fetch, get_available_dates_in_diamond
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, DIAMOND_BUCKET
import pandas as pd
import boto3
import psycopg2


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_data_completeness(**context):
    """
    Task 1: Check if we have all required dates in Diamond bucket
    Returns branch decision
    """
    n_days = context['dag_run'].conf.get('days', 5)
    
    print(f"\n🔍 Checking Diamond bucket for last {n_days} days...")
    
    result = find_missing_dates(n_days)
    
    print(f"\n📊 Data Completeness:")
    print(f"   • Expected dates: {len(result['expected'])}")
    print(f"   • Available dates: {len(result['available'])}")
    print(f"   • Missing dates: {len(result['missing'])}")
    print(f"   • Coverage: {result['coverage_pct']:.1f}%")
    
    # Store in XCom for downstream tasks
    context['ti'].xcom_push(key='missing_dates', value=[d.strftime('%Y-%m-%d') for d in result['missing']])
    context['ti'].xcom_push(key='available_dates', value=[d.strftime('%Y-%m-%d') for d in result['available']])
    context['ti'].xcom_push(key='coverage_pct', value=result['coverage_pct'])
    context['ti'].xcom_push(key='n_days', value=n_days)
    
    if result['missing']:
        print(f"\n⚠️ Found {len(result['missing'])} missing dates - will trigger data fetching")
        return 'trigger_missing_data_fetches'
    else:
        print(f"\n✅ All dates available - proceeding directly to load timeseries")
        return 'load_timeseries'


def trigger_missing_data_fetches(**context):
    """
    Task 2: Trigger wiki_trending_pipeline for each missing date
    """
    from airflow.api.common.trigger_dag import trigger_dag
    
    ti = context['ti']
    missing_dates = ti.xcom_pull(key='missing_dates', task_ids='check_completeness')
    
    if not missing_dates:
        print("✓ No missing dates to fetch")
        return
    
    print(f"\n🚀 Triggering {len(missing_dates)} DAG runs for missing dates...")
    
    triggered_runs = []
    
    for date_str in missing_dates:
        print(f"\n📅 Triggering for {date_str}...")
        
        try:
            # Trigger the main pipeline DAG with target date
            run = trigger_dag(
                dag_id='wiki_trending_pipeline',
                conf={'target_date': date_str},
                execution_date=None,
                replace_microseconds=False,
            )
            
            triggered_runs.append({
                'date': date_str,
                'run_id': str(run.run_id) if run else None,
                'triggered_at': datetime.now().isoformat()
            })
            
            print(f"✓ Triggered successfully")
            
            # Small delay between triggers
            time.sleep(5)
            
        except Exception as e:
            print(f"⚠️ Failed to trigger for {date_str}: {e}")
            triggered_runs.append({
                'date': date_str,
                'error': str(e),
                'triggered_at': datetime.now().isoformat()
            })
    
    # Store triggered runs info
    ti.xcom_push(key='triggered_runs', value=triggered_runs)
    
    print(f"\n✅ Triggered {len(triggered_runs)} DAG runs")
    print(f"⏳ Will wait for data to be ready before running analytics...")
    
    return triggered_runs


def wait_for_data_ready(**context):
    """
    Sensor Task: Wait for all missing dates to appear in Diamond bucket
    """
    ti = context['ti']
    missing_dates = ti.xcom_pull(key='missing_dates', task_ids='check_completeness')
    n_days = ti.xcom_pull(key='n_days', task_ids='check_completeness')
    
    if not missing_dates:
        print("✓ No missing dates - data is ready")
        return True
    
    print(f"\n⏳ Checking if all {len(missing_dates)} dates are now available...")
    
    # Re-check Diamond bucket
    result = find_missing_dates(n_days)
    
    still_missing = [d.strftime('%Y-%m-%d') for d in result['missing']]
    
    if not still_missing:
        print(f"✅ All dates now available! Coverage: {result['coverage_pct']:.1f}%")
        return True
    else:
        print(f"⏳ Still waiting for {len(still_missing)} dates: {', '.join(still_missing)}")
        return False


def load_timeseries_to_postgres(**context):
    """
    Task 3: Load last N days from Diamond to PostgreSQL with date column
    """
    ti = context['ti']
    n_days = ti.xcom_pull(key='n_days', task_ids='check_completeness')
    
    print(f"\n📊 Loading last {n_days} days to PostgreSQL...")
    
    # S3 client for MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )
    
    # Get all files from Diamond bucket
    response = s3_client.list_objects_v2(Bucket=DIAMOND_BUCKET)
    
    if 'Contents' not in response:
        raise Exception("Diamond bucket is empty")
    
    # Extract dates from filenames and get latest N days
    files_with_dates = []
    for obj in response['Contents']:
        key = obj['Key']
        if key.startswith('diamond_final_') and '/' in key:
            try:
                filename = key.split('/')[0]
                date_part = filename.replace('diamond_final_', '').replace('.csv', '')
                date_obj = datetime.strptime(date_part, '%Y_%m_%d')
                files_with_dates.append((date_obj, key))
            except ValueError:
                continue
    
    # Sort by date descending and take last N days
    files_with_dates.sort(reverse=True)
    files_to_load = files_with_dates[:n_days]
    
    print(f"� Found {len(files_to_load)} files to load:")
    for date_obj, key in files_to_load:
        print(f"   • {date_obj.strftime('%Y-%m-%d')}: {key}")
    
    # Load all data into single DataFrame
    all_data = []
    
    for date_obj, file_key in files_to_load:
        date_str = date_obj.strftime('%Y-%m-%d')
        print(f"📥 Loading {date_str}...")
        
        try:
            obj = s3_client.get_object(Bucket=DIAMOND_BUCKET, Key=file_key)
            df = pd.read_csv(obj['Body'])
            
            # Add date column
            df['date'] = date_str
            
            print(f"   ✓ {len(df)} records")
            all_data.append(df)
            
        except Exception as e:
            print(f"   ⚠️ Failed: {e}")
            continue
    
    if not all_data:
        raise Exception("No data loaded successfully")
    
    # Combine all DataFrames
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n✓ Combined {len(combined_df)} records from {len(all_data)} days")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    # Create table with date column
    print("\n🗄️ Creating wiki_timeseries table...")
    
    cursor.execute("DROP TABLE IF EXISTS wiki_timeseries CASCADE;")
    cursor.execute("""
        CREATE TABLE wiki_timeseries (
            id SERIAL PRIMARY KEY,
            page TEXT NOT NULL,
            page_display TEXT,
            date DATE NOT NULL,
            views INTEGER,
            rank INTEGER,
            category TEXT,
            location_type TEXT,
            location TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            UNIQUE(page, date)
        );
    """)
    
    # Create indexes separately
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timeseries_page ON wiki_timeseries(page);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timeseries_date ON wiki_timeseries(date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timeseries_category ON wiki_timeseries(category);")
    
    conn.commit()
    
    # Insert data
    print(f"💾 Inserting {len(combined_df)} records...")
    
    inserted_count = 0
    for _, row in combined_df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO wiki_timeseries 
                (page, page_display, date, views, rank, category, location_type, location, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (page, date) DO UPDATE SET
                    views = EXCLUDED.views,
                    rank = EXCLUDED.rank
            """, (
                row['page'],
                row.get('page_display', row['page'].replace('_', ' ')),
                row['date'],
                int(row['views']) if pd.notna(row['views']) else None,
                int(row['rank']) if pd.notna(row['rank']) else None,
                row.get('category'),
                row.get('location_type'),
                row.get('location'),
                float(row['latitude']) if pd.notna(row.get('latitude')) else None,
                float(row['longitude']) if pd.notna(row.get('longitude')) else None
            ))
            inserted_count += 1
                
        except Exception as e:
            print(f"⚠️ Error: {e}")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\n✅ Loaded {inserted_count} records to wiki_timeseries")
    
    # Store for next task
    ti.xcom_push(key='total_days', value=len(all_data))
    ti.xcom_push(key='total_records', value=inserted_count)
    
    return {
        'status': 'success',
        'records_inserted': inserted_count,
        'days_loaded': len(all_data)
    }


def find_common_articles(**context):
    """
    Task 4: Find articles present in ALL days and save to separate table
    """
    ti = context['ti']
    total_days = ti.xcom_pull(key='total_days', task_ids='load_timeseries')
    
    print(f"\n🔍 Finding articles present in all {total_days} days...")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    # Find articles present in ALL days
    cursor.execute(f"""
        SELECT 
            page,
            COUNT(DISTINCT date) as days_present
        FROM wiki_timeseries
        GROUP BY page
        HAVING COUNT(DISTINCT date) = {total_days}
    """)
    
    common_articles = cursor.fetchall()
    common_article_pages = [row[0] for row in common_articles]
    
    print(f"✓ Found {len(common_article_pages)} common articles")
    
    if len(common_article_pages) == 0:
        print("⚠️ No common articles found")
        cursor.close()
        conn.close()
        return {'status': 'success', 'common_articles': 0}
    
    # Create table
    print("\n🗄️ Creating wiki_common_articles table...")
    
    cursor.execute("DROP TABLE IF EXISTS wiki_common_articles CASCADE;")
    cursor.execute("""
        CREATE TABLE wiki_common_articles (
            id SERIAL PRIMARY KEY,
            page TEXT NOT NULL,
            page_display TEXT,
            date DATE NOT NULL,
            views INTEGER,
            rank INTEGER,
            category TEXT,
            location TEXT,
            UNIQUE(page, date)
        );
    """)
    
    # Create indexes separately (after table is created)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_common_page ON wiki_common_articles(page);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_common_date ON wiki_common_articles(date);")
    
    conn.commit()
    
    # Insert common articles data
    print(f"💾 Inserting data for {len(common_article_pages)} articles...")
    
    inserted_count = 0
    
    for page in common_article_pages:
        cursor.execute("""
            SELECT 
                page, page_display, date, views, rank, category, location
            FROM wiki_timeseries
            WHERE page = %s
            ORDER BY date
        """, (page,))
        
        rows = cursor.fetchall()
        
        for row in rows:
            try:
                cursor.execute("""
                    INSERT INTO wiki_common_articles 
                    (page, page_display, date, views, rank, category, location)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, row)
                inserted_count += 1
            except Exception as e:
                continue
    
    conn.commit()
    
    # Show top 10
    cursor.execute("""
        SELECT 
            page_display,
            AVG(views) as avg_views,
            category
        FROM wiki_common_articles
        GROUP BY page_display, category
        ORDER BY avg_views DESC
        LIMIT 10
    """)
    
    top_10 = cursor.fetchall()
    
    print(f"\n🔥 Top 10 Sustained Trending Articles:")
    for i, (page, avg_views, category) in enumerate(top_10, 1):
        print(f"   {i}. {page}: {int(avg_views):,} avg views ({category})")
    
    cursor.close()
    conn.close()
    
    print(f"\n✅ Created wiki_common_articles with {len(common_article_pages)} articles")
    
    return {
        'status': 'success',
        'common_articles': len(common_article_pages),
        'total_records': inserted_count
    }


def run_smart_analytics(**context):
    """
    Removed - analytics now done via load_timeseries and find_common_articles
    This is a placeholder for backward compatibility
    """
    pass


def save_to_historical_bucket(**context):
    """
    Task 5: Save timeseries and common articles data to historical-data bucket
    """
    print("\n📦 Saving data to historical-data bucket...")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    
    # S3 client for MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )
    
    # Ensure historical-data bucket exists
    try:
        s3_client.head_bucket(Bucket='historical-data')
        print("✓ historical-data bucket exists")
    except:
        print("Creating historical-data bucket...")
        s3_client.create_bucket(Bucket='historical-data')
        print("✓ Created historical-data bucket")
    
    # Get current timestamp for filename
    timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    
    # 1. Save wiki_timeseries data
    print("\n📊 Exporting wiki_timeseries...")
    
    query_timeseries = "SELECT * FROM wiki_timeseries ORDER BY date, page"
    df_timeseries = pd.read_sql(query_timeseries, conn)
    
    print(f"   • Records: {len(df_timeseries)}")
    print(f"   • Columns: {list(df_timeseries.columns)}")
    
    # Save to CSV
    csv_timeseries = df_timeseries.to_csv(index=False)
    filename_timeseries = f"timeseries_{timestamp}.csv"
    
    s3_client.put_object(
        Bucket='historical-data',
        Key=filename_timeseries,
        Body=csv_timeseries,
        ContentType='text/csv'
    )
    
    print(f"   ✓ Saved: s3://historical-data/{filename_timeseries}")
    
    # 2. Save wiki_common_articles data
    print("\n🔥 Exporting wiki_common_articles...")
    
    query_common = "SELECT * FROM wiki_common_articles ORDER BY date, page"
    df_common = pd.read_sql(query_common, conn)
    
    print(f"   • Records: {len(df_common)}")
    print(f"   • Unique articles: {df_common['page'].nunique()}")
    
    # Save to CSV
    csv_common = df_common.to_csv(index=False)
    filename_common = f"common_articles_{timestamp}.csv"
    
    s3_client.put_object(
        Bucket='historical-data',
        Key=filename_common,
        Body=csv_common,
        ContentType='text/csv'
    )
    
    print(f"   ✓ Saved: s3://historical-data/{filename_common}")
    
    # 3. Save summary metadata
    print("\n📋 Creating summary metadata...")
    
    cursor = conn.cursor()
    
    # Get stats
    cursor.execute("""
        SELECT 
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(DISTINCT date) as total_days,
            COUNT(DISTINCT page) as unique_articles,
            COUNT(*) as total_records
        FROM wiki_timeseries
    """)
    
    stats = cursor.fetchone()
    
    cursor.execute("SELECT COUNT(DISTINCT page) FROM wiki_common_articles")
    common_count = cursor.fetchone()[0]
    
    # Create metadata JSON
    metadata = {
        'generated_at': datetime.now().isoformat(),
        'timeseries_file': filename_timeseries,
        'common_articles_file': filename_common,
        'date_range': {
            'start': str(stats[0]),
            'end': str(stats[1])
        },
        'statistics': {
            'total_days': stats[2],
            'unique_articles': stats[3],
            'total_records': stats[4],
            'common_articles': common_count
        }
    }
    
    import json
    metadata_json = json.dumps(metadata, indent=2)
    filename_metadata = f"metadata_{timestamp}.json"
    
    s3_client.put_object(
        Bucket='historical-data',
        Key=filename_metadata,
        Body=metadata_json,
        ContentType='application/json'
    )
    
    print(f"   ✓ Saved: s3://historical-data/{filename_metadata}")
    
    cursor.close()
    conn.close()
    
    print(f"\n✅ Successfully saved all data to historical-data bucket!")
    print(f"   📂 Files:")
    print(f"      • {filename_timeseries}")
    print(f"      • {filename_common}")
    print(f"      • {filename_metadata}")
    
    return {
        'status': 'success',
        'bucket': 'historical-data',
        'files': {
            'timeseries': filename_timeseries,
            'common': filename_common,
            'metadata': filename_metadata
        },
        'timestamp': timestamp
    }


# Define the DAG
with DAG(
    dag_id='wiki_smart_analytics_orchestrator',
    default_args=default_args,
    description='Smart orchestrator: Ensures data completeness, triggers missing fetches, runs analytics',
    schedule_interval='0 3 * * *',  # Run daily at 3 AM (after data pipeline at 2 AM)
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['orchestrator', 'analytics', 'smart', 'wikipedia'],
    max_active_runs=1,  # Only one run at a time
) as dag:

    # Task 1: Check data completeness and branch
    check_completeness = BranchPythonOperator(
        task_id='check_completeness',
        python_callable=check_data_completeness,
    )

    # Task 2a: Trigger missing data fetches (if needed)
    trigger_fetches = PythonOperator(
        task_id='trigger_missing_data_fetches',
        python_callable=trigger_missing_data_fetches,
    )

    # Task 2b: Wait for data to be ready
    wait_sensor = PythonSensor(
        task_id='wait_for_data_ready',
        python_callable=wait_for_data_ready,
        poke_interval=60,  # Check every 60 seconds
        timeout=7200,  # 2 hours timeout
        mode='poke',
        trigger_rule=TriggerRule.NONE_FAILED,  # Run even if branch skipped
    )

    # Task 3: Load timeseries to PostgreSQL
    load_timeseries = PythonOperator(
        task_id='load_timeseries',
        python_callable=load_timeseries_to_postgres,
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # Task 4: Find common articles
    find_common = PythonOperator(
        task_id='find_common_articles',
        python_callable=find_common_articles,
    )

    # Task 5: Save to historical-data bucket
    save_historical = PythonOperator(
        task_id='save_to_historical_bucket',
        python_callable=save_to_historical_bucket,
    )

    # Define task dependencies
    # Branch path 1: completeness check -> load timeseries (if all data available)
    # Branch path 2: completeness check -> trigger fetches -> wait -> load timeseries (if missing data)
    # Then: load timeseries -> find common articles -> save to historical bucket
    
    check_completeness >> [trigger_fetches, load_timeseries]
    trigger_fetches >> wait_sensor >> load_timeseries
    load_timeseries >> find_common >> save_historical
