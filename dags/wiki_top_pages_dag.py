"""
Wikipedia Trending Pages Pipeline
Bronze -> Silver -> Gold architecture
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import from plugins (NO "plugins." prefix in imports!)
from utils.fetch_top_pages import fetch_top_pages
from utils.clean_top_pages import check_spark_cluster, clean_pages
from utils.categorize_pages import check_ollama, categorize_pages
from config import BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET, create_s3_client


def verify_pipeline(**context):
    """Verify pipeline completion"""
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='categorize_pages')
    
    s3_client = create_s3_client()
    
    print("=" * 70)
    print("📊 PIPELINE VERIFICATION")
    print("=" * 70)
    
    # Bronze
    print(f"\n🥉 Bronze: {BRONZE_BUCKET}")
    bronze = s3_client.list_objects_v2(Bucket=BRONZE_BUCKET)
    if 'Contents' in bronze:
        for obj in bronze['Contents']:
            print(f"  • {obj['Key']}")
    
    # Silver
    print(f"\n🥈 Silver: {SILVER_BUCKET}")
    silver = s3_client.list_objects_v2(Bucket=SILVER_BUCKET)
    if 'Contents' in silver:
        for obj in silver['Contents'][:3]:
            print(f"  • {obj['Key']}")
    
    # Gold
    print(f"\n🥇 Gold: {GOLD_BUCKET}")
    gold = s3_client.list_objects_v2(Bucket=GOLD_BUCKET)
    if 'Contents' in gold:
        for obj in gold['Contents']:
            print(f"  • {obj['Key']}")
    
    print(f"\n✅ Total Pages: {metadata['total_pages']}")
    print(f"📍 With Location: {metadata['pages_with_location']}")
    print("=" * 70)
    
    return "SUCCESS"


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='wiki_trending_pipeline',
    default_args=default_args,
    description='Wikipedia ETL with ML categorization',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['wikipedia', 'spark', 'ml'],
) as dag:

    fetch = PythonOperator(task_id='fetch_top_pages', python_callable=fetch_top_pages)
    check_spark = PythonOperator(task_id='check_spark_cluster', python_callable=check_spark_cluster)
    clean = PythonOperator(task_id='clean_top_pages', python_callable=clean_pages)
    check_llm = PythonOperator(task_id='check_ollama', python_callable=check_ollama)
    categorize = PythonOperator(task_id='categorize_pages', python_callable=categorize_pages)
    verify = PythonOperator(task_id='verify_pipeline', python_callable=verify_pipeline)



    fetch >>  clean >> categorize >> verify
    check_spark >> clean
    check_llm >> categorize