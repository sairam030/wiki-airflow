"""
Wikipedia Trending Pages Pipeline
Bronze -> Silver -> Gold architecture
"""

from datetime import datetime, timedelta
from pydoc import doc
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import from plugins (NO "plugins." prefix in imports!)
from utils.load_postgres import load_gold_to_postgres
from utils.fetch_top_pages import fetch_top_pages
from utils.clean_top_pages import check_spark_cluster, clean_pages
from utils.categorize_pages import check_ollama, categorize_pages
from utils.clean_gold_layer import clean_gold_layer
from config import BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET, create_s3_client


def verify_pipeline(**context):
    """Verify pipeline completion"""
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='clean_gold_layer')
    
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

    # task 1 fetch from wiki api
    fetch = PythonOperator(
        task_id='fetch_top_pages',
        python_callable=fetch_top_pages,
        doc_md="""
        ### Fetch Top Wikipedia Pages
        - Fetches top trending Wikipedia pages
        - Stores raw data in MinIO Bronze bucket
        """
    )

    #task 2 check spark cluster
    check_spark = PythonOperator(
        task_id='check_spark_cluster',
        python_callable=check_spark_cluster,
        doc_md="""
        ### Check Spark Cluster
        - Verifies if the Spark cluster is up and running
        """
    )

    # task 3 clean top pages
    clean = PythonOperator(
        task_id='clean_top_pages',
        python_callable=clean_pages,
        doc_md="""
        ### Clean Top Pages
        - Cleans and preprocesses the fetched Wikipedia pages
        - Stores cleaned data in MinIO Silver bucket
        """
    )

    # task 4 check ollama llm
    check_llm = PythonOperator(
        task_id='check_ollama',
        python_callable=check_ollama,
        doc_md="""
        ### Check Ollama LLM
        - Verifies if the Ollama LLM is up and running
        """
    )

    # task 5 categorize pages with llm
    categorize = PythonOperator(
        task_id='categorize_pages',
        python_callable=categorize_pages,
        doc_md="""
        ### Categorize Pages
        - Categorizes the cleaned Wikipedia pages using the Ollama LLM
        - Stores categorized data in MinIO Gold bucket
        """
    )

    
    # task 5 verify pipeline
    verify = PythonOperator(
        task_id='verify_pipeline',
        python_callable=verify_pipeline,
        doc_md="""
        ### Verify Pipeline
        - Verifies the completion and integrity of the entire pipeline
        """
    )

    # task 6 clean gold layer
    clean_gold_layer = PythonOperator(
        task_id='clean_gold_layer',
        python_callable=clean_gold_layer,
        doc_md="""
        ### Clean Gold Layer
        - Final cleaning of Gold layer data using Spark cluster
        """
    )

    # task 7 load gold data to postgres 
    load_gold_task = PythonOperator(
    task_id='load_gold_to_postgres',
    python_callable=load_gold_to_postgres,
    doc_md="""
    ### Load Gold Data to Postgres
    - Reads enriched Gold CSV from MinIO
    - Loads into Postgres for BI & Metabase
    """
    )




    fetch >>  clean >> categorize >> clean_gold_layer >> load_gold_task >> verify
    check_spark >> clean
    check_llm >> categorize