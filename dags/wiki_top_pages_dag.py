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
from utils.fetch_wiki_categories import fetch_wikipedia_categories_for_pages
from utils.clean_top_pages import check_spark_cluster, clean_pages
from utils.categorize_pages import check_ollama, categorize_pages
from utils.clean_gold_layer import clean_gold_layer
from utils.show_metrics import show_pipeline_metrics


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='wiki_trending_pipeline',
    default_args=default_args,
    description='Wikipedia ETL with ML categorization - Bronze → Silver → Gold → Diamond',
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

    # task 1.5 fetch wiki categories in parallel
    fetch_categories = PythonOperator(
        task_id='fetch_wikipedia_categories',
        python_callable=fetch_wikipedia_categories_for_pages,
        doc_md="""
        ### Fetch Wikipedia Categories
        - Reads page list from Bronze bucket
        - Fetches Wikipedia categories sequentially
        - Saves categories to separate CSV file
        - Runs in parallel with main pipeline
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
    # check_llm = PythonOperator(
    #     task_id='check_ollama',
    #     python_callable=check_ollama,
    #     doc_md="""
    #     ### Check Ollama LLM
    #     - Verifies if the Ollama LLM is up and running
    #     """
    # )

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

    
    # task 6 show comprehensive metrics
    show_metrics = PythonOperator(
        task_id='show_pipeline_metrics',
        python_callable=show_pipeline_metrics,
        doc_md="""
        ### Show Pipeline Metrics
        - Displays comprehensive metrics for all tasks
        - Shows data layer statistics (Bronze, Silver, Gold, Diamond)
        - Reports processing metrics and performance
        - System health check
        """
    )

    # task 7 clean gold layer
    clean_gold_layer = PythonOperator(
        task_id='clean_gold_layer',
        python_callable=clean_gold_layer,
        doc_md="""
        ### Clean Gold Layer
        - Final cleaning of Gold layer data using Spark cluster
        - Writes curated data to Diamond bucket
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



    # Dependencies - CORRECTED WORKFLOW
    # Step 1: Fetch pages first
    fetch
    
    # Step 2: Fetch categories (needs pages from Bronze)
    fetch >> fetch_categories
    
    # Step 3: Clean runs in parallel with fetch_categories
    # (both need fetch to complete first)
    check_spark >> clean

    fetch >> clean
    
    ## use this for ollama approach
    #check_llm >> categorize

    # Step 4: Categorize waits for BOTH categories AND cleaned data
    [fetch_categories, clean] >> categorize
    
    # Step 5: Rest of pipeline
    categorize >> clean_gold_layer >> load_gold_task >> show_metrics