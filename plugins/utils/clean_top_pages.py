"""
Task to clean Wikipedia data using Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col , concat, lit
import time
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    BRONZE_BUCKET,
    SILVER_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    ensure_bucket_exists
)


def check_spark_cluster(**context):
    """
    Check if Spark cluster is accessible
    """
    print("🔍 Checking Spark cluster connectivity...")
    
    max_retries = 5
    retry_delay = 10
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempt {attempt}/{max_retries} - Connecting to Spark cluster...")
            
            spark = SparkSession.builder \
                .appName("SparkHealthCheck") \
                .master("spark://spark-master:7077") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "1g") \
                .config("spark.driver.host", "airflow-scheduler") \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .config("spark.network.timeout", "300s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .config("spark.rpc.askTimeout", "300s") \
                .getOrCreate()
            
            # Simple test
            print("Running test computation...")
            test_rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 2)
            result = test_rdd.sum()
            
            print(f"✅ Spark cluster is healthy!")
            print(f"   • Spark Version: {spark.version}")
            print(f"   • Master: {spark.sparkContext.master}")
            print(f"   • Application ID: {spark.sparkContext.applicationId}")
            print(f"   • Test Result: Sum(1-5) = {result}")
            
            spark.stop()
            return "Spark cluster is ready"
            
        except Exception as e:
            print(f"❌ Attempt {attempt} failed: {str(e)}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to connect to Spark cluster after {max_retries} attempts")


def clean_pages(**context):
    """
    Read CSV from Bronze, merge with Wikipedia categories if available, 
    clean with Spark CLUSTER, write to Silver
    """
    # Get filename from previous task
    ti = context['ti']
    filename = ti.xcom_pull(task_ids='fetch_top_pages')
    
    if not filename:
        raise ValueError("No filename received from fetch_top_pages task")
    
    print(f"📊 Processing file: {filename}")
    
    # Extract date from filename to construct categories filename
    # filename format: top_pages_YYYY_MM_DD.csv
    date_part = filename.replace('top_pages_', '').replace('.csv', '')
    categories_filename = f"wiki_categories_{date_part}.csv"
    
    # Ensure Silver bucket exists
    ensure_bucket_exists(SILVER_BUCKET)
    
    # Create Spark session connected to CLUSTER
    print("🚀 Connecting to Spark cluster...")
    spark = SparkSession.builder \
        .appName("CleanWikiData") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.host", "airflow-scheduler") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.rpc.askTimeout", "300s") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    try:
        print(f"✅ Connected to Spark cluster!")
        print(f"   • Application ID: {spark.sparkContext.applicationId}")
        print(f"   • Master: {spark.sparkContext.master}")
        
        # Read CSV from Bronze bucket
        input_path = f"s3a://{BRONZE_BUCKET}/{filename}"
        print(f"📖 Reading pages from: {input_path}")
        
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        print(f"✓ Loaded {df.count()} records from Bronze bucket")
        
        # Try to read Wikipedia categories (may not exist yet if parallel DAG is still running)
        categories_path = f"s3a://{BRONZE_BUCKET}/{categories_filename}"
        print(f"📖 Checking for categories: {categories_path}")
        
        try:
            categories_df = spark.read.csv(categories_path, header=True, inferSchema=True)
            print(f"✓ Found categories file with {categories_df.count()} records")
            
            # Merge categories with main data
            df = df.join(
                categories_df.select("page", "wiki_categories"),
                df.article == categories_df.page,
                "left"
            ).drop("page")  # Drop the duplicate page column from join
            
            print("✓ Merged Wikipedia categories")
        except Exception as e:
            print(f"⚠️  Categories file not found (parallel DAG may still be running): {str(e)}")
            print("   → Proceeding without categories, they can be added later")
            # Add empty wiki_categories column
            df = df.withColumn("wiki_categories", lit(""))
        
        # Clean and transform data
        print("🧹 Cleaning data on Spark cluster...")
        
        # add a column called link with the full Wikipedia URL
        df = df.withColumn("link", concat(lit("https://en.wikipedia.org/wiki/"), col("article")))

        cleaned_df = df.filter(
            (col("views") > 0) & 
            (col("article") != "Main_Page") &
            (~col("article").startswith("Special:"))
        ).select(
            col("article").alias("page"),
            col("views"),
            col("rank"),
            col("link"),
            col("wiki_categories")  # Keep Wikipedia categories (empty if not available)
        ).orderBy(col("views").desc())
        
        # Show sample
        print("📋 Sample of cleaned data:")
        cleaned_df.show(10, truncate=False)
        
        record_count = cleaned_df.count()
        print(f"✓ Cleaned {record_count} records using Spark cluster")
        
        # Write to Silver bucket as CSV
        output_filename = filename.replace("top_pages_", "cleaned_")
        output_path = f"s3a://{SILVER_BUCKET}/{output_filename}"
        
        print(f"💾 Writing CSV to: {output_path}")
        
        # Write as single CSV file with proper escaping/quoting
        cleaned_df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("quoteAll", "true") \
            .csv(output_path)
        
        print(f"✅ Successfully wrote cleaned data to Silver bucket")
        
        # Return metadata
        return {
            'input_file': filename,
            'output_file': output_filename,
            'record_count': record_count
        }
        
    finally:
        print("🛑 Stopping Spark session...")
        spark.stop()