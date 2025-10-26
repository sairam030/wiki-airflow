"""
Final cleaning layer using Spark before Gold layer
Runs after categorization to clean and validate enriched data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, regexp_replace
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    GOLD_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)


def clean_gold_layer(**context):
    """
    Final cleaning with Spark before Gold layer:
    - Remove duplicates
    - Validate categories
    - Clean geography fields
    - Validate data quality
    - Format text fields
    """
    # Get filename from previous task
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='categorize_pages')
    
    if not metadata:
        raise ValueError("No metadata from categorize_pages task")
    
    output_filename = metadata['output_file']
    print(f"🧹 Final cleaning of: {output_filename}")
    
    # Create Spark session connected to CLUSTER
    print("🚀 Connecting to Spark cluster for final cleaning...")
    spark = SparkSession.builder \
        .appName("CleanGoldLayer") \
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
        
        # Read categorized data from Gold bucket
        input_path = f"s3a://{GOLD_BUCKET}/{output_filename}"
        print(f"📖 Reading categorized data from: {input_path}")
        
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        initial_count = df.count()
        print(f"✓ Loaded {initial_count} records")
        
        # Show initial state
        print("\n📋 Sample of data before cleaning:")
        df.show(5, truncate=False)
        
        # ===== DATA QUALITY CHECKS & CLEANING =====
        
        print("\n🧹 Applying cleaning transformations...")
        
        # 1. Remove duplicates based on page name
        df = df.dropDuplicates(['page'])
        
        # 2. Trim whitespace from text fields
        df = df.withColumn('page', trim(col('page'))) \
               .withColumn('category', trim(col('category'))) \
               .withColumn('location', trim(col('location'))) \
               .withColumn('location_type', trim(col('location_type')))
        
        # 3. Clean page names (remove underscores for better readability)
        df = df.withColumn('page_display', regexp_replace(col('page'), '_', ' '))
        
        # 4. Validate views and rank (must be positive)
        df = df.filter((col('views') > 0) & (col('rank') > 0))
        
        # 5. Handle missing/null geography - replace with "No specific location"
        df = df.withColumn(
            'location',
            when(
                (col('location').isNull()) | (col('location') == '') | (col('location') == 'null'),
                'No specific location'
            ).otherwise(col('location'))
        )
        
        df = df.withColumn(
            'location_type',
            when(
                (col('location_type').isNull()) | (col('location_type') == '') | (col('location_type') == 'null'),
                'General'
            ).otherwise(col('location_type'))
        )
        
        # 6. Standardize category names (title case)
        # Categories are already standardized from the LLM, but ensure consistency
        df = df.withColumn('category', 
            when(col('category').isNull(), 'Other').otherwise(col('category'))
        )
        
        # 7. Ensure link is properly formatted
        df = df.withColumn(
            'link',
            when(
                col('link').isNull(),
                regexp_replace(col('page'), ' ', '_')
            ).otherwise(col('link'))
        )
        
        # ===== VALIDATION & STATISTICS =====
        
        final_count = df.count()
        removed_count = initial_count - final_count
        
        print(f"\n📊 Cleaning Statistics:")
        print(f"   • Initial records: {initial_count}")
        print(f"   • Final records: {final_count}")
        print(f"   • Removed duplicates/invalid: {removed_count}")
        
        # Show data quality metrics
        print("\n📈 Data Quality Metrics:")
        
        # Count pages with location
        pages_with_location = df.filter(
            (col('location') != 'No specific location') & 
            (col('location').isNotNull())
        ).count()
        
        print(f"   • Pages with location: {pages_with_location} ({pages_with_location/final_count*100:.1f}%)")
        
        # Category distribution
        print("\n📊 Category Distribution:")
        category_dist = df.groupBy('category').count().orderBy(col('count').desc())
        category_dist.show(20, truncate=False)
        
        # Geography type distribution
        print("\n🌍 Geography Type Distribution:")
        geo_dist = df.groupBy('location_type').count().orderBy(col('count').desc())
        geo_dist.show(10, truncate=False)
        
        # ===== REORDER COLUMNS FOR BETTER READABILITY =====
        
        # Rename columns to match PostgreSQL schema
        final_df = df.select(
            'page',
            'page_display',
            'views',
            'rank',
            'link',
            'category',
            col('location_type'),
            col('location')
        ).orderBy(col('rank'))
        
        # Show final sample
        print("\n📋 Sample of cleaned data (ready for Gold):")
        final_df.show(10, truncate=False)
        
        # ===== WRITE TO GOLD BUCKET =====
        
        final_output_filename = output_filename.replace('categorized_', 'gold_final_')
        output_path = f"s3a://{GOLD_BUCKET}/{final_output_filename}"
        
        print(f"\n💾 Writing cleaned data to: {output_path}")
        
        # Write as single CSV file
        final_df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"✅ Successfully wrote {final_count} records to Gold bucket")
        
        # Return metadata for next task (compatible with verify_pipeline)
        return {
            'output_file': final_output_filename,
            'total_pages': final_count,
            'records_removed': removed_count,
            'pages_with_location': pages_with_location
        }
        
    finally:
        print("🛑 Stopping Spark session...")
        spark.stop()
