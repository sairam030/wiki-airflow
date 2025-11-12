"""
Final cleaning layer using Spark before Gold layer
Runs after categorization to clean and validate enriched data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, regexp_replace, lit, udf
from pyspark.sql.types import DoubleType, StructType, StructField, StringType
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    GOLD_BUCKET,
    DIAMOND_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    create_s3_client,
    ensure_bucket_exists
)

from utils.geocode import get_coordinates, KNOWN_LOCATIONS


def clean_gold_layer(**context):
    """
    Final cleaning with Spark - reads from Gold, writes to Diamond:
    - Remove duplicates
    - Validate categories
    - Clean geography fields
    - Validate data quality
    - Format text fields
    - Upload to Diamond bucket (final curated data)
    """
    # Ensure Diamond bucket exists
    print("💎 Ensuring Diamond bucket exists...")
    ensure_bucket_exists(DIAMOND_BUCKET)
    
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
        
        # Read CSV with proper quote handling
        df = spark.read.csv(
            input_path, 
            header=True, 
            inferSchema=True,
            quote='"',           # Handle quoted fields
            escape='"',          # Handle escaped quotes
            multiLine=True       # Handle multi-line fields
        )
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

        # 3.1 remove page name 
        
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
        
        # ===== ADD GEOCODING (Latitude & Longitude) =====
        
        print("\n🌍 Adding geocoding (latitude & longitude)...")
        
        # Collect unique locations that need geocoding
        unique_locations = df.filter(
            (col('location') != 'No specific location') & 
            (col('location').isNotNull())
        ).select('location', 'location_type').distinct().collect()
        
        print(f"   • Found {len(unique_locations)} unique locations to geocode")
        
        # Build a mapping of location -> (lat, lon) using the cache
        location_coords = {}
        cached = 0
        need_geocoding_list = []
        
        for row in unique_locations:
            location = row['location']
            loc_type = row['location_type']
            
            # Check if in known locations cache
            if location in KNOWN_LOCATIONS:
                location_coords[location] = KNOWN_LOCATIONS[location]
                cached += 1
            else:
                # Add to list for geocoding
                need_geocoding_list.append((location, loc_type))
        
        print(f"   • Cached coordinates: {cached}")
        print(f"   • Need geocoding: {len(need_geocoding_list)}")
        
        # Geocode remaining locations
        if need_geocoding_list:
            print(f"\n🌍 Fetching coordinates for {len(need_geocoding_list)} locations...")
            print(f"   ⏱️  Estimated time: ~{len(need_geocoding_list)} seconds (1 req/sec rate limit)")
            print(f"   📍 Using Nominatim (OpenStreetMap) geocoding API")
            
            # Import geocoding function
            from utils.geocode import batch_geocode_locations
            
            # Batch geocode
            geocoded_results = batch_geocode_locations(need_geocoding_list)
            location_coords.update(geocoded_results)
        
        # Create broadcast variable for efficient lookup
        from pyspark.sql.functions import broadcast
        
        # Convert to Spark DataFrame for joining
        coords_data = [(loc, lat, lon) for loc, (lat, lon) in location_coords.items()]
        coords_schema = StructType([
            StructField("location", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType())
        ])
        coords_df = spark.createDataFrame(coords_data, schema=coords_schema)
        
        # Left join to add coordinates
        df = df.join(broadcast(coords_df), on='location', how='left')
        
        # For locations without coordinates, set to NULL
        df = df.withColumn('latitude', when(col('latitude').isNull(), lit(None)).otherwise(col('latitude'))) \
               .withColumn('longitude', when(col('longitude').isNull(), lit(None)).otherwise(col('longitude')))
        
        geocoded_count = df.filter(col('latitude').isNotNull()).count()
        print(f"\n✅ Geocoding complete!")
        print(f"   • Pages with coordinates: {geocoded_count} ({geocoded_count/final_count*100:.1f}%)")
        
        # ===== REORDER COLUMNS FOR BETTER READABILITY =====
        
        # Rename columns to match PostgreSQL schema with lat/lon
        final_df = df.select(
            'page',
            'page_display',
            'views',
            'rank',
            'link',
            'category',
            col('location_type'),
            col('location'),
            col('latitude'),
            col('longitude')
        ).orderBy(col('rank'))
        
        # Show final sample
        print("\n📋 Sample of cleaned data (ready for Diamond):")
        final_df.show(10, truncate=False)
        
        # ===== WRITE TO DIAMOND BUCKET (Final Curated Data) =====


        
        final_output_filename = output_filename.replace('categorized_', 'diamond_final_')
        output_path = f"s3a://{DIAMOND_BUCKET}/{final_output_filename}"
        
        print(f"\n� Writing cleaned data to Diamond bucket: {output_path}")
        
        # Write as single CSV file
        final_df.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"✅ Successfully wrote {final_count} records to Diamond bucket")
        print(f"💎 Diamond bucket contains the final curated data!")
        
        # Count pages with coordinates for metrics
        pages_with_coordinates = final_df.filter(
            (col('latitude').isNotNull()) & 
            (col('longitude').isNotNull())
        ).count()
        
        print(f"📍 Geocoding summary:")
        print(f"   • Pages with coordinates: {pages_with_coordinates} ({pages_with_coordinates/final_count*100:.1f}%)")
        
        # Return metadata for next task (compatible with verify_pipeline)
        return {
            'output_file': final_output_filename,
            'total_pages': final_count,
            'records_removed': removed_count,
            'pages_with_location': pages_with_location,
            'pages_with_coordinates': pages_with_coordinates
        }
        
    finally:
        print("🛑 Stopping Spark session...")
        spark.stop()
