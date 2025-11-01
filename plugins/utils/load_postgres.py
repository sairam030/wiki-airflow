import pandas as pd
import boto3
from io import StringIO
import psycopg2
from plugins.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, SILVER_BUCKET, BRONZE_BUCKET, DIAMOND_BUCKET

def load_gold_to_postgres(**context):
    """
    Load enriched data from Diamond bucket (final curated data) into Postgres for BI
    """
    # Get filename from previous task
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='clean_gold_layer')
    
    if metadata:
        diamond_filename = metadata['output_file']
        print(f"📥 Loading specific file from Diamond: {diamond_filename}")
    else:
        print("⚠️  No metadata from clean_gold_layer, using latest file")
        diamond_filename = None
    
    # 1. Create S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    # 2. Get the cleaned CSV file from Diamond bucket
    if diamond_filename:
        # List files in the Spark output directory
        response = s3_client.list_objects_v2(Bucket=DIAMOND_BUCKET, Prefix=diamond_filename)
        if "Contents" not in response:
            raise Exception(f"No files found in Diamond bucket with prefix: {diamond_filename}")
        
        # Find the actual CSV file (not _SUCCESS)
        csv_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.csv') and not obj['Key'].endswith('/_SUCCESS')
        ]
        
        if not csv_files:
            raise Exception(f"No CSV files found in {DIAMOND_BUCKET}/{diamond_filename}")
        
        latest_file = csv_files[0]
    else:
        # Fallback to latest file
        response = s3_client.list_objects_v2(Bucket=DIAMOND_BUCKET)
        if "Contents" not in response:
            raise Exception("No files found in Diamond bucket")
        latest_file = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

    print(f"💎 Loading file from Diamond: {latest_file}")

    obj = s3_client.get_object(Bucket=DIAMOND_BUCKET, Key=latest_file)
    df = pd.read_csv(obj['Body'])

    print(f"✓ Loaded {len(df)} records from Diamond bucket")
    print(f"✓ Columns: {list(df.columns)}")

    # 3. Connect to Postgres
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()

    # 4. Drop and recreate table to ensure schema matches
    print("🗑️  Dropping old table if exists...")
    cursor.execute("DROP TABLE IF EXISTS wiki_diamond;")
    conn.commit()
    
    print("🏗️  Creating new table with latitude/longitude columns...")
    create_table_query = """
    CREATE TABLE wiki_diamond (
        page TEXT,
        page_display TEXT,
        views INT,
        rank INT,
        link TEXT,
        category TEXT,
        geography_type TEXT,
        geography TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    print("✓ Table created successfully")

    # 5. Insert data
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO wiki_diamond (page, page_display, views, rank, link, category, geography_type, geography, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (
                row.get("page"), 
                row.get("page_display", row.get("page", "").replace("_", " ")),  # Fallback if page_display missing
                row.get("views"), 
                row.get("rank"), 
                row.get("link"), 
                row.get("category"), 
                row.get("location_type"), 
                row.get("location"),
                row.get("latitude"),
                row.get("longitude")
            )
        )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Successfully loaded data into Postgres table 'wiki_diamond' (final curated data)")
