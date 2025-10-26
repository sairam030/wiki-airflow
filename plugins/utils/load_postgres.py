import pandas as pd
import boto3
from io import StringIO
import psycopg2
from plugins.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, SILVER_BUCKET, BRONZE_BUCKET

# Gold bucket name
GOLD_BUCKET = "gold"

def load_gold_to_postgres(**context):
    """
    Load enriched Gold data from MinIO into Postgres for BI
    """
    # Get filename from previous task
    ti = context['ti']
    metadata = ti.xcom_pull(task_ids='clean_gold_layer')
    
    if metadata:
        gold_filename = metadata['output_file']
        print(f"📥 Loading specific file from Gold: {gold_filename}")
    else:
        print("⚠️  No metadata from clean_gold_layer, using latest file")
        gold_filename = None
    
    # 1. Create S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1"
    )

    # 2. Get the cleaned CSV file from Gold bucket
    if gold_filename:
        # List files in the Spark output directory
        response = s3_client.list_objects_v2(Bucket=GOLD_BUCKET, Prefix=gold_filename)
        if "Contents" not in response:
            raise Exception(f"No files found in Gold bucket with prefix: {gold_filename}")
        
        # Find the actual CSV file (not _SUCCESS)
        csv_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.csv') and not obj['Key'].endswith('/_SUCCESS')
        ]
        
        if not csv_files:
            raise Exception(f"No CSV files found in {GOLD_BUCKET}/{gold_filename}")
        
        latest_file = csv_files[0]
    else:
        # Fallback to latest file
        response = s3_client.list_objects_v2(Bucket=GOLD_BUCKET)
        if "Contents" not in response:
            raise Exception("No files found in Gold bucket")
        latest_file = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

    print(f"📥 Loading file from Gold: {latest_file}")

    obj = s3_client.get_object(Bucket=GOLD_BUCKET, Key=latest_file)
    df = pd.read_csv(obj['Body'])

    print(f"✓ Loaded {len(df)} records from Gold bucket")
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

    # 4. Create table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS wiki_gold (
        page TEXT,
        page_display TEXT,
        views INT,
        rank INT,
        link TEXT,
        category TEXT,
        geography_type TEXT,
        geography TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # 5. Insert data
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO wiki_gold (page, page_display, views, rank, link, category, geography_type, geography) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (
                row.get("page"), 
                row.get("page_display", row.get("page", "").replace("_", " ")),  # Fallback if page_display missing
                row.get("views"), 
                row.get("rank"), 
                row.get("link"), 
                row.get("category"), 
                row.get("location_type"), 
                row.get("location")  
            )
        )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Successfully loaded data into Postgres table 'wiki_gold'")
