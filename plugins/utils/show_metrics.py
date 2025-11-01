"""
Show comprehensive pipeline metrics and statistics
"""
from datetime import datetime
from plugins.config import BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET, DIAMOND_BUCKET, create_s3_client


def show_pipeline_metrics(**context):
    """Show comprehensive pipeline metrics and statistics"""
    ti = context['ti']
    dag_run = context['dag_run']
    
    print("\n" + "=" * 80)
    print("📊 WIKI TRENDING PIPELINE - COMPREHENSIVE METRICS REPORT")
    print("=" * 80)
    
    # DAG Run Information
    print("\n🔄 DAG RUN INFORMATION")
    print("-" * 80)
    print(f"  DAG ID:           {dag_run.dag_id}")
    print(f"  Run ID:           {dag_run.run_id}")
    print(f"  Execution Date:   {dag_run.execution_date}")
    print(f"  Start Date:       {dag_run.start_date}")
    print(f"  State:            {dag_run.state}")
    
    # Calculate total duration
    total_duration = 0
    if dag_run.start_date:
        current_time = datetime.now(dag_run.start_date.tzinfo)
        total_duration = (current_time - dag_run.start_date).total_seconds()
        print(f"  Duration So Far:  {total_duration:.2f}s ({total_duration/60:.2f}m)")
    
    # Task-level metrics
    print("\n📋 TASK EXECUTION METRICS")
    print("-" * 80)
    
    task_ids = [
        'fetch_top_pages',
        'fetch_wikipedia_categories', 
        'check_spark_cluster',
        'clean_top_pages',
        'categorize_pages',
        'clean_gold_layer',
        'load_gold_to_postgres'
    ]
    
    total_task_time = 0
    for task_id in task_ids:
        try:
            task_instance = context['task_instance'].get_dagrun().get_task_instance(task_id)
            if task_instance and task_instance.start_date and task_instance.end_date:
                duration = (task_instance.end_date - task_instance.start_date).total_seconds()
                total_task_time += duration
                print(f"  ✓ {task_id:30s} | {duration:6.2f}s | {task_instance.state}")
            else:
                print(f"  ⏳ {task_id:30s} | Running or Not Started")
        except Exception as e:
            print(f"  ⚠️  {task_id:30s} | Unable to fetch metrics")
    
    if total_task_time > 0:
        print(f"\n  Total Task Time: {total_task_time:.2f}s ({total_task_time/60:.2f}m)")
    
    # Data Layer Statistics
    print("\n💾 DATA LAYER STATISTICS")
    print("-" * 80)
    
    s3_client = create_s3_client()
    
    layers = [
        ("🥉 Bronze (Raw)", BRONZE_BUCKET),
        ("🥈 Silver (Cleaned)", SILVER_BUCKET),
        ("🥇 Gold (Categorized)", GOLD_BUCKET),
        ("💎 Diamond (Curated)", DIAMOND_BUCKET)
    ]
    
    for layer_name, bucket in layers:
        try:
            response = s3_client.list_objects_v2(Bucket=bucket)
            if 'Contents' in response:
                total_size = sum(obj['Size'] for obj in response['Contents'])
                file_count = len(response['Contents'])
                print(f"\n  {layer_name}: {bucket}")
                print(f"    Files: {file_count}")
                print(f"    Total Size: {total_size / 1024:.2f} KB ({total_size / (1024*1024):.2f} MB)")
                
                # Show first 3 files
                for obj in response['Contents'][:3]:
                    file_size = obj['Size'] / 1024
                    print(f"      • {obj['Key']} ({file_size:.2f} KB)")
                
                if file_count > 3:
                    print(f"      ... and {file_count - 3} more files")
            else:
                print(f"\n  {layer_name}: {bucket}")
                print(f"    No files found")
        except Exception as e:
            print(f"\n  {layer_name}: {bucket}")
            print(f"    ⚠️  Error: {str(e)}")
    
    # Data Processing Metrics (from XCom)
    print("\n📈 DATA PROCESSING METRICS")
    print("-" * 80)
    
    try:
        gold_metadata = ti.xcom_pull(task_ids='clean_gold_layer')
        if gold_metadata:
            print(f"  Total Pages Processed:     {gold_metadata.get('total_pages', 'N/A')}")
            print(f"  Pages with Location:       {gold_metadata.get('pages_with_location', 'N/A')}")
            print(f"  Pages with Coordinates:    {gold_metadata.get('pages_with_coordinates', 'N/A')}")
            
            if gold_metadata.get('total_pages', 0) > 0:
                location_pct = (gold_metadata.get('pages_with_location', 0) / gold_metadata['total_pages']) * 100
                coord_pct = (gold_metadata.get('pages_with_coordinates', 0) / gold_metadata['total_pages']) * 100
                print(f"  Location Coverage:         {location_pct:.1f}%")
                print(f"  Geocoding Coverage:        {coord_pct:.1f}%")
    except Exception as e:
        print(f"  ⚠️  Unable to fetch processing metrics: {str(e)}")
    
    try:
        categorize_metadata = ti.xcom_pull(task_ids='categorize_pages')
        if categorize_metadata:
            print(f"\n  Category Distribution:")
            for category, count in categorize_metadata.items():
                if category != 'total':
                    print(f"    • {category:20s}: {count}")
    except Exception as e:
        print(f"  ⚠️  Unable to fetch categorization metrics: {str(e)}")
    
    # Performance Summary
    print("\n⚡ PERFORMANCE SUMMARY")
    print("-" * 80)
    
    try:
        gold_metadata = ti.xcom_pull(task_ids='clean_gold_layer')
        if total_task_time > 0 and gold_metadata and gold_metadata.get('total_pages', 0) > 0:
            pages_per_second = gold_metadata['total_pages'] / total_task_time
            print(f"  Throughput:           {pages_per_second:.2f} pages/second")
            print(f"  Avg Time per Page:    {total_task_time / gold_metadata['total_pages']:.2f}s")
    except Exception as e:
        print(f"  ⚠️  Unable to calculate performance metrics")
    
    # System Health
    print("\n🏥 SYSTEM HEALTH CHECK")
    print("-" * 80)
    
    try:
        # Check Spark
        spark_status = ti.xcom_pull(task_ids='check_spark_cluster')
        print(f"  Spark Cluster:    {'✅ Healthy' if spark_status else '❌ Unavailable'}")
    except:
        print(f"  Spark Cluster:    ⚠️  Unknown")
    
    try:
        # Check MinIO
        s3_client.list_buckets()
        print(f"  MinIO Storage:    ✅ Healthy")
    except:
        print(f"  MinIO Storage:    ❌ Unavailable")
    
    try:
        # Check PostgreSQL
        postgres_metadata = ti.xcom_pull(task_ids='load_gold_to_postgres')
        if postgres_metadata:
            print(f"  PostgreSQL:       ✅ Healthy ({postgres_metadata.get('rows_inserted', 'N/A')} rows inserted)")
        else:
            print(f"  PostgreSQL:       ⚠️  Unknown")
    except:
        print(f"  PostgreSQL:       ⚠️  Unknown")
    
    print("\n" + "=" * 80)
    print("✅ PIPELINE EXECUTION COMPLETE")
    print("=" * 80 + "\n")
    
    return {
        "status": "SUCCESS",
        "total_duration": total_duration,
        "total_task_time": total_task_time,
        "timestamp": datetime.now().isoformat()
    }
