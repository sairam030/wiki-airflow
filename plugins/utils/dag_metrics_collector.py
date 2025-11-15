"""
DAG Metrics Collector
Captures comprehensive metrics for pipeline performance tracking
Stores: latency, throughput, task durations, data volumes, success rates
"""

import psycopg2
from datetime import datetime
import json
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.utils.session import create_session


def collect_dag_metrics(dag_id, run_id, **context):
    """
    Collect comprehensive metrics for a DAG run
    
    Metrics captured:
    - Latency: Total pipeline duration, task durations
    - Throughput: Records processed per second, data volumes
    - Success Rate: Task success/failure counts
    - Data Quality: Record counts at each layer
    - Resource Usage: Task retry counts, execution times
    """
    
    print(f"\n📊 Collecting metrics for {dag_id} - {run_id}")
    
    dag_run = context['dag_run']
    ti = context['ti']
    
    # Get all task instances for this DAG run using create_session
    with create_session() as session:
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id
        ).all()
    
    # Calculate metrics
    metrics = {
        'dag_id': dag_id,
        'run_id': run_id,
        'execution_date': dag_run.execution_date.isoformat() if dag_run.execution_date else None,
        'start_date': dag_run.start_date.isoformat() if dag_run.start_date else None,
        'end_date': dag_run.end_date.isoformat() if dag_run.end_date else None,
        'state': str(dag_run.state),
        'collected_at': datetime.now().isoformat(),
    }
    
    # Calculate total duration
    if dag_run.start_date and dag_run.end_date:
        total_duration = (dag_run.end_date - dag_run.start_date).total_seconds()
        metrics['total_duration_seconds'] = total_duration
        metrics['total_duration_minutes'] = round(total_duration / 60, 2)
    else:
        metrics['total_duration_seconds'] = None
        metrics['total_duration_minutes'] = None
    
    # Task-level metrics
    task_metrics = []
    total_tasks = len(task_instances)
    successful_tasks = 0
    failed_tasks = 0
    total_retries = 0
    
    for task in task_instances:
        task_duration = None
        if task.start_date and task.end_date:
            task_duration = (task.end_date - task.start_date).total_seconds()
        
        task_info = {
            'task_id': task.task_id,
            'state': str(task.state),
            'start_date': task.start_date.isoformat() if task.start_date else None,
            'end_date': task.end_date.isoformat() if task.end_date else None,
            'duration_seconds': task_duration,
            'try_number': task.try_number,
            'max_tries': task.max_tries,
        }
        
        task_metrics.append(task_info)
        
        if task.state == State.SUCCESS:
            successful_tasks += 1
        elif task.state == State.FAILED:
            failed_tasks += 1
        
        total_retries += (task.try_number - 1) if task.try_number > 1 else 0
    
    metrics['task_metrics'] = task_metrics
    metrics['total_tasks'] = total_tasks
    metrics['successful_tasks'] = successful_tasks
    metrics['failed_tasks'] = failed_tasks
    metrics['success_rate'] = round((successful_tasks / total_tasks * 100), 2) if total_tasks > 0 else 0
    metrics['total_retries'] = total_retries
    
    # Try to get data volumes from XCom (if available)
    try:
        # Look for common XCom keys that might contain data volumes
        xcom_data = {}
        
        # Check for records processed
        for key in ['total_records', 'records_inserted', 'records_processed', 'row_count']:
            value = ti.xcom_pull(key=key)
            if value:
                xcom_data[key] = value
        
        metrics['data_volumes'] = xcom_data
        
    except Exception as e:
        print(f"Could not fetch XCom data: {e}")
        metrics['data_volumes'] = {}
    
    # Calculate throughput (if we have duration and record count)
    if metrics['total_duration_seconds'] and metrics['data_volumes'].get('total_records'):
        records = metrics['data_volumes']['total_records']
        duration = metrics['total_duration_seconds']
        metrics['throughput_records_per_second'] = round(records / duration, 2)
        metrics['throughput_records_per_minute'] = round(records / duration * 60, 2)
    else:
        metrics['throughput_records_per_second'] = None
        metrics['throughput_records_per_minute'] = None
    
    # Store metrics in PostgreSQL
    save_metrics_to_db(metrics)
    
    # Print summary
    print(f"\n✅ Metrics Collection Summary:")
    print(f"   • DAG: {dag_id}")
    print(f"   • Run ID: {run_id}")
    print(f"   • State: {metrics['state']}")
    print(f"   • Duration: {metrics['total_duration_minutes']} minutes")
    print(f"   • Tasks: {successful_tasks}/{total_tasks} successful ({metrics['success_rate']}%)")
    print(f"   • Retries: {total_retries}")
    
    if metrics['throughput_records_per_second']:
        print(f"   • Throughput: {metrics['throughput_records_per_second']:.2f} records/sec")
    
    return metrics


def save_metrics_to_db(metrics):
    """
    Save collected metrics to PostgreSQL for historical tracking
    """
    
    print("\n💾 Saving metrics to database...")
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    # Create metrics table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dag_execution_metrics (
            id SERIAL PRIMARY KEY,
            dag_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            execution_date TIMESTAMP,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            state TEXT,
            total_duration_seconds NUMERIC,
            total_duration_minutes NUMERIC,
            total_tasks INTEGER,
            successful_tasks INTEGER,
            failed_tasks INTEGER,
            success_rate NUMERIC,
            total_retries INTEGER,
            throughput_records_per_second NUMERIC,
            throughput_records_per_minute NUMERIC,
            data_volumes JSONB,
            task_metrics JSONB,
            collected_at TIMESTAMP,
            UNIQUE(dag_id, run_id)
        );
    """)
    
    # Create indexes for faster queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_dag_id 
        ON dag_execution_metrics(dag_id);
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_execution_date 
        ON dag_execution_metrics(execution_date);
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_metrics_collected_at 
        ON dag_execution_metrics(collected_at);
    """)
    
    conn.commit()
    
    # Insert metrics
    try:
        cursor.execute("""
            INSERT INTO dag_execution_metrics (
                dag_id, run_id, execution_date, start_date, end_date, state,
                total_duration_seconds, total_duration_minutes, 
                total_tasks, successful_tasks, failed_tasks, success_rate, total_retries,
                throughput_records_per_second, throughput_records_per_minute,
                data_volumes, task_metrics, collected_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (dag_id, run_id) 
            DO UPDATE SET
                end_date = EXCLUDED.end_date,
                state = EXCLUDED.state,
                total_duration_seconds = EXCLUDED.total_duration_seconds,
                total_duration_minutes = EXCLUDED.total_duration_minutes,
                successful_tasks = EXCLUDED.successful_tasks,
                failed_tasks = EXCLUDED.failed_tasks,
                success_rate = EXCLUDED.success_rate,
                throughput_records_per_second = EXCLUDED.throughput_records_per_second,
                throughput_records_per_minute = EXCLUDED.throughput_records_per_minute,
                task_metrics = EXCLUDED.task_metrics,
                collected_at = EXCLUDED.collected_at
        """, (
            metrics['dag_id'],
            metrics['run_id'],
            metrics['execution_date'],
            metrics['start_date'],
            metrics['end_date'],
            metrics['state'],
            metrics['total_duration_seconds'],
            metrics['total_duration_minutes'],
            metrics['total_tasks'],
            metrics['successful_tasks'],
            metrics['failed_tasks'],
            metrics['success_rate'],
            metrics['total_retries'],
            metrics['throughput_records_per_second'],
            metrics['throughput_records_per_minute'],
            json.dumps(metrics['data_volumes']),
            json.dumps(metrics['task_metrics']),
            metrics['collected_at']
        ))
        
        conn.commit()
        print("✅ Metrics saved successfully")
        
    except Exception as e:
        print(f"⚠️ Error saving metrics: {e}")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()


def get_dag_performance_comparison(dag_id, days=7):
    """
    Get performance comparison for last N days
    Useful for identifying trends and anomalies
    """
    
    print(f"\n📈 Fetching {days}-day performance comparison for {dag_id}...")
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    
    query = f"""
        SELECT 
            execution_date::date as run_date,
            COUNT(*) as total_runs,
            AVG(total_duration_minutes) as avg_duration_min,
            MAX(total_duration_minutes) as max_duration_min,
            MIN(total_duration_minutes) as min_duration_min,
            AVG(success_rate) as avg_success_rate,
            SUM(total_retries) as total_retries,
            AVG(throughput_records_per_second) as avg_throughput
        FROM dag_execution_metrics
        WHERE dag_id = %s
        AND execution_date >= NOW() - INTERVAL '{days} days'
        GROUP BY execution_date::date
        ORDER BY run_date DESC
    """
    
    import pandas as pd
    df = pd.read_sql(query, conn, params=(dag_id,))
    
    conn.close()
    
    print(f"\n📊 Performance Summary (Last {days} days):")
    print(df.to_string(index=False))
    
    return df


def get_task_performance_breakdown(dag_id, run_id):
    """
    Get detailed task-level performance for a specific run
    """
    
    print(f"\n🔍 Fetching task breakdown for {dag_id} - {run_id}...")
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT task_metrics 
        FROM dag_execution_metrics 
        WHERE dag_id = %s AND run_id = %s
    """, (dag_id, run_id))
    
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if result and result[0]:
        task_metrics = result[0]  # Already JSON
        
        print(f"\n📋 Task Performance:")
        print(f"{'Task ID':<30} {'Duration (s)':<15} {'State':<12} {'Tries':<8}")
        print("-" * 70)
        
        for task in task_metrics:
            duration = f"{task['duration_seconds']:.2f}" if task['duration_seconds'] else "N/A"
            print(f"{task['task_id']:<30} {duration:<15} {task['state']:<12} {task['try_number']:<8}")
        
        return task_metrics
    else:
        print("No metrics found for this run")
        return None
