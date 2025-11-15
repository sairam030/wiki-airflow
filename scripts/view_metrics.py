#!/usr/bin/env python3
"""
DAG Metrics CLI Tool
Quickly view and compare DAG execution metrics from the command line
"""

import argparse
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import sys


def get_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )


def show_recent_runs(dag_id=None, days=7):
    """Show recent DAG runs with key metrics"""
    
    conn = get_connection()
    
    where_clause = ""
    params = []
    
    if dag_id:
        where_clause = "WHERE dag_id = %s AND "
        params.append(dag_id)
    else:
        where_clause = "WHERE "
    
    query = f"""
        SELECT 
            dag_id,
            execution_date::date as run_date,
            state,
            ROUND(total_duration_minutes, 2) as duration_min,
            ROUND(success_rate, 2) as success_rate,
            total_retries as retries,
            ROUND(throughput_records_per_second, 2) as throughput_rps
        FROM dag_execution_metrics
        {where_clause}execution_date >= NOW() - INTERVAL '{days} days'
        ORDER BY execution_date DESC
        LIMIT 50
    """
    
    df = pd.read_sql(query, conn, params=params if params else None)
    conn.close()
    
    if df.empty:
        print(f"\n❌ No runs found for the last {days} days")
        return
    
    print(f"\n📊 Recent DAG Runs (Last {days} days)")
    print("=" * 100)
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
    
    return df


def compare_performance(dag_id, days=30):
    """Compare daily performance metrics"""
    
    conn = get_connection()
    
    query = f"""
        SELECT 
            execution_date::date as run_date,
            COUNT(*) as runs,
            ROUND(AVG(total_duration_minutes), 2) as avg_duration,
            ROUND(MIN(total_duration_minutes), 2) as min_duration,
            ROUND(MAX(total_duration_minutes), 2) as max_duration,
            ROUND(AVG(success_rate), 2) as avg_success_rate,
            SUM(total_retries) as retries,
            ROUND(AVG(throughput_records_per_second), 2) as avg_throughput
        FROM dag_execution_metrics
        WHERE dag_id = %s
        AND execution_date >= NOW() - INTERVAL '{days} days'
        GROUP BY execution_date::date
        ORDER BY run_date DESC
    """
    
    df = pd.read_sql(query, conn, params=(dag_id,))
    conn.close()
    
    if df.empty:
        print(f"\n❌ No data found for DAG: {dag_id}")
        return
    
    print(f"\n📈 Daily Performance Comparison - {dag_id}")
    print("=" * 120)
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
    
    # Calculate trends
    if len(df) > 1:
        latest = df.iloc[0]
        previous = df.iloc[1]
        
        print(f"\n📊 Latest vs Previous Day:")
        print(f"   Duration: {latest['avg_duration']} min (vs {previous['avg_duration']} min)")
        
        duration_change = ((latest['avg_duration'] - previous['avg_duration']) / previous['avg_duration'] * 100) if previous['avg_duration'] else 0
        
        if duration_change > 0:
            print(f"   Change: ⬆️ +{duration_change:.2f}% slower")
        elif duration_change < 0:
            print(f"   Change: ⬇️ {duration_change:.2f}% faster")
        else:
            print(f"   Change: ➡️ No change")
    
    return df


def show_task_breakdown(dag_id, run_id=None):
    """Show task-level performance breakdown"""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    if run_id:
        # Show specific run
        cursor.execute("""
            SELECT task_metrics 
            FROM dag_execution_metrics 
            WHERE dag_id = %s AND run_id = %s
        """, (dag_id, run_id))
    else:
        # Show latest run
        cursor.execute("""
            SELECT task_metrics 
            FROM dag_execution_metrics 
            WHERE dag_id = %s
            ORDER BY execution_date DESC
            LIMIT 1
        """, (dag_id,))
    
    result = cursor.fetchone()
    
    if not result or not result[0]:
        print(f"\n❌ No task metrics found for {dag_id}")
        cursor.close()
        conn.close()
        return
    
    task_metrics = result[0]  # JSONB data
    
    # Convert to DataFrame
    df = pd.DataFrame(task_metrics)
    
    # Select relevant columns
    if 'duration_seconds' in df.columns:
        df['duration_seconds'] = pd.to_numeric(df['duration_seconds'], errors='coerce')
        df = df.sort_values('duration_seconds', ascending=False)
    
    display_cols = ['task_id', 'state', 'duration_seconds', 'try_number']
    df_display = df[display_cols]
    
    print(f"\n🔍 Task Performance Breakdown - {dag_id}")
    print("=" * 80)
    print(tabulate(df_display, headers='keys', tablefmt='grid', showindex=False))
    
    # Summary
    total_duration = df['duration_seconds'].sum()
    print(f"\n📊 Summary:")
    print(f"   Total Tasks: {len(df)}")
    print(f"   Total Duration: {total_duration:.2f} seconds")
    print(f"   Slowest Task: {df.iloc[0]['task_id']} ({df.iloc[0]['duration_seconds']:.2f}s)")
    
    cursor.close()
    conn.close()
    
    return df


def show_slowest_tasks(days=7, limit=20):
    """Show slowest tasks across all DAGs"""
    
    conn = get_connection()
    
    query = f"""
        WITH task_performance AS (
            SELECT 
                dag_id,
                jsonb_array_elements(task_metrics)->>'task_id' as task_id,
                (jsonb_array_elements(task_metrics)->>'duration_seconds')::numeric as duration_sec
            FROM dag_execution_metrics
            WHERE execution_date >= NOW() - INTERVAL '{days} days'
            AND task_metrics IS NOT NULL
        )
        SELECT 
            task_id,
            dag_id,
            COUNT(*) as executions,
            ROUND(AVG(duration_sec), 2) as avg_duration_sec,
            ROUND(MAX(duration_sec), 2) as max_duration_sec,
            ROUND(MIN(duration_sec), 2) as min_duration_sec
        FROM task_performance
        WHERE duration_sec IS NOT NULL
        GROUP BY task_id, dag_id
        ORDER BY avg_duration_sec DESC
        LIMIT {limit}
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if df.empty:
        print(f"\n❌ No task data found for the last {days} days")
        return
    
    print(f"\n🐌 Slowest Tasks (Last {days} days)")
    print("=" * 100)
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
    
    return df


def detect_anomalies(dag_id=None, days=7):
    """Detect anomalous execution times"""
    
    conn = get_connection()
    
    where_clause = ""
    params = []
    
    if dag_id:
        where_clause = "AND m.dag_id = %s"
        params.append(dag_id)
    
    query = f"""
        WITH stats AS (
            SELECT 
                dag_id,
                AVG(total_duration_minutes) as mean_duration,
                STDDEV(total_duration_minutes) as stddev_duration
            FROM dag_execution_metrics
            WHERE execution_date >= NOW() - INTERVAL '30 days'
            GROUP BY dag_id
        )
        SELECT 
            m.dag_id,
            m.execution_date::date as run_date,
            m.total_duration_minutes as duration_min,
            s.mean_duration as mean_min,
            ROUND(
                (m.total_duration_minutes - s.mean_duration) / NULLIF(s.stddev_duration, 0),
                2
            ) as z_score,
            CASE 
                WHEN ABS((m.total_duration_minutes - s.mean_duration) / NULLIF(s.stddev_duration, 0)) > 2 
                THEN 'ANOMALY'
                ELSE 'NORMAL'
            END as status
        FROM dag_execution_metrics m
        JOIN stats s ON m.dag_id = s.dag_id
        WHERE m.execution_date >= NOW() - INTERVAL '{days} days'
        {where_clause}
        ORDER BY ABS(
            (m.total_duration_minutes - s.mean_duration) / NULLIF(s.stddev_duration, 0)
        ) DESC
        LIMIT 20
    """
    
    df = pd.read_sql(query, conn, params=params if params else None)
    conn.close()
    
    if df.empty:
        print(f"\n❌ No data found")
        return
    
    print(f"\n⚠️ Anomaly Detection (Last {days} days)")
    print("=" * 100)
    print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
    
    anomalies = df[df['status'] == 'ANOMALY']
    if not anomalies.empty:
        print(f"\n🚨 Found {len(anomalies)} anomalies!")
    else:
        print(f"\n✅ No anomalies detected")
    
    return df


def main():
    parser = argparse.ArgumentParser(
        description='DAG Metrics CLI - Analyze pipeline performance',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show recent runs for all DAGs
  python view_metrics.py recent
  
  # Show recent runs for specific DAG
  python view_metrics.py recent --dag wiki_trending_pipeline
  
  # Compare daily performance
  python view_metrics.py compare --dag wiki_trending_pipeline --days 30
  
  # Show task breakdown for latest run
  python view_metrics.py tasks --dag wiki_trending_pipeline
  
  # Show slowest tasks across all DAGs
  python view_metrics.py slowest --days 7
  
  # Detect anomalies
  python view_metrics.py anomalies --dag wiki_trending_pipeline
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Recent runs command
    recent_parser = subparsers.add_parser('recent', help='Show recent DAG runs')
    recent_parser.add_argument('--dag', help='Filter by DAG ID')
    recent_parser.add_argument('--days', type=int, default=7, help='Number of days to look back')
    
    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare daily performance')
    compare_parser.add_argument('--dag', required=True, help='DAG ID to analyze')
    compare_parser.add_argument('--days', type=int, default=30, help='Number of days to compare')
    
    # Tasks command
    tasks_parser = subparsers.add_parser('tasks', help='Show task breakdown')
    tasks_parser.add_argument('--dag', required=True, help='DAG ID')
    tasks_parser.add_argument('--run', help='Specific run ID (optional, defaults to latest)')
    
    # Slowest tasks command
    slowest_parser = subparsers.add_parser('slowest', help='Show slowest tasks')
    slowest_parser.add_argument('--days', type=int, default=7, help='Number of days to analyze')
    slowest_parser.add_argument('--limit', type=int, default=20, help='Number of tasks to show')
    
    # Anomalies command
    anomalies_parser = subparsers.add_parser('anomalies', help='Detect anomalous runs')
    anomalies_parser.add_argument('--dag', help='Filter by DAG ID')
    anomalies_parser.add_argument('--days', type=int, default=7, help='Number of days to check')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'recent':
            show_recent_runs(dag_id=args.dag, days=args.days)
        
        elif args.command == 'compare':
            compare_performance(dag_id=args.dag, days=args.days)
        
        elif args.command == 'tasks':
            show_task_breakdown(dag_id=args.dag, run_id=args.run)
        
        elif args.command == 'slowest':
            show_slowest_tasks(days=args.days, limit=args.limit)
        
        elif args.command == 'anomalies':
            detect_anomalies(dag_id=args.dag, days=args.days)
        
    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
