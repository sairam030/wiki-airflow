#!/usr/bin/env python3
"""
DAG Metrics Comparison Tool
Compare performance metrics across different DAG runs
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import argparse
import sys


def get_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )


def compare_today_vs_yesterday(dag_id='wiki_trending_pipeline'):
    """Compare today's run with yesterday's"""
    
    conn = get_connection()
    
    query = """
        SELECT 
            execution_date::date as run_date,
            COUNT(*) as runs,
            AVG(total_duration_minutes) as avg_duration_min,
            AVG(success_rate) as avg_success_rate,
            SUM(total_retries) as total_retries,
            AVG(total_tasks) as avg_tasks,
            AVG(successful_tasks) as avg_successful_tasks
        FROM dag_execution_metrics
        WHERE dag_id = %s
        AND execution_date >= CURRENT_DATE - INTERVAL '2 days'
        GROUP BY execution_date::date
        ORDER BY run_date DESC
        LIMIT 2
    """
    
    df = pd.read_sql(query, conn, params=(dag_id,))
    conn.close()
    
    if len(df) < 2:
        print(f"⚠️  Not enough data to compare (found {len(df)} day(s))")
        return
    
    print(f"\n📊 Performance Comparison: {dag_id}")
    print("=" * 80)
    print(f"\nToday vs Yesterday:")
    print("-" * 80)
    print(df.to_string(index=False))
    print("-" * 80)
    
    # Calculate differences
    if len(df) >= 2:
        today = df.iloc[0]
        yesterday = df.iloc[1]
        
        print(f"\n📈 Changes from Yesterday:")
        print(f"  • Duration: {today['avg_duration_min']:.2f} min → {yesterday['avg_duration_min']:.2f} min")
        
        if pd.notna(today['avg_duration_min']) and pd.notna(yesterday['avg_duration_min']):
            duration_change = ((today['avg_duration_min'] - yesterday['avg_duration_min']) / 
                             yesterday['avg_duration_min'] * 100)
            print(f"    Change: {duration_change:+.2f}%")
        
        print(f"  • Success Rate: {today['avg_success_rate']:.2f}% → {yesterday['avg_success_rate']:.2f}%")
        print(f"  • Retries: {int(today['total_retries'])} → {int(yesterday['total_retries'])}")
    

def show_latest_metrics(dag_id='wiki_trending_pipeline', limit=5):
    """Show latest metrics"""
    
    conn = get_connection()
    
    query = """
        SELECT 
            dag_id,
            run_id,
            execution_date,
            state,
            total_duration_minutes,
            total_tasks,
            successful_tasks,
            success_rate,
            total_retries,
            throughput_records_per_second
        FROM dag_execution_metrics
        WHERE dag_id = %s
        ORDER BY execution_date DESC
        LIMIT %s
    """
    
    df = pd.read_sql(query, conn, params=(dag_id, limit))
    conn.close()
    
    print(f"\n📋 Latest {limit} Runs: {dag_id}")
    print("=" * 120)
    print(df.to_string(index=False))
    

def show_task_breakdown(dag_id, run_id):
    """Show task-level breakdown for a specific run"""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT task_metrics 
        FROM dag_execution_metrics 
        WHERE dag_id = %s AND run_id = %s
    """, (dag_id, run_id))
    
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if not result or not result[0]:
        print(f"⚠️  No task metrics found for {dag_id} - {run_id}")
        return
    
    task_metrics = result[0]
    
    print(f"\n🔍 Task Breakdown: {dag_id} - {run_id}")
    print("=" * 100)
    print(f"{'Task ID':<35} {'Duration (s)':<15} {'State':<12} {'Tries':<8}")
    print("-" * 100)
    
    # Sort by duration (longest first)
    sorted_tasks = sorted(task_metrics, 
                         key=lambda x: x.get('duration_seconds', 0) or 0, 
                         reverse=True)
    
    for task in sorted_tasks:
        duration = f"{task['duration_seconds']:.2f}" if task.get('duration_seconds') else "N/A"
        print(f"{task['task_id']:<35} {duration:<15} {task['state']:<12} {task['try_number']:<8}")
    

def show_trend(dag_id='wiki_trending_pipeline', days=7):
    """Show performance trend over time"""
    
    conn = get_connection()
    
    query = f"""
        SELECT 
            execution_date::date as run_date,
            COUNT(*) as total_runs,
            AVG(total_duration_minutes) as avg_duration_min,
            MAX(total_duration_minutes) as max_duration_min,
            MIN(total_duration_minutes) as min_duration_min,
            AVG(success_rate) as avg_success_rate,
            SUM(total_retries) as total_retries
        FROM dag_execution_metrics
        WHERE dag_id = %s
        AND execution_date >= NOW() - INTERVAL '{days} days'
        GROUP BY execution_date::date
        ORDER BY run_date DESC
    """
    
    df = pd.read_sql(query, conn, params=(dag_id,))
    conn.close()
    
    print(f"\n📈 Performance Trend ({days} days): {dag_id}")
    print("=" * 120)
    print(df.to_string(index=False))
    

def main():
    parser = argparse.ArgumentParser(description='Compare DAG metrics')
    parser.add_argument('--dag-id', default='wiki_trending_pipeline', 
                       help='DAG ID to analyze')
    parser.add_argument('--compare', action='store_true',
                       help='Compare today vs yesterday')
    parser.add_argument('--latest', type=int, metavar='N',
                       help='Show latest N runs')
    parser.add_argument('--trend', type=int, metavar='DAYS',
                       help='Show trend over N days')
    parser.add_argument('--task-breakdown', metavar='RUN_ID',
                       help='Show task breakdown for specific run')
    
    args = parser.parse_args()
    
    try:
        if args.compare:
            compare_today_vs_yesterday(args.dag_id)
        elif args.latest:
            show_latest_metrics(args.dag_id, args.latest)
        elif args.trend:
            show_trend(args.dag_id, args.trend)
        elif args.task_breakdown:
            show_task_breakdown(args.dag_id, args.task_breakdown)
        else:
            # Default: show latest 5 runs
            show_latest_metrics(args.dag_id, 5)
            
    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
