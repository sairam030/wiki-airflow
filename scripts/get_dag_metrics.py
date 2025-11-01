#!/usr/bin/env python3
"""
Get DAG metrics: latency, throughput, task durations
Uses Airflow REST API to fetch execution metrics
"""

import requests
from datetime import datetime, timedelta
import json
from collections import defaultdict

# Airflow API configuration
AIRFLOW_URL = "http://localhost:8080/api/v1"
USERNAME = "admin"
PASSWORD = "admin"

def get_dag_runs(dag_id, limit=10):
    """Get recent DAG runs"""
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    params = {"limit": limit, "order_by": "-execution_date"}
    
    response = requests.get(
        url,
        auth=(USERNAME, PASSWORD),
        params=params
    )
    
    if response.status_code == 200:
        return response.json()["dag_runs"]
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return []


def get_task_instances(dag_id, dag_run_id):
    """Get all task instances for a DAG run"""
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    
    response = requests.get(url, auth=(USERNAME, PASSWORD))
    
    if response.status_code == 200:
        return response.json()["task_instances"]
    else:
        return []


def calculate_duration(start_str, end_str):
    """Calculate duration in seconds"""
    if not start_str or not end_str:
        return None
    
    start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
    end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
    
    return (end - start).total_seconds()


def analyze_dag_metrics(dag_id="wiki_trending_pipeline", num_runs=10):
    """Analyze DAG performance metrics"""
    
    print("=" * 80)
    print(f"📊 DAG PERFORMANCE METRICS: {dag_id}")
    print("=" * 80)
    
    # Get recent DAG runs
    dag_runs = get_dag_runs(dag_id, limit=num_runs)
    
    if not dag_runs:
        print("❌ No DAG runs found!")
        return
    
    print(f"\n✓ Analyzing {len(dag_runs)} most recent runs...\n")
    
    # Overall DAG metrics
    dag_durations = []
    task_durations = defaultdict(list)
    success_count = 0
    failed_count = 0
    
    for run in dag_runs:
        run_id = run["dag_run_id"]
        state = run["state"]
        
        # Count success/failure
        if state == "success":
            success_count += 1
        elif state == "failed":
            failed_count += 1
        
        # Calculate DAG duration
        dag_duration = calculate_duration(run["start_date"], run["end_date"])
        if dag_duration:
            dag_durations.append(dag_duration)
        
        # Get task instances
        tasks = get_task_instances(dag_id, run_id)
        
        for task in tasks:
            task_id = task["task_id"]
            task_duration = calculate_duration(task["start_date"], task["end_date"])
            
            if task_duration:
                task_durations[task_id].append(task_duration)
    
    # Print DAG-level metrics
    print("📈 DAG-LEVEL METRICS")
    print("-" * 80)
    
    if dag_durations:
        avg_duration = sum(dag_durations) / len(dag_durations)
        min_duration = min(dag_durations)
        max_duration = max(dag_durations)
        
        print(f"  • Total Runs Analyzed: {len(dag_runs)}")
        print(f"  • Successful Runs: {success_count}")
        print(f"  • Failed Runs: {failed_count}")
        print(f"  • Success Rate: {(success_count/len(dag_runs)*100):.1f}%")
        print(f"\n  ⏱️  DAG Duration:")
        print(f"     - Average: {avg_duration:.2f}s ({avg_duration/60:.2f} min)")
        print(f"     - Min: {min_duration:.2f}s ({min_duration/60:.2f} min)")
        print(f"     - Max: {max_duration:.2f}s ({max_duration/60:.2f} min)")
        
        # Throughput (runs per day)
        if len(dag_runs) > 1:
            first_run = datetime.fromisoformat(dag_runs[-1]["execution_date"].replace('Z', '+00:00'))
            last_run = datetime.fromisoformat(dag_runs[0]["execution_date"].replace('Z', '+00:00'))
            time_span = (last_run - first_run).total_seconds() / 86400  # days
            
            if time_span > 0:
                throughput = len(dag_runs) / time_span
                print(f"\n  📊 Throughput: {throughput:.2f} runs/day")
    
    # Print task-level metrics
    print("\n\n📋 TASK-LEVEL METRICS")
    print("-" * 80)
    
    # Sort tasks by average duration
    task_stats = []
    for task_id, durations in task_durations.items():
        if durations:
            avg = sum(durations) / len(durations)
            task_stats.append((task_id, avg, min(durations), max(durations), len(durations)))
    
    task_stats.sort(key=lambda x: x[1], reverse=True)  # Sort by average duration
    
    print(f"{'Task Name':<35} {'Avg (s)':<12} {'Min (s)':<12} {'Max (s)':<12} {'Runs':<8}")
    print("-" * 80)
    
    for task_id, avg, min_dur, max_dur, count in task_stats:
        print(f"{task_id:<35} {avg:>10.2f}  {min_dur:>10.2f}  {max_dur:>10.2f}  {count:>6}")
    
    # Identify bottlenecks
    print("\n\n🔍 BOTTLENECK ANALYSIS")
    print("-" * 80)
    
    if task_stats:
        # Top 3 slowest tasks
        print("  Slowest Tasks:")
        for i, (task_id, avg, _, _, _) in enumerate(task_stats[:3], 1):
            percentage = (avg / sum(dag_durations) * len(dag_durations) * 100) if dag_durations else 0
            print(f"    {i}. {task_id}: {avg:.2f}s ({percentage:.1f}% of total)")
    
    print("\n" + "=" * 80)


def get_recent_dag_runs_summary(dag_id="wiki_trending_pipeline", num_runs=5):
    """Get a simple summary of recent runs"""
    
    print("\n" + "=" * 80)
    print(f"📜 RECENT DAG RUNS: {dag_id}")
    print("=" * 80)
    
    dag_runs = get_dag_runs(dag_id, limit=num_runs)
    
    if not dag_runs:
        print("❌ No runs found")
        return
    
    print(f"\n{'Execution Date':<30} {'State':<12} {'Duration':<15} {'Run ID':<40}")
    print("-" * 80)
    
    for run in dag_runs:
        exec_date = run["execution_date"][:19]  # Truncate
        state = run["state"]
        duration = calculate_duration(run["start_date"], run["end_date"])
        duration_str = f"{duration:.2f}s ({duration/60:.2f} min)" if duration else "N/A"
        run_id = run["dag_run_id"][:35]  # Truncate
        
        # Color code state
        state_emoji = "✅" if state == "success" else "❌" if state == "failed" else "⏳"
        
        print(f"{exec_date:<30} {state_emoji} {state:<10} {duration_str:<15} {run_id:<40}")
    
    print("=" * 80)


if __name__ == "__main__":
    import sys
    
    # Get DAG ID from command line or use default
    dag_id = sys.argv[1] if len(sys.argv) > 1 else "wiki_trending_pipeline"
    num_runs = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    # Show recent runs
    get_recent_dag_runs_summary(dag_id, num_runs=5)
    
    # Show detailed metrics
    analyze_dag_metrics(dag_id, num_runs=num_runs)
