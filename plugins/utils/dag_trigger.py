"""
DAG Trigger Utility
Triggers wiki_trending_pipeline for specific dates
"""

import subprocess
import time
import json
from datetime import datetime


def trigger_dag_for_date(target_date, wait_for_completion=True, timeout_minutes=30):
    """
    Trigger wiki_trending_pipeline DAG for a specific date
    
    Args:
        target_date: datetime object or 'YYYY-MM-DD' string
        wait_for_completion: Wait for DAG run to complete
        timeout_minutes: Maximum wait time
    
    Returns:
        dict: {'success': bool, 'run_id': str, 'status': str}
    """
    # Convert to string if datetime object
    if isinstance(target_date, datetime):
        date_str = target_date.strftime('%Y-%m-%d')
    else:
        date_str = target_date
    
    print(f"\n🚀 Triggering wiki_trending_pipeline for {date_str}")
    
    # Trigger DAG with configuration
    # The DAG will use this date instead of "today"
    conf = json.dumps({'target_date': date_str})
    
    try:
        # Trigger the DAG
        result = subprocess.run(
            ['airflow', 'dags', 'trigger', 'wiki_trending_pipeline', '--conf', conf],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"❌ Failed to trigger DAG: {result.stderr}")
            return {'success': False, 'error': result.stderr}
        
        # Extract run_id from output
        output = result.stdout
        # Example output: "Created <DagRun wiki_trending_pipeline @ 2025-11-12T10:00:00+00:00: manual__2025-11-12T10:00:00+00:00, state:queued, queued_at: 2025-11-12 10:00:00+00:00>"
        run_id = None
        if 'manual__' in output:
            try:
                run_id = output.split('manual__')[1].split(',')[0].strip()
            except:
                pass
        
        print(f"✓ DAG triggered successfully")
        if run_id:
            print(f"  Run ID: {run_id}")
        
        if not wait_for_completion:
            return {
                'success': True,
                'run_id': run_id,
                'status': 'triggered',
                'date': date_str
            }
        
        # Wait for completion
        print(f"⏳ Waiting for completion (timeout: {timeout_minutes} min)...")
        
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        while True:
            # Check if timeout
            if time.time() - start_time > timeout_seconds:
                print(f"⚠️ Timeout after {timeout_minutes} minutes")
                return {
                    'success': False,
                    'run_id': run_id,
                    'status': 'timeout',
                    'date': date_str
                }
            
            # Check DAG run status
            result = subprocess.run(
                ['airflow', 'dags', 'list-runs', '-d', 'wiki_trending_pipeline', '--state', 'running,queued,success,failed', '-o', 'json'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                try:
                    runs = json.loads(result.stdout)
                    # Find our run (most recent if no run_id)
                    latest_run = runs[0] if runs else None
                    
                    if latest_run:
                        state = latest_run.get('state', 'unknown')
                        
                        if state == 'success':
                            print(f"✅ DAG completed successfully!")
                            return {
                                'success': True,
                                'run_id': latest_run.get('run_id'),
                                'status': 'success',
                                'date': date_str
                            }
                        elif state == 'failed':
                            print(f"❌ DAG failed!")
                            return {
                                'success': False,
                                'run_id': latest_run.get('run_id'),
                                'status': 'failed',
                                'date': date_str
                            }
                        elif state in ['running', 'queued']:
                            elapsed = int(time.time() - start_time)
                            print(f"  ⏳ Still {state}... ({elapsed}s elapsed)")
                        
                except Exception as e:
                    print(f"⚠️ Error checking status: {e}")
            
            # Wait before next check
            time.sleep(30)  # Check every 30 seconds
            
    except Exception as e:
        print(f"❌ Error triggering DAG: {e}")
        return {'success': False, 'error': str(e)}


def trigger_multiple_dates(dates, wait_between_runs=60):
    """
    Trigger DAG for multiple dates sequentially
    
    Args:
        dates: List of datetime objects or 'YYYY-MM-DD' strings
        wait_between_runs: Seconds to wait between triggers
    
    Returns:
        list: [{'date': str, 'success': bool, 'status': str}, ...]
    """
    results = []
    
    print(f"\n" + "="*70)
    print(f"🚀 TRIGGERING {len(dates)} DAG RUNS")
    print("="*70)
    
    for i, date in enumerate(dates, 1):
        print(f"\n[{i}/{len(dates)}] Processing date: {date}")
        
        result = trigger_dag_for_date(date, wait_for_completion=True)
        results.append(result)
        
        if result['success']:
            print(f"✓ Date {date} completed successfully")
        else:
            print(f"✗ Date {date} failed: {result.get('status', 'unknown')}")
        
        # Wait before next run (except for last one)
        if i < len(dates):
            print(f"\n⏸️ Waiting {wait_between_runs}s before next run...")
            time.sleep(wait_between_runs)
    
    # Summary
    print(f"\n" + "="*70)
    print(f"📊 SUMMARY")
    print("="*70)
    
    success_count = sum(1 for r in results if r['success'])
    failed_count = len(results) - success_count
    
    print(f"✅ Successful: {success_count}/{len(dates)}")
    print(f"❌ Failed: {failed_count}/{len(dates)}")
    
    if failed_count > 0:
        print(f"\n⚠️ Failed dates:")
        for r in results:
            if not r['success']:
                print(f"   • {r['date']}: {r.get('status', 'unknown')}")
    
    return results


if __name__ == "__main__":
    # Test triggering
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python dag_trigger.py YYYY-MM-DD [YYYY-MM-DD ...]")
        sys.exit(1)
    
    dates = sys.argv[1:]
    results = trigger_multiple_dates(dates)
    
    # Exit with error if any failed
    if any(not r['success'] for r in results):
        sys.exit(1)
