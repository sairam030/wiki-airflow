# 🤖 Smart Analytics Orchestrator - Complete Guide

## Overview

The **Smart Analytics Orchestrator** is an intelligent system that automatically:

1. ✅ Checks Diamond bucket for data availability
2. ✅ Identifies missing dates (gaps in historical data)
3. ✅ Automatically triggers `wiki_trending_pipeline` for missing dates only
4. ✅ Waits for all data to be ready
5. ✅ Runs analytics on complete dataset

**Zero changes to your working pipeline!** This is a completely separate orchestration layer.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│         Smart Analytics Orchestrator DAG                    │
│         (wiki_smart_analytics_orchestrator)                 │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  1. Check Completeness │
              │  (Diamond bucket scan) │
              └────────────┬───────────┘
                           │
                ┌──────────┴──────────┐
                │                     │
         ┌──────▼──────┐       ┌─────▼──────┐
         │ All Data OK │       │ Missing    │
         │             │       │ Dates Found│
         └──────┬──────┘       └─────┬──────┘
                │                    │
                │              ┌─────▼──────────────────┐
                │              │ 2. Trigger Missing     │
                │              │    wiki_trending_      │
                │              │    pipeline (N times)  │
                │              └─────┬──────────────────┘
                │                    │
                │              ┌─────▼──────────────────┐
                │              │ 3. Wait for Data Ready │
                │              │    (Sensor - polls     │
                │              │     Diamond bucket)    │
                │              └─────┬──────────────────┘
                │                    │
                └────────────────────┘
                           │
                    ┌──────▼──────────┐
                    │ 4. Run Analytics│
                    │    (Complete    │
                    │     Dataset)    │
                    └─────────────────┘
```

---

## 📁 New Files Created

```
wiki-trends-pipeline/
├── plugins/utils/
│   ├── date_checker.py              # Check Diamond for available dates
│   └── dag_trigger.py               # Trigger DAGs programmatically
├── dags/
│   └── wiki_smart_orchestrator_dag.py  # Smart orchestrator DAG
├── check_and_fetch_data.py          # Manual CLI tool
└── docs/
    └── SMART_ORCHESTRATOR.md        # This file
```

---

## 🚀 Usage

### Option 1: Automatic (Airflow Scheduled)

The orchestrator runs **daily at 3 AM** automatically:

```bash
# Enable automatic scheduling
docker exec -it airflow-scheduler airflow dags unpause wiki_smart_analytics_orchestrator

# Check status
docker exec -it airflow-scheduler airflow dags list | grep orchestrator
```

**What happens:**
1. Daily at 3 AM, checks last 5 days
2. If gaps found, triggers `wiki_trending_pipeline` for each missing date
3. Waits for all data to be ready (polls every 60 seconds)
4. Runs analytics on complete dataset
5. Populates analytics tables

### Option 2: Manual Trigger (Airflow)

```bash
# Trigger with default settings (5 days)
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator

# Trigger with custom days (7 days)
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator --conf '{"days": 7}'

# View logs
docker exec -it airflow-scheduler airflow tasks logs wiki_smart_analytics_orchestrator check_completeness latest
```

### Option 3: Manual CLI Tool (Recommended for Testing)

```bash
# Check date completeness (no auto-trigger)
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py

# Check and auto-trigger missing dates
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py --auto

# Check last 7 days
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py --days 7

# Check 7 days and auto-trigger
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py --days 7 --auto
```

**Interactive Example:**
```
🔍 SMART DATA COMPLETENESS CHECK

================================================================================
📅 DIAMOND BUCKET DATE AVAILABILITY REPORT
================================================================================

📊 Analysis for last 5 days (accounting for 3-day API delay):
   • Total files in Diamond: 8
   • Expected dates: 5
   • Available dates: 3
   • Missing dates: 2
   • Coverage: 60.0%

✅ Available Dates (3):
   • 2025-11-09 (Saturday)
   • 2025-11-08 (Friday)
   • 2025-11-07 (Thursday)

⚠️ Missing Dates (2):
   • 2025-11-06 (Wednesday)
   • 2025-11-05 (Tuesday)

💡 These dates need to be fetched from Wikipedia API

================================================================================

⚠️ Missing 2 dates:
   • 2025-11-06 (6 days ago)
   • 2025-11-05 (7 days ago)

🤔 Trigger wiki_trending_pipeline for these 2 dates? (y/n): y

🚀 Triggering data fetches for 2 missing dates...

[1/2] Processing date: 2025-11-06
🚀 Triggering wiki_trending_pipeline for 2025-11-06
✓ DAG triggered successfully
⏳ Waiting for completion (timeout: 30 min)...
✅ DAG completed successfully!

[2/2] Processing date: 2025-11-05
🚀 Triggering wiki_trending_pipeline for 2025-11-05
✓ DAG triggered successfully
⏳ Waiting for completion (timeout: 30 min)...
✅ DAG completed successfully!

✅ All dates fetched successfully!
📊 You can now run analytics with complete data.
```

### Option 4: Python Import

```python
from plugins.utils.date_checker import find_missing_dates, get_dates_to_fetch
from plugins.utils.dag_trigger import trigger_multiple_dates

# Check what's missing
result = find_missing_dates(days=5)
print(f"Missing: {len(result['missing'])} dates")

# Get dates to fetch
missing = get_dates_to_fetch(days=5)

# Trigger fetches
if missing:
    dates = [m['date'] for m in missing]
    trigger_multiple_dates(dates)
```

---

## 🎯 How It Works

### 1. Date Checking Logic

The system accounts for **Wikipedia API's 1-3 day delay**:

```python
# Example: Today is Nov 12, 2025
# Checking last 5 days:

Expected Range:
  Nov 9 (3 days ago) ← Start here (API has data)
  Nov 8 (4 days ago)
  Nov 7 (5 days ago)
  Nov 6 (6 days ago)
  Nov 5 (7 days ago) ← End here

Why not Nov 10-11?
  → Wikipedia API doesn't have data yet (1-3 day delay)
  → We don't try to fetch these
```

### 2. Missing Date Detection

Scans Diamond bucket for files:
```
diamond_final_YYYY_MM_DD.csv/
```

Compares with expected range and identifies gaps.

### 3. Smart Triggering

For each missing date:
1. Triggers `wiki_trending_pipeline` with config: `{'target_date': 'YYYY-MM-DD'}`
2. Waits for completion (polls every 30 seconds)
3. Handles failures gracefully
4. Small delays between triggers (5 seconds) to avoid overload

### 4. Data Ready Sensor

Polls Diamond bucket every 60 seconds:
- Checks if all expected dates now exist
- Timeout after 2 hours
- Proceeds to analytics when ready

### 5. Analytics Execution

Runs standard analytics on **complete dataset** only.

---

## ⚙️ Configuration

### Change Number of Days

**In Airflow DAG:**
```python
# Edit dags/wiki_smart_orchestrator_dag.py
# Line ~105 (in check_data_completeness function)

n_days = context['dag_run'].conf.get('days', 7)  # Change default from 5 to 7
```

**When triggering:**
```bash
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator --conf '{"days": 10}'
```

### Change Schedule

```python
# Edit dags/wiki_smart_orchestrator_dag.py
# Line ~152

schedule_interval='0 3 * * *',  # Daily at 3 AM
# Change to:
schedule_interval='0 */6 * * *',  # Every 6 hours
```

### Change Sensor Timeout

```python
# Edit dags/wiki_smart_orchestrator_dag.py
# Line ~201 (wait_sensor task)

timeout=7200,  # 2 hours
# Change to:
timeout=3600,  # 1 hour
```

### Change Poll Interval

```python
# Edit dags/wiki_smart_orchestrator_dag.py
# Line ~200

poke_interval=60,  # Check every 60 seconds
# Change to:
poke_interval=30,  # Check every 30 seconds
```

---

## 📊 Monitoring

### Check Orchestrator Status

```bash
# List recent runs
docker exec -it airflow-scheduler airflow dags list-runs -d wiki_smart_analytics_orchestrator

# View task logs
docker exec -it airflow-scheduler airflow tasks logs wiki_smart_analytics_orchestrator check_completeness latest

# View sensor logs (waiting for data)
docker exec -it airflow-scheduler airflow tasks logs wiki_smart_analytics_orchestrator wait_for_data_ready latest
```

### Check Triggered Pipeline Runs

```bash
# See all recent wiki_trending_pipeline runs
docker exec -it airflow-scheduler airflow dags list-runs -d wiki_trending_pipeline --state success,failed,running -o json
```

### Check Diamond Bucket

```bash
# List all files with dates
docker exec -it minio mc ls minio/diamond/ | grep diamond_final
```

---

## 🔍 Troubleshooting

### No missing dates detected but data is incomplete

**Cause:** Diamond bucket has old data (before 3-day window)

**Solution:**
```bash
# Manually check dates
docker exec -it airflow-scheduler python -c "
from plugins.utils.date_checker import print_date_report
print_date_report(10)  # Check last 10 days
"
```

### Sensor times out waiting for data

**Cause:** Triggered DAGs failed or are stuck

**Solution:**
```bash
# Check triggered DAG status
docker exec -it airflow-scheduler airflow dags list-runs -d wiki_trending_pipeline

# Check failed tasks
docker exec -it airflow-scheduler airflow tasks list wiki_trending_pipeline --tree
```

### Duplicate DAG runs

**Cause:** Multiple orchestrator runs triggered simultaneously

**Solution:**
```python
# Already configured in DAG
max_active_runs=1  # Only one orchestrator run at a time
```

### Analytics fails even with complete data

**Cause:** Analytics tables missing or corrupted

**Solution:**
```bash
# Manually run analytics
docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py

# Check tables
docker exec -it postgres psql -U airflow -d airflow -c "\dt wiki_trends*"
```

---

## 🎯 Use Cases

### Use Case 1: Daily Automated Completeness

**Scenario:** You want analytics to always have last 5 days of data

**Solution:**
```bash
# Enable orchestrator (runs daily at 3 AM)
docker exec -it airflow-scheduler airflow dags unpause wiki_smart_analytics_orchestrator
```

Result:
- Every day at 3 AM, checks for gaps
- Automatically fills missing dates
- Analytics always has complete 5-day window

### Use Case 2: Historical Backfill

**Scenario:** You want to analyze last 30 days but have gaps

**Solution:**
```bash
# Check last 30 days and auto-fill
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py --days 30 --auto
```

Result:
- Scans last 30 days
- Triggers fetches for all missing dates
- Waits for completion
- Analytics ready with 30 days

### Use Case 3: Weekly Deep Analysis

**Scenario:** Weekly report on last 7 days

**Solution:**
```bash
# Trigger orchestrator for 7 days
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator --conf '{"days": 7}'
```

Result:
- Ensures all 7 days present
- Runs analytics
- Generates comprehensive report

---

## 🔄 Integration with Existing Pipeline

### No Changes Required

✅ **wiki_trending_pipeline** - No modifications  
✅ **wiki_analytics_trends** - No modifications  
✅ **Diamond bucket** - Same structure  
✅ **Database tables** - Same schema  

### How They Work Together

```
Daily Schedule:
  
2:00 AM → wiki_trending_pipeline (scheduled)
          ├─ Fetches today's data (3 days ago due to API delay)
          └─ Saves to Diamond bucket

3:00 AM → wiki_smart_analytics_orchestrator (scheduled)
          ├─ Checks last 5 days
          ├─ Finds missing dates (if any)
          ├─ Triggers wiki_trending_pipeline for each missing
          ├─ Waits for completion
          └─ Runs analytics on complete dataset

Result:
  → Complete 5-day window always maintained
  → Analytics always has full data
  → No manual intervention needed
```

---

## 📈 Advanced Features

### Custom Date Ranges

```python
# In Python code
from plugins.utils.date_checker import find_missing_dates
from datetime import datetime, timedelta

# Check specific date range
end_date = datetime(2025, 11, 1)
result = find_missing_dates(n_days=10, end_date=end_date)
```

### Conditional Analytics

```python
# Only run analytics if coverage > 80%
def run_smart_analytics(**context):
    coverage = context['ti'].xcom_pull(key='coverage_pct', task_ids='check_completeness')
    
    if coverage < 80:
        raise Exception(f"Insufficient data coverage: {coverage}%")
    
    # Proceed with analytics...
```

### Notification Integration

```python
# Add to orchestrator DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

notify = SlackWebhookOperator(
    task_id='notify_completion',
    http_conn_id='slack_webhook',
    message=f"Analytics complete! Coverage: {coverage}%",
    trigger_rule=TriggerRule.ALL_SUCCESS
)
```

---

## 📊 Example Output

### Successful Run (All Data Available)

```
[2025-11-12 03:00:00] Starting: check_completeness

🔍 Checking Diamond bucket for last 5 days...

📊 Data Completeness:
   • Expected dates: 5
   • Available dates: 5
   • Missing dates: 0
   • Coverage: 100.0%

✅ All dates available - proceeding directly to analytics

[2025-11-12 03:00:15] Starting: run_analytics

📊 Running analytics on 5 days of data (coverage: 100.0%)
✓ Combined data: 4975 total records
✓ Found 523 common articles
✅ Analytics complete!
```

### Successful Run (With Missing Dates)

```
[2025-11-12 03:00:00] Starting: check_completeness

🔍 Checking Diamond bucket for last 5 days...

📊 Data Completeness:
   • Expected dates: 5
   • Available dates: 3
   • Missing dates: 2
   • Coverage: 60.0%

⚠️ Found 2 missing dates - will trigger data fetching

[2025-11-12 03:00:15] Starting: trigger_missing_data_fetches

🚀 Triggering 2 DAG runs for missing dates...
📅 Triggering for 2025-11-06...
✓ Triggered successfully
📅 Triggering for 2025-11-05...
✓ Triggered successfully

[2025-11-12 03:00:30] Starting: wait_for_data_ready

⏳ Checking if all 2 dates are now available...
⏳ Still waiting for 2 dates: 2025-11-06, 2025-11-05
[60s later]
⏳ Still waiting for 1 date: 2025-11-05
[60s later]
✅ All dates now available! Coverage: 100.0%

[2025-11-12 03:15:00] Starting: run_analytics

📊 Running analytics on 5 days of data (coverage: 100.0%)
✓ Combined data: 4975 total records
✓ Found 523 common articles
✅ Analytics complete!
```

---

## ✅ Summary

You now have a **complete smart orchestration system** that:

✅ **Automatically checks** for data completeness  
✅ **Identifies gaps** in historical data  
✅ **Triggers fetches** for missing dates only  
✅ **Waits intelligently** for data to be ready  
✅ **Runs analytics** on complete dataset  
✅ **Zero changes** to your working pipeline  
✅ **Fully automated** or manual trigger options  
✅ **Comprehensive logging** for monitoring  

**Ready to use!** 🚀

---

## 🎓 Learn More

- **Full Analytics Docs**: `ANALYTICS_QUICKSTART.md`
- **Main Pipeline Docs**: `README.md`
- **Airflow UI**: `http://localhost:8080`
