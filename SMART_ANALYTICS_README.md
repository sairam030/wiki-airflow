# 🎯 Smart Analytics System - Complete Overview

## What You Now Have

Your Wikipedia Trends Pipeline now has **TWO powerful systems**:

### 1. **Original Pipeline** (Unchanged ✅)
- `wiki_trending_pipeline` DAG
- Fetches Wikipedia data daily
- Processes Bronze → Silver → Gold → Diamond
- Loads to PostgreSQL

### 2. **NEW: Smart Analytics System** 🆕

#### A. Basic Analytics (`wiki_analytics_trends` DAG)
- Analyzes last 5 days from Diamond bucket
- Tracks article growth/decline
- Creates analytics tables
- **File**: `dags/wiki_analytics_dag.py`

#### B. Smart Orchestrator (`wiki_smart_analytics_orchestrator` DAG) ⭐
- **Checks** Diamond bucket for missing dates
- **Triggers** main pipeline for gaps
- **Waits** for data to be ready
- **Runs** analytics automatically
- **File**: `dags/wiki_smart_orchestrator_dag.py`

---

## 🗂️ Complete File Structure

```
wiki-trends-pipeline/
├── dags/
│   ├── wiki_top_pages_dag.py              # Original pipeline (unchanged)
│   ├── wiki_analytics_dag.py              # NEW: Basic analytics
│   └── wiki_smart_orchestrator_dag.py     # NEW: Smart orchestrator
│
├── plugins/
│   └── utils/
│       ├── analytics_trends.py            # NEW: Analytics engine
│       ├── date_checker.py                # NEW: Check Diamond dates
│       └── dag_trigger.py                 # NEW: Trigger DAGs
│
├── Scripts/
│   ├── run_analytics.py                   # NEW: Run analytics manually
│   ├── check_and_fetch_data.py            # NEW: Check & fetch CLI
│   ├── show_analytics_report.py           # NEW: Pretty reports
│   ├── test_analytics.sh                  # NEW: Test analytics
│   └── test_smart_orchestrator.sh         # NEW: Test orchestrator
│
└── docs/
    ├── ANALYTICS_QUICKSTART.md            # NEW: Analytics guide
    └── SMART_ORCHESTRATOR.md              # NEW: Orchestrator guide
```

---

## 🚀 Quick Start (Choose Your Path)

### Path 1: Fully Automated (Recommended)

**Set it and forget it!**

```bash
# Enable smart orchestrator (runs daily at 3 AM)
docker exec -it airflow-scheduler airflow dags unpause wiki_smart_analytics_orchestrator

# That's it! System will:
#   ✅ Check for missing dates daily
#   ✅ Auto-trigger fetches for gaps
#   ✅ Run analytics on complete data
#   ✅ Keep 5-day window always complete
```

### Path 2: Manual Control

**Check and fetch when you want:**

```bash
# 1. Check what's missing
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py

# 2. Fetch missing dates (with prompt)
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py --auto

# 3. Run analytics
docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py

# 4. View report
docker exec -it airflow-scheduler python /opt/airflow/show_analytics_report.py
```

### Path 3: Trigger Orchestrator On-Demand

```bash
# Trigger smart orchestrator manually
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator

# With custom days (e.g., last 7 days)
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator --conf '{"days": 7}'
```

---

## 📊 Database Tables Overview

### Original Pipeline Tables
- `wiki_diamond` - Current day's data (from main pipeline)

### NEW Analytics Tables (Separate!)
- `wiki_trends_timeseries` - All days combined with date column
- `wiki_trends_common` - Articles present in ALL days
- `wiki_trends_growth` - Growth/decline metrics

**No conflicts!** Analytics tables are completely separate.

---

## 🎯 Common Scenarios

### Scenario 1: First Time Setup

```bash
# 1. Test that everything works
./test_smart_orchestrator.sh

# 2. Check current data availability
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py

# 3. Fetch any missing dates
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py --auto

# 4. Run initial analytics
docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py

# 5. View results
docker exec -it airflow-scheduler python /opt/airflow/show_analytics_report.py

# 6. Enable automation
docker exec -it airflow-scheduler airflow dags unpause wiki_smart_analytics_orchestrator
```

### Scenario 2: Daily Operations (Automated)

**No action needed!** If you enabled the orchestrator:

```
Daily Schedule:
  2:00 AM → wiki_trending_pipeline runs (fetches yesterday's data)
  3:00 AM → wiki_smart_analytics_orchestrator runs
            ├─ Checks last 5 days
            ├─ Fills any gaps
            └─ Runs analytics

Result:
  → Always have complete 5-day analytics
  → Check results anytime with show_analytics_report.py
```

### Scenario 3: Historical Backfill

**Want to analyze last 30 days?**

```bash
# Check and auto-fetch last 30 days
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py --days 30 --auto

# Run analytics on 30 days
docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py 30

# View report
docker exec -it airflow-scheduler python /opt/airflow/show_analytics_report.py
```

### Scenario 4: Weekly Deep Dive

```bash
# Every Monday, trigger 7-day analysis
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator --conf '{"days": 7}'

# Or add to cron:
# 0 8 * * 1 docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator --conf '{"days": 7}'
```

---

## 🔍 Monitoring & Debugging

### Check System Status

```bash
# All DAGs status
docker exec -it airflow-scheduler airflow dags list

# Orchestrator status
docker exec -it airflow-scheduler airflow dags list-runs -d wiki_smart_analytics_orchestrator

# View orchestrator logs
docker exec -it airflow-scheduler airflow tasks logs wiki_smart_analytics_orchestrator check_completeness latest
```

### Check Data Availability

```bash
# Quick check
docker exec -it minio mc ls minio/diamond/ | grep diamond_final

# Detailed report
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py
```

### Check Analytics Tables

```bash
# List tables
docker exec -it postgres psql -U airflow -d airflow -c "\dt wiki_*"

# Count records
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT 
  'timeseries' as table, COUNT(*) as records FROM wiki_trends_timeseries
UNION ALL
SELECT 
  'common' as table, COUNT(*) as records FROM wiki_trends_common
UNION ALL
SELECT 
  'growth' as table, COUNT(*) as records FROM wiki_trends_growth;
"
```

---

## 📈 Example Queries

### Top 10 Growing Articles
```sql
SELECT 
    page_display,
    first_views,
    last_views,
    views_change,
    views_change_pct,
    category
FROM wiki_trends_growth 
ORDER BY views_change DESC 
LIMIT 10;
```

### Articles Trending All 5 Days
```sql
SELECT 
    page_display,
    COUNT(DISTINCT date) as days_trending,
    AVG(views) as avg_views,
    category
FROM wiki_trends_timeseries
GROUP BY page_display, category
HAVING COUNT(DISTINCT date) = 5
ORDER BY avg_views DESC
LIMIT 20;
```

### Category Performance
```sql
SELECT 
    category,
    COUNT(*) as article_count,
    AVG(views_change_pct) as avg_growth,
    SUM(CASE WHEN views_change > 0 THEN 1 ELSE 0 END) as growing,
    SUM(CASE WHEN views_change < 0 THEN 1 ELSE 0 END) as declining
FROM wiki_trends_growth 
GROUP BY category 
ORDER BY avg_growth DESC;
```

---

## 🎓 Learn More

- **Analytics Guide**: `docs/ANALYTICS_QUICKSTART.md`
- **Orchestrator Guide**: `docs/SMART_ORCHESTRATOR.md`
- **Main Pipeline**: `README.md`

---

## ✅ Summary Checklist

What you now have:

- [x] Original pipeline (unchanged, working)
- [x] Analytics engine (track trends over time)
- [x] Smart orchestrator (auto-fill missing dates)
- [x] 3 new database tables (separate from main)
- [x] Manual CLI tools (full control)
- [x] Automated scheduling (set and forget)
- [x] Comprehensive reporting (pretty output)
- [x] Complete documentation (this file + guides)

**Everything is modular and independent!**

---

## 🚀 Ready to Go!

```bash
# Test everything
./test_smart_orchestrator.sh

# Check your data
docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py

# Enable automation
docker exec -it airflow-scheduler airflow dags unpause wiki_smart_analytics_orchestrator

# View results anytime
docker exec -it airflow-scheduler python /opt/airflow/show_analytics_report.py
```

**Enjoy your new analytics superpowers! 🎉**
