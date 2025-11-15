# 📊 Wikipedia Trends Analytics - Quick Start Guide

## Overview

Your **analytics module** is now ready! It analyzes historical Diamond bucket data to track Wikipedia article trends over time.

### ✅ What's New (Separate from Main Pipeline)

- **3 new PostgreSQL tables** for analytics
- **Standalone analytics script** (no changes to main pipeline)
- **Airflow DAG** for scheduled analytics
- **Visualization reports** for insights

---

## 🚀 Quick Start

### Step 1: Run Analytics (Manual)

**From your host machine:**

```bash
# Run analytics for last 5 days (default)
docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py

# Or specify number of days
docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py 7
```

**Expected Output:**
```
================================================================================
📊 WIKIPEDIA TRENDS ANALYTICS
================================================================================
📅 Found 5 files from last 5 days:
   • 2025-11-12: diamond_final_2025_11_12.csv
   • 2025-11-11: diamond_final_2025_11_11.csv
   ...

✓ Combined data: 4975 total records across 5 days
✓ Found 523 articles present in all 5 days
✓ Calculated metrics for 523 articles

✅ Loaded 4975 timeseries records
✅ Loaded 2615 common article records
✅ Loaded 523 growth metrics
```

### Step 2: View Analytics Report

```bash
# Generate beautiful formatted report
docker exec -it airflow-scheduler python /opt/airflow/show_analytics_report.py
```

**Report Includes:**
- 📈 Top 10 growing articles
- 📉 Top 10 declining articles
- 🎢 Most volatile articles
- 🚀 Best rank improvements
- 📊 Category-level trends
- 🔥 Sustained trending articles

---

## 📊 Database Tables Created

### 1. `wiki_trends_timeseries`
**Complete time series data with dates**

```sql
SELECT * FROM wiki_trends_timeseries LIMIT 5;
```

Columns:
- `page`, `page_display`, `date`
- `views`, `rank`, `category`
- `location_type`, `location`
- `latitude`, `longitude`

### 2. `wiki_trends_common`
**Articles present in ALL analyzed days**

```sql
SELECT COUNT(DISTINCT page) FROM wiki_trends_common;
```

Perfect for finding **sustained trends** (not one-day spikes).

### 3. `wiki_trends_growth`
**Growth/decline metrics and statistics**

```sql
SELECT * FROM wiki_trends_growth ORDER BY views_change DESC LIMIT 10;
```

Columns:
- View changes (absolute & percentage)
- Rank changes
- Volatility (max - min views)
- Average, max, min views
- Days tracked

---

## 🔍 Useful SQL Queries

### Top Growing Articles
```sql
SELECT 
    page_display,
    views_change,
    views_change_pct,
    first_views,
    last_views,
    category
FROM wiki_trends_growth 
ORDER BY views_change DESC 
LIMIT 10;
```

### Top Declining Articles
```sql
SELECT 
    page_display,
    views_change,
    views_change_pct,
    category
FROM wiki_trends_growth 
ORDER BY views_change ASC 
LIMIT 10;
```

### Most Volatile (Trending Spikes)
```sql
SELECT 
    page_display,
    volatility,
    max_views,
    min_views,
    category
FROM wiki_trends_growth 
ORDER BY volatility DESC 
LIMIT 10;
```

### Category Performance
```sql
SELECT 
    category,
    COUNT(*) as article_count,
    AVG(views_change_pct) as avg_growth_pct,
    SUM(CASE WHEN views_change > 0 THEN 1 ELSE 0 END) as growing,
    SUM(CASE WHEN views_change < 0 THEN 1 ELSE 0 END) as declining
FROM wiki_trends_growth 
GROUP BY category 
ORDER BY avg_growth_pct DESC;
```

### Articles Present All Days
```sql
SELECT 
    page_display,
    AVG(views) as avg_views,
    category
FROM wiki_trends_common
GROUP BY page_display, category
ORDER BY avg_views DESC
LIMIT 20;
```

### Daily Trend for Specific Article
```sql
SELECT 
    date,
    views,
    rank,
    category
FROM wiki_trends_timeseries
WHERE page_display = 'United_States'
ORDER BY date;
```

### Best Rank Improvements
```sql
SELECT 
    page_display,
    first_rank,
    last_rank,
    rank_change,
    views_change_pct,
    category
FROM wiki_trends_growth 
WHERE rank_change > 0  -- Improved (lower rank number)
ORDER BY rank_change DESC 
LIMIT 20;
```

---

## 🤖 Airflow DAG (Automated)

### Schedule
The analytics DAG runs **daily at 2 AM** automatically.

### Trigger Manually
```bash
# Trigger analytics DAG
docker exec -it airflow-scheduler airflow dags trigger wiki_analytics_trends

# Check status
docker exec -it airflow-scheduler airflow dags list | grep analytics

# View logs
docker exec -it airflow-scheduler airflow tasks logs wiki_analytics_trends analyze_trends latest
```

### DAG Details
- **DAG ID**: `wiki_analytics_trends`
- **Schedule**: `0 2 * * *` (Daily at 2 AM)
- **Task**: `analyze_trends`
- **Timeout**: 30 minutes
- **Retries**: 1

---

## 🧪 Testing

### Test Script
```bash
# Run comprehensive test
./test_analytics.sh
```

This checks:
1. Diamond bucket has data
2. Python imports work
3. Module is accessible

### Manual Test
```bash
# Check Diamond bucket
docker exec -it minio mc ls minio/diamond/

# Test Python import
docker exec -it airflow-scheduler python -c "
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from utils.analytics_trends import run_analytics
print('✅ Import successful')
"
```

---

## 📈 Use Cases

### 1. Track Article Growth Over Time
```python
# Python example
from utils.analytics_trends import run_analytics

analytics = run_analytics(days=7)

# Get top 10 growing
top_growing = analytics['growth_metrics'].nlargest(10, 'views_change')
print(top_growing[['page_display', 'views_change', 'views_change_pct']])
```

### 2. Find Sustained Trends (Not Spikes)
```sql
-- Articles trending consistently for 5+ days
SELECT page_display, AVG(views) as avg_views
FROM wiki_trends_common
GROUP BY page_display
HAVING COUNT(*) >= 5
ORDER BY avg_views DESC;
```

### 3. Category Analysis
```sql
-- Which categories are growing/declining?
SELECT 
    category,
    AVG(views_change_pct) as trend
FROM wiki_trends_growth
GROUP BY category
ORDER BY trend DESC;
```

### 4. Geographic Trends
```sql
-- Trending by location
SELECT 
    location,
    COUNT(*) as article_count,
    AVG(views_change_pct) as avg_growth
FROM wiki_trends_growth 
WHERE location IS NOT NULL
GROUP BY location
ORDER BY avg_growth DESC;
```

---

## 🔧 Configuration

### Change Number of Days
Edit `dags/wiki_analytics_dag.py`:

```python
# Change from 5 to 7 days
analytics_data = run_analytics(days=7)
```

### Change Schedule
Edit `dags/wiki_analytics_dag.py`:

```python
# Run every 6 hours instead of daily
schedule_interval='0 */6 * * *'
```

### Add Email Alerts
```python
default_args = {
    'email': ['your@email.com'],
    'email_on_failure': True,
    'email_on_success': True,
}
```

---

## 📁 Files Created

```
wiki-trends-pipeline/
├── plugins/utils/
│   └── analytics_trends.py         # Main analytics module
├── dags/
│   └── wiki_analytics_dag.py       # Airflow DAG
├── run_analytics.py                # Manual runner
├── show_analytics_report.py        # Report generator
├── test_analytics.sh               # Test script
└── docs/
    └── ANALYTICS_README.md         # Full documentation
```

---

## ⚠️ Important Notes

1. **No Changes to Main Pipeline**
   - Completely separate from `wiki_trending_pipeline`
   - Read-only access to Diamond bucket
   - Independent database tables

2. **Requires Existing Data**
   - Need at least 2 days of Diamond bucket data
   - Run main pipeline first to generate data

3. **Database Tables**
   - Creates 3 new tables (not affecting `wiki_diamond`)
   - Uses UPSERT (safe to run multiple times)

4. **Performance**
   - Handles 1000s of records efficiently
   - Typical runtime: 10-30 seconds

---

## 🎯 Next Steps

1. **Run Initial Analytics**
   ```bash
   docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py
   ```

2. **View Report**
   ```bash
   docker exec -it airflow-scheduler python /opt/airflow/show_analytics_report.py
   ```

3. **Query Database**
   ```bash
   docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM wiki_trends_growth;"
   ```

4. **Enable Scheduled Analytics**
   ```bash
   docker exec -it airflow-scheduler airflow dags unpause wiki_analytics_trends
   ```

---

## 🐛 Troubleshooting

### No files found in Diamond bucket
```bash
# Check bucket
docker exec -it minio mc ls minio/diamond/

# Run main pipeline first
docker exec -it airflow-scheduler airflow dags trigger wiki_trending_pipeline
```

### Import errors
```bash
# Run from container (not host)
docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py
```

### Database connection failed
```bash
# Verify PostgreSQL
docker ps | grep postgres

# Test connection
docker exec -it postgres psql -U airflow -d airflow -c "SELECT 1;"
```

---

## 📚 Documentation

See `docs/ANALYTICS_README.md` for complete documentation including:
- Detailed architecture
- All query examples
- Advanced use cases
- API reference

---

## ✅ Summary

You now have a **complete analytics system** that:

- ✅ Tracks article trends over multiple days
- ✅ Identifies sustained vs. spike trends
- ✅ Calculates growth/decline metrics
- ✅ Stores in separate PostgreSQL tables
- ✅ Runs manually or scheduled via Airflow
- ✅ Generates formatted reports
- ✅ **NO changes to your working pipeline**

**Ready to use!** 🚀
