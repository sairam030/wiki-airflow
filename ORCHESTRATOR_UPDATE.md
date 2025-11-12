# 🎯 Smart Orchestrator - Updated Flow

## Overview

The `wiki_smart_analytics_orchestrator` DAG now:

1. ✅ Checks Diamond bucket for missing dates
2. ✅ Triggers `wiki_trending_pipeline` for missing dates (with specific target_date)
3. ✅ Loads last 5 days to PostgreSQL with **date column** (`wiki_timeseries`)
4. ✅ Finds common articles across all 5 days (`wiki_common_articles`)

---

## DAG Flow

```
┌─────────────────────────┐
│  check_completeness     │
│  (Branch)               │
└───────┬─────────────────┘
        │
    ┌───┴───┐
    │       │
    ▼       ▼
┌─────┐  ┌──────────────────┐
│Skip │  │trigger_missing_  │
│     │  │data_fetches      │
└──┬──┘  └────────┬─────────┘
   │             │
   │         ┌───▼──────────────┐
   │         │wait_for_data_    │
   │         │ready (Sensor)    │
   │         └────────┬─────────┘
   │                  │
   └──────────────────┘
           │
      ┌────▼────────────────┐
      │load_timeseries      │
      │(PostgreSQL)         │
      └─────┬───────────────┘
            │
      ┌─────▼───────────────┐
      │find_common_articles │
      │(PostgreSQL)         │
      └─────────────────────┘
```

---

## PostgreSQL Tables Created

### 1. `wiki_timeseries`
All data from last 5 days with date column

**Columns:**
- `id` (primary key)
- `page` (article name)
- `page_display` (readable name)
- `date` (DATE - which day this data is from) ⭐
- `views`, `rank`, `category`
- `location_type`, `location`
- `latitude`, `longitude`

**Use cases:**
- Track article views over time
- Find trending patterns
- Analyze daily changes

**Example queries:**
```sql
-- Daily views for an article
SELECT date, views, rank 
FROM wiki_timeseries 
WHERE page = 'United_States' 
ORDER BY date;

-- Top articles on specific date
SELECT page_display, views, category
FROM wiki_timeseries
WHERE date = '2025-11-10'
ORDER BY views DESC
LIMIT 10;

-- Articles with increasing views
SELECT page_display,
       MIN(views) as min_views,
       MAX(views) as max_views,
       MAX(views) - MIN(views) as growth
FROM wiki_timeseries
GROUP BY page_display
HAVING MAX(views) > MIN(views)
ORDER BY growth DESC;
```

### 2. `wiki_common_articles`
Articles present in **ALL 5 days** (sustained trending)

**Columns:**
- `id` (primary key)
- `page`, `page_display`
- `date` (which day)
- `views`, `rank`, `category`
- `location`

**Use cases:**
- Find consistently trending topics
- Exclude one-day viral spikes
- Focus on sustained interest

**Example queries:**
```sql
-- Top sustained trending articles
SELECT page_display,
       AVG(views) as avg_views,
       MIN(views) as min_views,
       MAX(views) as max_views,
       category
FROM wiki_common_articles
GROUP BY page_display, category
ORDER BY avg_views DESC
LIMIT 20;

-- How many articles are consistently trending?
SELECT COUNT(DISTINCT page) FROM wiki_common_articles;

-- Sustained trending by category
SELECT category,
       COUNT(DISTINCT page) as article_count,
       AVG(views) as avg_views
FROM wiki_common_articles
GROUP BY category
ORDER BY avg_views DESC;
```

---

## Usage

### Automatic (Scheduled)

```bash
# Enable the orchestrator (runs daily at 3 AM)
docker exec -it airflow-scheduler airflow dags unpause wiki_smart_analytics_orchestrator
```

### Manual Trigger

```bash
# Trigger with default (5 days)
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator

# Trigger with custom days
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator --conf '{"days": 7}'
```

### Check Results

```bash
# Check timeseries table
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT COUNT(*) as total_records, 
       COUNT(DISTINCT page) as unique_articles,
       COUNT(DISTINCT date) as days
FROM wiki_timeseries;
"

# Check common articles
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT COUNT(DISTINCT page) as common_articles
FROM wiki_common_articles;
"

# Top 10 sustained trending
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT page_display, 
       ROUND(AVG(views)) as avg_views,
       category
FROM wiki_common_articles
GROUP BY page_display, category
ORDER BY avg_views DESC
LIMIT 10;
"
```

---

## Key Changes from Before

### ✅ What Changed
- ❌ Removed: `run_analytics` task (old analytics module)
- ✅ Added: `load_timeseries` task (loads to PostgreSQL with date)
- ✅ Added: `find_common_articles` task (finds sustained trends)
- ✅ Updated: Branch now returns `load_timeseries` instead of `run_analytics`

### ✅ What Stayed the Same
- ✅ Still checks Diamond bucket for missing dates
- ✅ Still triggers main pipeline for gaps
- ✅ Still waits for data to be ready
- ✅ Still runs daily at 3 AM

### ✅ What's Better
- 📊 Direct PostgreSQL access (no need for separate analytics script)
- 📅 Date column for easy time-series queries
- 🔥 Separate table for sustained trends (filter out viral spikes)
- 🚀 Simpler architecture (fewer dependencies)

---

## Monitoring

### Check DAG Status
```bash
docker exec -it airflow-scheduler airflow dags list-runs -d wiki_smart_analytics_orchestrator
```

### View Task Logs
```bash
# Check completeness
docker exec -it airflow-scheduler airflow tasks logs wiki_smart_analytics_orchestrator check_completeness latest

# Load timeseries
docker exec -it airflow-scheduler airflow tasks logs wiki_smart_analytics_orchestrator load_timeseries latest

# Find common
docker exec -it airflow-scheduler airflow tasks logs wiki_smart_analytics_orchestrator find_common_articles latest
```

---

## Example Analysis

### Track Article Growth Over Time
```sql
SELECT 
    date,
    page_display,
    views,
    rank,
    LAG(views) OVER (PARTITION BY page ORDER BY date) as prev_views,
    views - LAG(views) OVER (PARTITION BY page ORDER BY date) as daily_change
FROM wiki_timeseries
WHERE page = 'ChatGPT'
ORDER BY date;
```

### Find Breakout Articles (Sudden Spikes)
```sql
SELECT 
    page_display,
    MIN(views) as min_views,
    MAX(views) as max_views,
    MAX(views) / NULLIF(MIN(views), 0) as spike_ratio
FROM wiki_timeseries
GROUP BY page_display
HAVING MAX(views) > MIN(views) * 2  -- At least 2x spike
ORDER BY spike_ratio DESC
LIMIT 20;
```

### Compare Sustained vs. Spike Trends
```sql
-- Articles in common (sustained) vs all timeseries
WITH common AS (
    SELECT DISTINCT page FROM wiki_common_articles
),
all_articles AS (
    SELECT DISTINCT page FROM wiki_timeseries
)
SELECT 
    (SELECT COUNT(*) FROM common) as sustained_trends,
    (SELECT COUNT(*) FROM all_articles) as total_articles,
    ROUND(100.0 * (SELECT COUNT(*) FROM common) / (SELECT COUNT(*) FROM all_articles), 2) as sustained_pct;
```

---

## Summary

✅ **wiki_timeseries** - All data with dates (time-series analysis)  
✅ **wiki_common_articles** - Sustained trends only (filter spikes)  
✅ **Automatic backfilling** - Fills missing dates  
✅ **Daily updates** - Runs at 3 AM automatically  
✅ **PostgreSQL native** - Direct SQL queries, no Python scripts needed  

**Ready to use!** 🚀
