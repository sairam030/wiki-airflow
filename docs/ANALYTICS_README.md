# Wikipedia Trends Analytics Module

**Separate analytics system for tracking Wikipedia article trends over time.**

This module analyzes historical data from your Diamond bucket to identify:
- 📈 Growing/declining articles
- 🔄 Articles consistently trending (present in all days)
- 📊 Category-level trends
- 🌍 Geographic trends
- 📉 Volatility metrics

## Features

### 1. **Time Series Analysis**
- Tracks article views, ranks across multiple days
- Stores complete history in `wiki_trends_timeseries` table

### 2. **Common Articles Tracking**
- Identifies articles present in ALL analyzed days
- Stored in `wiki_trends_common` table
- Useful for finding consistently trending topics

### 3. **Growth Metrics**
- Calculates view changes (absolute & percentage)
- Tracks rank improvements/declines
- Measures volatility (max - min views)
- Stored in `wiki_trends_growth` table

## Database Tables

### `wiki_trends_timeseries`
Complete time series data with date tracking:
```sql
- page, page_display, date
- views, rank, category
- location_type, location
- latitude, longitude
```

### `wiki_trends_common`
Articles present in all analyzed days:
```sql
- page, page_display, date
- views, rank, category, location
```

### `wiki_trends_growth`
Growth/decline metrics:
```sql
- page, page_display, category, location
- first_date, last_date, days_tracked
- first_views, last_views, views_change, views_change_pct
- first_rank, last_rank, rank_change
- avg_views, max_views, min_views, volatility
```

## Usage

### Option 1: Manual Execution (Recommended for testing)

```bash
# Run analytics for last 5 days (default)
python run_analytics.py

# Run analytics for last 7 days
python run_analytics.py 7

# Run analytics for last 3 days
python run_analytics.py 3
```

### Option 2: Airflow DAG (Scheduled)

The DAG `wiki_analytics_trends` is configured to run daily at 2 AM:

```bash
# Trigger manually
docker exec -it airflow-scheduler airflow dags trigger wiki_analytics_trends

# View status
docker exec -it airflow-scheduler airflow dags list | grep analytics
```

### Option 3: Python Import

```python
from plugins.utils.analytics_trends import run_analytics

# Run analytics
analytics_data = run_analytics(days=5)

# Access results
print(analytics_data['growth_metrics'])
print(analytics_data['common_articles'])
```

## Query Examples

### Top 10 Growing Articles
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

### Top 10 Declining Articles
```sql
SELECT 
    page_display,
    views_change,
    views_change_pct,
    first_views,
    last_views
FROM wiki_trends_growth 
ORDER BY views_change ASC 
LIMIT 10;
```

### Most Volatile Articles (Trending Spikes)
```sql
SELECT 
    page_display,
    volatility,
    max_views,
    min_views,
    avg_views
FROM wiki_trends_growth 
ORDER BY volatility DESC 
LIMIT 10;
```

### Category-Level Trends
```sql
SELECT 
    category,
    COUNT(*) as article_count,
    AVG(views_change_pct) as avg_growth_pct,
    SUM(CASE WHEN views_change > 0 THEN 1 ELSE 0 END) as growing_count,
    SUM(CASE WHEN views_change < 0 THEN 1 ELSE 0 END) as declining_count
FROM wiki_trends_growth 
GROUP BY category 
ORDER BY avg_growth_pct DESC;
```

### Articles Present All Days (Sustained Trends)
```sql
SELECT COUNT(DISTINCT page) as sustained_trending_articles
FROM wiki_trends_common;

-- Top sustained trending articles
SELECT 
    page_display,
    AVG(views) as avg_views,
    AVG(rank) as avg_rank,
    category
FROM wiki_trends_common
GROUP BY page_display, category
ORDER BY avg_views DESC
LIMIT 20;
```

### Daily View Trends for Specific Article
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

### Geographic Trends
```sql
SELECT 
    location,
    COUNT(*) as article_count,
    AVG(views_change_pct) as avg_growth
FROM wiki_trends_growth 
WHERE location IS NOT NULL AND location != 'No specific location'
GROUP BY location 
HAVING COUNT(*) > 2
ORDER BY avg_growth DESC 
LIMIT 20;
```

### Rank Improvement Leaders
```sql
SELECT 
    page_display,
    first_rank,
    last_rank,
    rank_change,
    views_change_pct,
    category
FROM wiki_trends_growth 
WHERE rank_change > 0  -- Improved rank (lower number = better)
ORDER BY rank_change DESC 
LIMIT 20;
```

## Architecture

```
Diamond Bucket (MinIO)
    ↓
analytics_trends.py
    ↓
┌─────────────────────────────────┐
│  PostgreSQL Analytics Tables    │
├─────────────────────────────────┤
│  • wiki_trends_timeseries       │  ← All data with dates
│  • wiki_trends_common           │  ← Articles in all days
│  • wiki_trends_growth           │  ← Growth metrics
└─────────────────────────────────┘
```

## How It Works

1. **Data Collection**:
   - Scans Diamond bucket for last N days
   - Loads CSV files: `diamond_final_YYYY_MM_DD.csv`
   - Combines data with date tracking

2. **Common Articles Identification**:
   - Groups articles by page name
   - Counts unique dates per article
   - Filters articles present in ALL days

3. **Metrics Calculation**:
   - Views change (absolute & percentage)
   - Rank change (positive = improved)
   - Volatility (max - min views)
   - Average, max, min views

4. **Database Loading**:
   - Creates analytics tables (if not exist)
   - Loads timeseries data
   - Loads common articles
   - Loads growth metrics
   - Uses UPSERT (ON CONFLICT UPDATE)

## Requirements

- Python 3.8+
- pandas
- boto3
- psycopg2
- Existing Diamond bucket data (from main pipeline)

## No Changes to Main Pipeline

✅ **Completely separate** from `wiki_trending_pipeline` DAG  
✅ **Read-only** access to Diamond bucket  
✅ **Independent tables** (won't affect `wiki_diamond`)  
✅ **Can run anytime** without disrupting main pipeline  

## Example Output

```
================================================================================
📊 WIKIPEDIA TRENDS ANALYTICS
================================================================================
📅 Found 5 files from last 5 days:
   • 2025-11-12: diamond_final_2025_11_12.csv/part-00000.csv
   • 2025-11-11: diamond_final_2025_11_11.csv/part-00000.csv
   • 2025-11-10: diamond_final_2025_11_10.csv/part-00000.csv
   • 2025-11-09: diamond_final_2025_11_09.csv/part-00000.csv
   • 2025-11-08: diamond_final_2025_11_08.csv/part-00000.csv

✓ Combined data: 4975 total records across 5 days

🔍 Finding articles present in all days...
✓ Found 523 articles present in all 5 days

📈 Calculating growth metrics...
✓ Calculated metrics for 523 articles

📊 TREND ANALYSIS SUMMARY
----------------------------------------------------------------------
Date Range: 2025-11-08 to 2025-11-12
Total Articles Tracked: 523

Top Growing Articles:
                    page_display  views_change  views_change_pct
456              Breaking_News      +125000          +85.3
234           Tech_Conference       +98000          +72.1
789        Political_Election       +87000          +65.4

Top Declining Articles:
                    page_display  views_change  views_change_pct
123           Yesterday_News       -45000          -32.1
567          Fading_Celebrity      -38000          -28.7
901          Old_News_Event        -29000          -24.5

✅ Loaded 4975 timeseries records
✅ Loaded 2615 common article records
✅ Loaded 523 growth metrics
```

## Troubleshooting

### No files found in Diamond bucket
```bash
# Check bucket contents
docker exec -it minio mc ls minio/diamond/

# Run main pipeline first to generate data
docker exec -it airflow-scheduler airflow dags trigger wiki_trending_pipeline
```

### Database connection errors
```bash
# Verify PostgreSQL is running
docker ps | grep postgres

# Check connection from container
docker exec -it airflow-scheduler python -c "import psycopg2; conn = psycopg2.connect(host='postgres', database='airflow', user='airflow', password='airflow'); print('✓ Connected')"
```

### Import errors
```bash
# Run from project root
cd /home/ram/wiki-trends-pipeline
python run_analytics.py
```

## Future Enhancements

- 📊 Grafana dashboards for visualization
- 📧 Email alerts for unusual trends
- 🤖 ML-based anomaly detection
- 📱 REST API for analytics queries
- 📈 Predictive trending (forecast next day's trends)

## License

Same as main project.
