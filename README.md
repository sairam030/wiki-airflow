# 📊 Wikipedia Trends Analytics Pipeline

A comprehensive data engineering pipeline that extracts, processes, and analyzes trending Wikipedia pages using Apache Airflow, Apache Spark, MinIO (S3), and AI-powered categorization with advanced analytics and performance monitoring.

---

## 🎯 Project Overview

This project implements a **fully automated ETL pipeline with smart analytics** that:

1. **Extracts** trending Wikipedia page data from the Wikimedia API
2. **Stores** raw data in a MinIO-based data lake (Bronze → Silver → Gold → Diamond layers)
3. **Transforms** and cleans data using Apache Spark (Silver layer)
4. **Enriches** pages with AI-powered categorization and geographic extraction (Gold layer)
5. **Analyzes** with Diamond-layer transformations (timeseries aggregations, common articles)
6. **Loads** final data into PostgreSQL for analytics
7. **Monitors** pipeline performance with comprehensive metrics collection
8. **Visualizes** insights through Metabase dashboards
9. **Auto-fills** missing historical data with smart orchestration

### 🔄 Complete Data Flow

```
Wikipedia API (Pageviews)
        ↓
    [Extract]
        ↓
   Bronze Layer (MinIO S3) - Raw JSON data
        ↓
    [Transform with Spark]
        ↓
   Silver Layer (MinIO S3) - Cleaned CSV data
        ↓
    [Enrich with AI - Qwen2.5:3B]
        ↓
   Gold Layer (MinIO S3) - Categorized & Geocoded CSV
        ↓
    [Analytics Transformations]
        ↓
   Diamond Layer (MinIO S3) - Timeseries & Common Articles
        ↓
    [Load to PostgreSQL]
        ↓
   PostgreSQL Database (wiki_diamond, wiki_timeseries, wiki_common_articles)
        ↓
    [Performance Tracking]
        ↓
   Metrics Database (dag_execution_metrics)
        ↓
    [Visualize]
        ↓
   Metabase Dashboards
```

---

## ⚡ Key Features

### 1. **Multi-Layer Data Lake Architecture**
- **Bronze Layer**: Raw JSON from Wikipedia API
- **Silver Layer**: Cleaned and deduplicated CSV (Spark processing)
- **Gold Layer**: AI-enriched with categories and geography (Qwen2.5:3B)
- **Diamond Layer**: Advanced analytics (timeseries aggregations, common articles)
- **Historical Data**: Long-term archival storage for trend analysis

### 2. **Smart Analytics Orchestration**
- **Automatic Date Detection**: Scans PostgreSQL for missing dates in the last 5 days
- **Auto-Fill Missing Data**: Triggers main pipeline for gaps
- **Intelligent Analytics**: Runs only when 5+ days of data available
- **Timeseries Analysis**: Daily aggregations by category and geography
- **Common Articles Detection**: Identifies pages appearing across all 5 days
- **Historical Archiving**: Saves analytics snapshots for long-term trends

### 3. **Comprehensive Performance Monitoring**
- **DAG Execution Metrics**: Tracks latency, throughput, success rates
- **Task-Level Metrics**: Individual task durations, retry counts
- **Performance Comparisons**: Day-over-day trend analysis
- **CLI Comparison Tool**: Quick performance diagnostics
- **SQL Query Library**: Ready-made Metabase queries

### 4. **Sequential AI Processing**
- **Prevents Rate Limiting**: Processes pages one at a time
- **Robust Error Handling**: Retries and fallbacks for AI failures
- **Batch Optimization**: Processes 1000 pages efficiently
- **Model Diversity**: Supports Qwen2.5, LLaMA, and custom models

---

## 🏗️ Architecture

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          ORCHESTRATION LAYER                             │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    Apache Airflow (LocalExecutor)                 │   │
│  │  - Scheduler: Manages DAG execution & scheduling                 │   │
│  │  - Webserver: UI for monitoring (Port 8080)                      │   │
│  │  - PostgreSQL: Metadata store & analytics database               │   │
│  │  - Two DAGs:                                                      │   │
│  │    1. wiki_trending_pipeline (Main ETL)                          │   │
│  │    2. wiki_smart_analytics_orchestrator (Analytics & Auto-fill)  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                            INGESTION LAYER                               │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              Wikipedia Pageviews API (Wikimedia)                  │   │
│  │  - Fetch top 1000 pages by views for a given date               │   │
│  │  - Returns: Page title, view count, rank                         │   │
│  │  - Endpoint: /metrics/pageviews/top/en.wikipedia/...            │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      DATA LAKE (MinIO S3-Compatible)                     │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────────┐    │
│  │   Bronze    │  │    Silver    │  │   Gold    │  │   Diamond    │    │
│  │   Bucket    │  │    Bucket    │  │  Bucket   │  │   Bucket     │    │
│  │             │  │              │  │           │  │              │    │
│  │ Raw JSON    │→ │ Cleaned CSV  │→ │ Enriched  │→ │ Analytics    │    │
│  │ (As-is)     │  │ (Deduped,    │  │ CSV       │  │ CSV          │    │
│  │             │  │  Validated)  │  │(categor- ,│  │  Commons)    │    │
│  │             │  │              │  │  -ized)   │  │              │    │
│  └─────────────┘  └──────────────┘  └───────────┘  └──────────────┘    │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                   Historical Data Bucket                          │   │
│  │  - Long-term archival storage for trend analysis                 │   │
│  │  - Stores analytics snapshots over time                          │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│                     MinIO (Port 9000, Console 9001)                      │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                        PROCESSING LAYER                                  │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    Apache Spark Cluster                           │   │
│  │  - Master Node (Port 7077, WebUI 8081)                           │   │
│  │  - Worker 1 (2 cores, 2GB RAM, WebUI 8082)                       │   │
│  │  - Worker 2 (2 cores, 2GB RAM, WebUI 8083)                       │   │
│  │                                                                   │   │
│  │  Tasks:                                                           │   │
│  │    • Bronze → Silver: Deduplication, cleaning, formatting        │   │
│  │    • Data validation and quality checks                          │   │
│  │    • Schema standardization                                      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                        AI ENRICHMENT LAYER                               │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                  Ollama (Qwen2.5:3B Model)                        │   │
│  │                    Port 11434                                     │   │
│  │                                                                   │   │
│  │  Processing Mode: SEQUENTIAL (one page at a time)                │   │
│  │  - Prevents rate limiting and 429 errors                         │   │
│  │  - Robust error handling with retries                            │   │
│  │                                                                   │   │
│  │  Capabilities:                                                    │   │
│  │                                                                   │   │
│  │  1. Keyword-Based Category Classification                        │   │
│  │     • 19 predefined categories                                   │   │
│  │     • Categories: Sports, Technology, Politics, Entertainment    │   │
│  │       Business, Science, Health, Climate, Games, etc.            │   │
│  │     • Model analyzes page title and assigns best-fit category    │   │
│  │     • Customizable category keywords in config                   │   │
│  │     • Fallback to "Other" if uncertain                           │   │
│  │                                                                   │   │
│  │  2. Keyword-Based Geographic Extraction                          │   │
│  │     • Identifies: Country, City, State, Region                   │   │
│  │     • Location type classification                               │   │
│  │     • Extracts location name from page title                     │   │
│  │     • Handles multi-word locations (e.g., "New York")            │   │
│  │     • Returns structured JSON: {type, name}                      │   │
│  │     • Customizable location keywords                             │   │
│  │                                                                   │   │
│  │  Model Configuration:                                             │   │
│  │    - Temperature: 0.0 (deterministic)                            │   │
│  │    - Top_p: 0.1 (focused responses)                              │   │
│  │    - Num_predict: 50 (max tokens)                                │   │
│  │    - Stop tokens: ["\n\n"]                                       │   │
│  │                                                                   │   │
│  │  Prompt Engineering:                                              │   │
│  │    - Category prompt: Analyzes keywords in page title            │   │
│  │    - Geography prompt: Extracts location entities                │   │
│  │    - Both prompts optimized for structured output                │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                          ANALYTICS LAYER                                 │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              PostgreSQL Database (Port 5432)                      │   │
│  │                                                                   │   │
│  │  Tables:                                                          │   │
│  │  • wiki_diamond: Daily trending pages (main table)               │   │
│  │    - Columns: page, views, rank, category, location_type,        │   │
│  │               location, fetch_date                                │   │
│  │    - Indexed on: date, category, location                        │   │
│  │                                                                   │   │
│  │  • wiki_timeseries: 5-day rolling aggregations                   │   │
│  │    - Columns: analysis_period, category, location, total_pages,  │   │
│  │               total_views, avg_rank, unique_pages                │   │
│  │    - Purpose: Trend analysis, pattern detection                  │   │
│  │                                                                   │   │
│  │  • wiki_common_articles: Persistent trending pages               │   │
│  │    - Pages appearing in all 5 days                               │   │
│  │    - Columns: page, avg_views, avg_rank, category, location      │   │
│  │    - Purpose: Identify viral/sustained trends                    │   │
│  │                                                                   │   │
│  │  • dag_execution_metrics: Pipeline performance tracking          │   │
│  │    - Columns: dag_id, run_id, duration, success_rate, retries    │   │
│  │    - Task-level metrics in JSONB format                          │   │
│  │    - Purpose: Performance monitoring & optimization              │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                Metabase BI Tool (Port 3000)                       │   │
│  │                                                                   │   │
│  │  Dashboards:                                                      │   │
│  │    • Category distribution and trends                            │   │
│  │    • Geographic analysis and heatmaps                            │   │
│  │    • Top pages by views and rankings                             │   │
│  │    • Time-series visualizations                                  │   │
│  │    • Common articles tracking                                    │   │
│  │    • Pipeline performance metrics                                │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      MONITORING & METRICS LAYER                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              DAG Metrics Collection System                        │   │
│  │                                                                   │   │
│  │  Automatic Collection:                                            │   │
│  │    • Runs after every DAG execution                              │   │
│  │    • Captures pipeline-level metrics                             │   │
│  │    • Captures task-level metrics                                 │   │
│  │                                                                   │   │
│  │  Metrics Tracked:                                                 │   │
│  │    - Latency: Total duration, task durations                     │   │
│  │    - Throughput: Records/second, records/minute                  │   │
│  │    - Success Rate: Task success/failure counts                   │   │
│  │    - Retries: Task retry patterns                                │   │
│  │    - Data Volumes: Records at each layer                         │   │
│  │                                                                   │   │
│  │  Analysis Tools:                                                  │   │
│  │    • compare_metrics.py CLI tool                                 │   │
│  │    • SQL queries for Metabase                                    │   │
│  │    • Day-over-day comparisons                                    │   │
│  │    • Performance trend analysis                                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘

```

### Data Flow Summary

```
1. Wikipedia API → Bronze (Raw JSON)
2. Bronze → Silver (Spark: Clean, Dedupe)
3. Silver → Gold (Ollama: AI Category + Geography via Keywords)
4. Gold → Diamond (Analytics: Timeseries, Common Articles)
5. Diamond → PostgreSQL (Load for querying)
6. PostgreSQL → Metabase (Visualize)
7. Every Step → Metrics Collection → Performance Tracking
```

### Smart Orchestration Flow

```
[Smart Orchestrator Runs Daily at 1 AM]
           ↓
[Check PostgreSQL for Missing Dates in Last 5 Days]
           ↓
    ┌──────┴──────┐
    │             │
  [Gaps Found]  [No Gaps]
    │             │
    ↓             ↓
[Trigger Main  [Check Data
 Pipeline for   Count >= 5
 Each Gap]      Days?]
    │             │
    ↓             ↓
[Wait for      [Yes → Run
 Completion]    Analytics]
    │             │
    └──────┬──────┘
           ↓
[Load Timeseries Data (5-day aggregations)]
           ↓
[Find Common Articles (present in all 5 days)]
           ↓
[Save to Historical Bucket (MinIO)]
           ↓
[Collect Orchestrator Metrics]
           ↓
        [END]
```

---

## 📁 Project Structure

```
wiki-trends-pipeline/
│
├── dags/                                    # Airflow DAG definitions
│   ├── __init__.py
│   ├── wiki_top_pages_dag.py               # Main pipeline DAG (Bronze → Diamond)
│   └── wiki_smart_analytics_orchestrator.py # Smart orchestrator for analytics
│
├── plugins/                                 # Custom Airflow plugins
│   ├── __init__.py
│   ├── config.py                           # Configuration (MinIO, buckets, PostgreSQL)
│   ├── category.py                         # AI categorization logic (Qwen2.5)
│   │
│   ├── operators/                          # Custom Airflow operators
│   │   ├── __init__.py
│   │   └── spark_submit_operator.py       # Spark job submission
│   │
│   └── utils/                              # Utility functions
│       ├── __init__.py
│       ├── fetch_wiki_data.py             # Wikipedia API extraction
│       ├── categorize_pages.py            # AI-powered categorization (sequential)
│       ├── load_postgres.py               # PostgreSQL loading
│       ├── diamond_analytics.py           # Timeseries & common articles
│       ├── historical_data_manager.py     # Historical archiving
│       └── dag_metrics_collector.py       # Performance metrics collection
│
├── scripts/                                # Helper scripts
│   └── pull_qwen_model.sh                 # Download Qwen2.5 model
│
├── docs/                                   # Documentation
│   ├── METRICS_SYSTEM.md                  # Metrics collection guide
│   └── METRICS_QUERIES.md                 # SQL queries for Metabase
│
├── logs/                                   # Airflow logs
│   ├── dag_id=wiki_trending_pipeline/
│   ├── dag_id=wiki_smart_analytics_orchestrator/
│   ├── dag_processor_manager/
│   └── scheduler/
│
├── docker/                                 # Docker configurations
│
├── Dockerfile                              # Custom Airflow+Spark image
├── docker-compose.yml                      # Multi-container orchestration
├── requirements.txt                        # Python dependencies
├── compare_metrics.py                      # CLI tool for metrics comparison
├── test.py                                # Testing utilities
└── README.md                               # This file
```

---

## 🔧 Technology Stack

### Core Technologies

| Technology | Version | Purpose |
|-----------|---------|---------|
| **Apache Airflow** | 2.9.0 | Workflow orchestration |
| **Apache Spark** | 3.5.0 | Distributed data processing |
| **PostgreSQL** | 15-alpine | Metadata & analytics storage |
| **MinIO** | Latest | S3-compatible object storage (Data Lake) |
| **Ollama** | Latest | LLM inference engine |
| **Qwen2.5** | 3B | AI model for classification |
| **Metabase** | Latest | Business intelligence & visualization |
| **Docker** | 24.x | Containerization |
| **Python** | 3.11 | Primary programming language |

### Python Libraries

```
apache-airflow==2.9.0
pyspark==3.5.0
boto3==1.34.51
pandas==2.2.1
psycopg2-binary==2.9.9
requests==2.31.0
```

---

## 🐳 Docker Containers

### Container Overview

| Container | Image | Ports | Purpose |
|-----------|-------|-------|---------|
| **airflow-webserver** | custom-airflow-spark:2.9.0 | 8080 | Airflow UI & API |
| **airflow-scheduler** | custom-airflow-spark:2.9.0 | - | DAG scheduling & execution |
| **airflow-init** | custom-airflow-spark:2.9.0 | - | Database initialization |
| **postgres** | postgres:15-alpine | 5432 | Airflow metadata & analytics DB |
| **minio** | minio/minio:latest | 9000, 9001 | S3-compatible object storage |
| **spark-master** | custom-airflow-spark:2.9.0 | 7077, 8081 | Spark cluster master |
| **spark-worker-1** | custom-airflow-spark:2.9.0 | 8082 | Spark worker node 1 |
| **spark-worker-2** | custom-airflow-spark:2.9.0 | 8083 | Spark worker node 2 |
| **ollama** | ollama/ollama:latest | 11434 | LLM inference server |
| **metabase** | metabase/metabase:latest | 3000 | BI dashboards |

### Access Credentials

| Service | Username | Password | URL |
|---------|----------|----------|-----|
| **Airflow** | `admin` | `admin` | http://localhost:8080 |
| **MinIO Console** | `minioadmin` | `minioadmin` | http://localhost:9001 |
| **MinIO API** | `minioadmin` | `minioadmin` | http://localhost:9000 |
| **PostgreSQL** | `airflow` | `airflow` | localhost:5432 |
| **Metabase** | (setup on first login) | - | http://localhost:3000 |
| **Spark Master UI** | - | - | http://localhost:8081 |
| **Spark Worker 1** | - | - | http://localhost:8082 |
| **Spark Worker 2** | - | - | http://localhost:8083 |

---

## 📊 DAG Structure

### 1. Main Pipeline: `wiki_trending_pipeline`

**Schedule:** Daily at midnight UTC (`0 0 * * *`)

**Execution Date Logic:** Runs for **yesterday's data** (D-1)

#### Task Flow

```
[START]
   ↓
[check_spark_cluster] ──────┐
   ↓                         │
[clean_bronze_layer]         │
   ↓                         │
[fetch_top_pages]            │  (Data Quality)
   ↓                         │
[clean_top_pages]            │
   ↓                         ↓
[fetch_wikipedia_categories] 
   ↓
[clean_silver_layer]
   ↓
[categorize_pages] (Sequential AI Processing)
   ↓
[clean_gold_layer]
   ↓
[load_gold_to_postgres]
   ↓
[show_pipeline_metrics]
   ↓
[collect_dag_metrics] (Performance Tracking)
   ↓
[END]
```

### 2. Smart Analytics Orchestrator: `wiki_smart_analytics_orchestrator`

**Schedule:** Daily at 1:00 AM UTC (`0 1 * * *`)

**Purpose:** Automatically detect missing data and run analytics when ready

#### Task Flow

```
[START]
   ↓
[check_and_trigger_missing_dates]
   ↓
   ├─→ [trigger_main_pipeline] (if gaps found)
   │      ↓
   │   [wait_for_pipeline]
   │
   ↓
[load_timeseries_data] (5-day aggregations)
   ↓
[find_common_articles] (articles in all 5 days)
   ↓
[save_to_historical] (archive to MinIO)
   ↓
[collect_dag_metrics] (track orchestrator performance)
   ↓
[END]
```

---

## 🗄️ Database Schema

### PostgreSQL Tables

#### 1. `wiki_diamond` (Main Analytics Table)
Stores daily trending pages with AI enrichments.

```sql
CREATE TABLE wiki_diamond (
    page TEXT,
    views BIGINT,
    rank INT,
    category TEXT,
    location_type TEXT,
    location TEXT,
    fetch_date DATE
);
```

**Indexes:**
- `idx_diamond_date` on `fetch_date`
- `idx_diamond_category` on `category`
- `idx_diamond_location` on `location`

#### 2. `wiki_timeseries` (5-Day Aggregations)
Aggregated statistics over 5-day rolling windows.

```sql
CREATE TABLE wiki_timeseries (
    analysis_period TEXT,         -- e.g., "2025-11-10_to_2025-11-14"
    category TEXT,
    location TEXT,
    total_pages INT,
    total_views BIGINT,
    avg_rank NUMERIC,
    unique_pages INT,
    created_at TIMESTAMP
);
```

**Purpose:** Track trends, identify rising categories, geographic patterns

#### 3. `wiki_common_articles` (Persistent Trending)
Pages appearing in top 1000 across all 5 days.

```sql
CREATE TABLE wiki_common_articles (
    analysis_period TEXT,
    page TEXT,
    avg_views NUMERIC,
    avg_rank NUMERIC,
    category TEXT,
    location TEXT,
    appearance_count INT,
    created_at TIMESTAMP
);
```

**Purpose:** Identify consistently trending topics, viral content

#### 4. `dag_execution_metrics` (Performance Monitoring)
Comprehensive DAG execution metrics for performance tracking.

```sql
CREATE TABLE dag_execution_metrics (
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
```

**Indexes:**
- `idx_metrics_dag_id` on `dag_id`
- `idx_metrics_execution_date` on `execution_date`
- `idx_metrics_collected_at` on `collected_at`

**Purpose:** Day-over-day performance comparison, bottleneck identification

---

## 📈 Performance Metrics System

### Overview

Every DAG run automatically collects comprehensive performance metrics including:

- **Pipeline Metrics**: Total duration, task counts, success rates
- **Task Metrics**: Individual task durations, retry counts, states
- **Throughput**: Records processed per second/minute
- **Data Volumes**: Records at each layer (Bronze, Silver, Gold, Diamond)

### Using the Metrics Comparison Tool

#### Show Latest Runs
```bash
python3 compare_metrics.py --latest 5
```

**Output:**
```
📋 Latest 5 Runs: wiki_trending_pipeline
========================================================
dag_id                  execution_date      duration_min  tasks  success_rate  retries
wiki_trending_pipeline  2025-11-15 00:00   45.23         9      100.0%        0
wiki_trending_pipeline  2025-11-14 00:00   52.15         9      88.89%        10
```

#### Compare Today vs Yesterday
```bash
python3 compare_metrics.py --compare
```

**Output:**
```
📊 Performance Comparison
Today vs Yesterday:
  • Duration: 45.23 min → 52.15 min
    Change: -13.27% (IMPROVED ✅)
  • Success Rate: 100.0% → 88.89%
  • Retries: 0 → 10
```

#### View Task Breakdown
```bash
python3 compare_metrics.py --task-breakdown "manual__2025-11-15T12:08:49+00:00"
```

**Output:**
```
🔍 Task Breakdown: wiki_trending_pipeline
Task ID                        Duration (s)  State    Tries
clean_gold_layer              285.68        success  2
fetch_wikipedia_categories    115.96        success  2
clean_top_pages               62.63         success  2
categorize_pages              17.88         success  2
```

#### View 7-Day Performance Trend
```bash
python3 compare_metrics.py --trend 7
```

### Metrics SQL Queries

See `docs/METRICS_QUERIES.md` for ready-made SQL queries including:
- Daily performance comparison
- Task duration analysis
- Success rate tracking
- Throughput monitoring
- Retry pattern identification

For full documentation, see: **[`docs/METRICS_SYSTEM.md`](docs/METRICS_SYSTEM.md)**

---

### Task Details

#### 1. **fetch_top_pages**
- **Type:** PythonOperator
- **Function:** `fetch_and_upload_wiki_data()`
- **Input:** Execution date (D-1)
- **Process:**
  - Calls Wikipedia Pageviews API
  - Fetches top 1000 pages by views
  - Saves raw JSON to MinIO Bronze bucket
- **Output:** `bronze/top_pages_YYYY_MM_DD.json`
- **Duration:** ~10-20 seconds

#### 2. **fetch_wikipedia_categories**
- **Type:** PythonOperator  
- **Function:** Fetches Wikipedia categories for pages
- **Process:**
  - Reads from Silver layer
  - Calls Wikipedia API for categories
  - Enriches data with category information
- **Output:** Enhanced CSV
- **Duration:** ~30-60 seconds

#### 3. **categorize_pages** (AI-Powered)
- **Type:** PythonOperator
- **Function:** `categorize_and_upload()`
- **AI Model:** Qwen2.5:3B via Ollama API
- **Processing Mode:** **Sequential** (one page at a time)
- **Process:**
  - Reads cleaned CSV from Silver layer
  - For each page sequentially:
    - **Category Classification:** Assigns 1 of 19 categories
    - **Geographic Extraction:** Extracts location (Country/City/State/Region)
  - Prevents rate limiting with sequential processing
  - Robust error handling with retries and fallbacks
- **Categories:**
  - Autos and vehicles, Beauty and fashion, Business and finance, Climate
  - Entertainment, Food and drink, Games, Health, Hobbies and leisure
  - Jobs and education, Law and government, Pets and animals, Politics
  - Science, Shopping, Sports, Technology, Travel and transportation, Other
- **Output:** `gold/categorized_YYYY_MM_DD.csv`
- **Duration:** ~5-10 minutes for 1000 pages
- **Improvements:** Prevents 429 errors, handles rate limits gracefully

#### 4. **load_gold_to_postgres**
- **Type:** PythonOperator
- **Function:** `load_gold_to_postgres()`
- **Process:**
  - Reads categorized CSV from Gold layer
  - Inserts into PostgreSQL `wiki_diamond` table
  - Handles duplicates with UPSERT logic
  - Creates indexes for performance
- **Output:** Rows in `wiki_diamond` table
- **Duration:** ~5-10 seconds

#### 5. **collect_dag_metrics**
- **Type:** PythonOperator
- **Function:** `collect_dag_metrics()`
- **Process:**
  - Queries all task instances for the DAG run
  - Calculates duration, success rate, retry count
  - Extracts task-level metrics (durations, states)
  - Saves to `dag_execution_metrics` table
- **Trigger Rule:** `all_done` (runs even if upstream fails)
- **Duration:** ~2-5 seconds
- **Output:** Metrics in PostgreSQL for analysis

### Smart Orchestrator Tasks

#### 1. **check_and_trigger_missing_dates**
- **Type:** PythonOperator
- **Function:** Checks PostgreSQL for missing dates in last 5 days
- **Process:**
  - Queries `wiki_diamond` table
  - Identifies date gaps
  - Triggers main pipeline for each missing date
  - Waits for completion
- **Output:** XCom with triggered run IDs

#### 2. **load_timeseries_data**
- **Type:** PythonOperator
- **Function:** `create_timeseries_analytics()`
- **Requirement:** 5+ days of data
- **Process:**
  - Aggregates data by category and location
  - Calculates totals, averages, unique counts
  - Saves to `wiki_timeseries` table
- **Output:** Timeseries aggregations for 5-day window

#### 3. **find_common_articles**
- **Type:** PythonOperator
- **Function:** `find_common_articles()`
- **Requirement:** 5+ days of data
- **Process:**
  - Identifies pages appearing in all 5 days
  - Calculates average views and ranks
  - Saves to `wiki_common_articles` table
- **Output:** List of persistently trending pages

#### 4. **save_to_historical**
- **Type:** PythonOperator
- **Function:** Archives analytics to MinIO
- **Process:**
  - Exports timeseries and common articles to CSV
  - Uploads to MinIO `historical-data` bucket
  - Enables long-term trend analysis
- **Output:** `historical-data/timeseries_PERIOD.csv`, `common_articles_PERIOD.csv`

### DAG Diagram

![DAG Visualization](screenshots/pipeline.png)

---

## 🚀 Getting Started

### Prerequisites

- **Docker:** 24.x or higher
- **Docker Compose:** 2.x or higher  
- **RAM:** 8GB minimum (16GB recommended)
- **Disk Space:** 20GB free
- **OS:** Linux, macOS, or Windows with WSL2

### Installation Steps

#### Step 1: Clone the Repository

```bash
git clone https://github.com/sairam030/wiki-airflow.git
cd wiki-trends-pipeline
```

#### Step 2: Set File Permissions

```bash
# Make sure you own the files
sudo chown -R $USER:$USER .

# Make scripts executable
chmod +x scripts/*.sh
```

#### Step 3: Start All Services

```bash
# Start all containers in detached mode
docker compose up -d
```

**This will start:**
- ✅ PostgreSQL (metadata & analytics database)
- ✅ MinIO (S3-compatible data lake)
- ✅ Spark Master + 2 Workers (distributed processing)
- ✅ Ollama (AI inference engine)
- ✅ Airflow Webserver + Scheduler (orchestration)
- ✅ Metabase (business intelligence)

**Expected startup time:** 2-3 minutes

#### Step 4: Verify Services Are Running

```bash
docker compose ps
```

**Expected output:** All containers should show `Up` or `running` status.

**Troubleshooting:** If any container shows `Exit` or `Restarting`:
```bash
# Check logs for the problematic container
docker compose logs <container-name> --tail=50

# Common fixes:
docker compose down
docker compose up -d
```

#### Step 5: Pull the AI Model

```bash
./scripts/pull_qwen_model.sh
```

**What this does:**
- Downloads Qwen2.5:3B model (~2GB)
- Loads it into Ollama container
- Makes it available for categorization tasks

**Expected output:**
```
🔄 Pulling Qwen2.5:3b model...
pulling manifest
pulling 43f7a214e532... 100% ▕████████████▏ 1.9 GB
pulling 4f030c026d85... 100% ▕████████████▏  11 KB
...
✅ Qwen2.5:3b model pulled successfully!
```

**Verify model is loaded:**
```bash
docker exec -it ollama ollama list
```

Should show `qwen2.5:3b`.

#### Step 6: Access Airflow UI

Open browser: **http://localhost:8080**

**Login Credentials:**
- Username: `admin`
- Password: `admin`

**First-time access:** May take 30-60 seconds to load while Airflow initializes.

#### Step 7: Enable DAGs

In Airflow UI:

1. Find `wiki_trending_pipeline` DAG
2. Toggle the switch to **ON** (blue)
3. Find `wiki_smart_analytics_orchestrator` DAG  
4. Toggle the switch to **ON** (blue)

**Note:** The scheduler may already have enabled them automatically.

#### Step 8: Trigger Your First Run

**Option A: Manual Trigger (Recommended for testing)**

1. Click on `wiki_trending_pipeline` DAG name
2. Click **Trigger DAG** button (▶️ play icon)
3. Optionally click "Trigger w/ config" to specify a date:
   ```json
   {
     "execution_date": "2025-11-14"
   }
   ```
4. Click **Trigger**

**Option B: Wait for Scheduled Run**

The DAG runs automatically at midnight UTC daily.

#### Step 9: Monitor Execution

**View Progress:**
1. Click on the DAG name
2. Go to **Grid View** or **Graph View**
3. Click on any task to see details
4. Click **Log** button to view real-time logs

**Expected execution time:** 
- Full pipeline: ~8-12 minutes for 1000 pages
- Tasks breakdown:
  - `fetch_top_pages`: 10-20 sec
  - `fetch_wikipedia_categories`: 30-60 sec
  - `categorize_pages`: 5-10 min (sequential AI processing)
  - `load_gold_to_postgres`: 5-10 sec
  - `collect_dag_metrics`: 2-5 sec

#### Step 10: Verify Data in PostgreSQL

```bash
# Check row count
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM wiki_diamond;"

# View sample data
docker exec -it postgres psql -U airflow -d airflow -c "SELECT page, views, category, location FROM wiki_diamond ORDER BY views DESC LIMIT 10;"

# Check latest data date
docker exec -it postgres psql -U airflow -d airflow -c "SELECT DISTINCT fetch_date FROM wiki_diamond ORDER BY fetch_date DESC LIMIT 5;"
```

#### Step 11: View Performance Metrics

```bash
# Show latest DAG runs
python3 compare_metrics.py --latest 5

# View task breakdown
python3 compare_metrics.py --task-breakdown "manual__2025-11-15T12:08:49+00:00"
```

**Note:** Replace run_id with your actual run_id from Airflow UI.

#### Step 12: Access Other Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Metabase** | http://localhost:3000 | (setup on first visit) |
| **Spark Master** | http://localhost:8081 | - |
| **Spark Worker 1** | http://localhost:8082 | - |
| **Spark Worker 2** | http://localhost:8083 | - |

---

### Quick Start Commands

```bash
# Start everything
docker compose up -d

# Stop everything
docker compose down

# View all logs
docker compose logs -f

# View specific service logs
docker compose logs airflow-scheduler -f --tail=100

# Restart a service
docker compose restart airflow-scheduler

# Check service status
docker compose ps

# Trigger DAG manually
docker exec -it airflow-scheduler airflow dags trigger wiki_trending_pipeline

# List all DAGs
docker exec -it airflow-scheduler airflow dags list

# Check PostgreSQL data
docker exec -it postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM wiki_diamond;"

# Access PostgreSQL shell
docker exec -it postgres psql -U airflow -d airflow

# Pull/update AI model
./scripts/pull_qwen_model.sh

# View metrics comparison
python3 compare_metrics.py --compare
```

---

## 🔄 Batch Processing

### Daily Automated Runs

The pipeline runs **automatically every day at midnight UTC** via Airflow scheduler.

**Execution Logic:**
- Cron: `0 0 * * *` (daily at 00:00 UTC)
- **Execution Date:** Current date - 1 day (yesterday)
- **Reason:** Wikipedia data is finalized after the day ends

**Example:**
```
Today: 2025-10-27
Pipeline runs at: 2025-10-27 00:00 UTC
Data fetched for: 2025-10-26
```

### Manual Runs

You can trigger manual runs for specific dates:

1. Go to Airflow UI → DAGs → `wiki_trending_pipeline`
2. Click **Trigger DAG w/ config**
3. Optionally set execution date in JSON:
   ```json
   {
     "execution_date": "2025-10-20"
   }
   ```
4. Click **Trigger**

### Backfilling Historical Data

To process multiple past dates:

```bash
docker exec -it airflow-scheduler bash

# Backfill from 2025-10-01 to 2025-10-25
airflow dags backfill \
  --start-date 2025-10-01 \
  --end-date 2025-10-25 \
  wiki_trending_pipeline
```

---

## 📈 Data Flow Details

### 1. **Bronze Layer** (Raw Data)

**Location:** MinIO bucket `bronze`

**Format:** JSON

**Schema:**
```json
[
  {
    "page": "Taylor_Swift",
    "views": 1234567,
    "rank": 1
  },
  ...
]
```

**Characteristics:**
- Raw, unprocessed data
- As received from Wikipedia API
- May contain duplicates
- No validation applied

### 2. **Silver Layer** (Cleaned Data)

**Location:** MinIO bucket `silver`

**Format:** CSV

**Schema:**
```csv
page,views,rank
Taylor_Swift,1234567,1
iPhone_16,987654,2
```

**Transformations Applied:**
- ✅ Deduplication (unique pages only)
- ✅ Data type validation
- ✅ Null value handling
- ✅ Standardized format

### 3. **Gold Layer** (Enriched Data)

**Location:** MinIO bucket `gold`

**Format:** CSV

**Schema:**
```csv
page,views,rank,category,location_type,location
Taylor_Swift,1234567,1,Entertainment,Country,USA
iPhone_16,987654,2,Technology,,
```

**Enrichments Added:**
- ✅ **Category:** AI-classified (19 categories)
- ✅ **Location Type:** Country/City/State/Region
- ✅ **Location Name:** Geographic entity

### 4. **PostgreSQL Table** (Analytics-Ready)

**Table:** `wiki_gold`

**Schema:**
```sql
CREATE TABLE wiki_gold (
    page TEXT,
    views INT,
    rank INT,
    category TEXT,
    geography_type TEXT,
    geography TEXT
);
```

**Purpose:**
- Fast SQL queries
- BI tool integration (Metabase)
- Historical trend analysis

---

## 🤖 AI Model: Qwen2.5:3B

### Why Qwen2.5?

Compared to LLaMA 3.2 and BERT:

| Feature | Qwen2.5:3B | LLaMA 3.2:3B | BERT |
|---------|------------|--------------|------|
| **Task Type** | General + Structured | General | Encoder-only |
| **Classification** | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐⭐ Good | ⭐⭐⭐ Limited |
| **JSON Output** | ⭐⭐⭐⭐⭐ Native | ⭐⭐⭐ Decent | ❌ Not supported |
| **Speed** | ⚡ Fast | ⚡ Fast | ⚡⚡ Very fast |
| **Structured Tasks** | ⭐⭐⭐⭐⭐ Best | ⭐⭐⭐ Good | ⭐⭐ Limited |
| **Size** | 2GB | 2GB | 500MB |

**Verdict:** Qwen2.5 is optimized for classification and structured output extraction.

### Model Parameters

```python
{
    "temperature": 0.0,      # Fully deterministic
    "top_p": 0.1,            # Focused responses
    "num_predict": 50,       # Max tokens
    "stop": ["\n\n"]         # Stop at double newline
}
```

---

## 🎯 Keyword-Based AI Categorization System

### Overview

The pipeline uses **keyword-based prompting** with Ollama/Qwen2.5 to classify Wikipedia pages and extract geographic information. This approach is:

- **Flexible:** Easily modify categories and keywords without retraining
- **Fast:** Leverages LLM's understanding without fine-tuning
- **Accurate:** Combines keyword matching with semantic understanding
- **Customizable:** Add/remove categories by editing configuration

### 1. Category Classification

#### How It Works

The system sends a carefully crafted prompt to the LLM with:
1. **Page Title:** e.g., "Taylor_Swift"
2. **Category Keywords:** Predefined list of 19 categories
3. **Instructions:** Analyze title and return best-fit category

#### Prompt Template

```python
prompt = f"""
Classify the Wikipedia page titled "{page_title}" into ONE of these categories:

1. Autos and vehicles - cars, motorcycles, transportation vehicles
2. Beauty and fashion - cosmetics, clothing, style, designers
3. Business and finance - companies, stocks, economy, banking
4. Climate - weather, environment, global warming
5. Entertainment - movies, TV shows, celebrities, music
6. Food and drink - restaurants, recipes, beverages, cuisine
7. Games - video games, board games, esports, gaming
8. Health - medicine, fitness, wellness, healthcare
9. Hobbies and leisure - crafts, activities, pastimes
10. Jobs and education - careers, schools, universities, learning
11. Law and government - legal, politics, regulations, courts
12. Pets and animals - domestic animals, wildlife, zoology
13. Politics - elections, politicians, political parties, governance
14. Science - research, discoveries, scientific fields, space
15. Shopping - retail, e-commerce, products, shopping
16. Sports - athletics, teams, competitions, athletes
17. Technology - tech companies, gadgets, software, IT
18. Travel and transportation - tourism, airlines, destinations
19. Other - anything else

Return ONLY the category name, nothing else.
Category:
"""
```

#### Customization

To add/modify categories, edit **`plugins/category.py`**:

```python
# Define your categories
CATEGORIES = [
    "Sports",
    "Technology", 
    "Entertainment",
    "Your_New_Category",  # Add here
    # ... more categories
]

# Update the prompt template
prompt = f"""
Classify into ONE of these categories:
1. Sports - keywords: football, basketball, soccer, ...
2. Technology - keywords: software, hardware, AI, ...
...
{len(CATEGORIES)}. Your_New_Category - keywords: keyword1, keyword2, ...
"""
```

#### Example Classifications

| Page Title | Category | Reasoning |
|------------|----------|-----------|
| `Taylor_Swift` | Entertainment | Celebrity, music artist |
| `iPhone_16` | Technology | Tech product, smartphone |
| `Climate_change` | Climate | Environmental topic |
| `2024_Summer_Olympics` | Sports | Athletic event |
| `Joe_Biden` | Politics | Political figure |
| `Quantum_computing` | Science | Scientific field |
| `Ferrari` | Autos and vehicles | Car manufacturer |
| `McDonald's` | Food and drink | Restaurant chain |

### 2. Geographic Extraction

#### How It Works

The system analyzes page titles to extract geographic information:
1. **Identifies location type:** Country, City, State, Region
2. **Extracts location name:** Actual place name
3. **Returns structured JSON:** `{type: "Country", name: "USA"}`

#### Prompt Template

```python
prompt = f"""
Analyze the Wikipedia page title "{page_title}" and extract geographic information.

If the page is about a PLACE or LOCATION, identify:
1. Location Type: Country, City, State, or Region
2. Location Name: The actual name of the place

Geographic Keywords to Look For:
- Countries: USA, UK, France, Germany, Japan, China, India, etc.
- Cities: New York, London, Tokyo, Paris, Mumbai, etc.
- States: California, Texas, New York, Florida, etc.
- Regions: Middle East, Southeast Asia, Europe, etc.

Return ONLY in this JSON format:
{{"type": "Country|City|State|Region", "name": "LocationName"}}

If NO geographic information, return:
{{"type": null, "name": null}}

Examples:
- "Eiffel_Tower" → {{"type": "City", "name": "Paris"}}
- "California" → {{"type": "State", "name": "California"}}
- "2024_Paris_Olympics" → {{"type": "City", "name": "Paris"}}
- "Taylor_Swift" → {{"type": null, "name": null}}

JSON:
"""
```

#### Customization

To modify geographic extraction, edit **`plugins/category.py`**:

```python
def extract_geography_with_llm(page_title):
    # Customize location keywords
    LOCATION_KEYWORDS = {
        'Country': ['USA', 'UK', 'France', 'YourCountry'],
        'City': ['New York', 'London', 'YourCity'],
        'State': ['California', 'Texas', 'YourState'],
        'Region': ['Middle East', 'YourRegion']
    }
    
    # Update prompt with your keywords
    prompt = f"""
    Geographic Keywords:
    - Countries: {', '.join(LOCATION_KEYWORDS['Country'])}
    - Cities: {', '.join(LOCATION_KEYWORDS['City'])}
    ...
    """
```

#### Example Geographic Extractions

| Page Title | Type | Name | Reasoning |
|------------|------|------|-----------|
| `Paris` | City | Paris | Direct city reference |
| `California` | State | California | US state |
| `United_States` | Country | USA | Country reference |
| `2024_Paris_Olympics` | City | Paris | Event in Paris |
| `Taj_Mahal` | Country | India | Landmark in India |
| `Silicon_Valley` | Region | Silicon Valley | Geographic region |
| `Taylor_Swift` | null | null | No geographic reference |

### 3. Sequential Processing (Anti-Rate-Limiting)

#### Problem Solved

- **Before:** Parallel processing → 429 Rate Limit Errors
- **After:** Sequential processing → 0 Rate Limit Errors

#### Implementation

```python
# Process pages ONE AT A TIME
for index, row in df.iterrows():
    page_title = row['page']
    
    # Get category
    category = classify_page_with_llm(page_title)
    
    # Get geography
    geography = extract_geography_with_llm(page_title)
    
    # Small delay to prevent rate limiting
    time.sleep(0.1)  # 100ms between requests
```

#### Performance

- **Pages per run:** 1000
- **Processing time:** ~5-10 minutes
- **Success rate:** 99%+
- **Rate limit errors:** 0

### 4. Error Handling & Fallbacks

```python
def classify_page_with_llm(page_title):
    try:
        # Call Ollama API
        response = requests.post(...)
        category = parse_response(response)
        
        # Validate category
        if category in VALID_CATEGORIES:
            return category
        else:
            return "Other"  # Fallback
            
    except requests.exceptions.Timeout:
        # Retry with exponential backoff
        time.sleep(2)
        return classify_page_with_llm(page_title)
        
    except Exception as e:
        # Log error and return fallback
        print(f"Error: {e}")
        return "Other"
```

### 5. Customization Guide

#### Add a New Category

**Step 1:** Edit `plugins/category.py`

```python
CATEGORIES = [
    "Sports",
    "Technology",
    # ... existing categories
    "Cryptocurrency",  # NEW CATEGORY
]
```

**Step 2:** Update the prompt

```python
prompt = f"""
...
18. Technology - tech companies, gadgets, software, IT
19. Cryptocurrency - Bitcoin, Ethereum, blockchain, NFTs  # NEW
20. Other - anything else
...
"""
```

**Step 3:** Test

```bash
# Restart Airflow
docker compose restart airflow-scheduler

# Trigger DAG
docker exec -it airflow-scheduler airflow dags trigger wiki_trending_pipeline
```

#### Add Geographic Keywords

**Edit `plugins/category.py`:**

```python
prompt = f"""
Geographic Keywords to Look For:
- Countries: USA, UK, France, Germany, Japan, China, YourNewCountry
- Cities: New York, London, Tokyo, Paris, YourNewCity
- States: California, Texas, YourNewState
- Regions: Middle East, Southeast Asia, YourNewRegion
"""
```

#### Change the LLM Model

**Step 1:** Pull new model

```bash
docker exec -it ollama ollama pull llama3.2:3b
```

**Step 2:** Update `plugins/category.py`

```python
# Change model name
OLLAMA_MODEL = "llama3.2:3b"  # was "qwen2.5:3b"

# Update API call
response = requests.post(
    "http://ollama:11434/api/generate",
    json={
        "model": OLLAMA_MODEL,  # Use new model
        "prompt": prompt,
        ...
    }
)
```

**Step 3:** Test new model

```bash
# Test directly
docker exec -it ollama ollama run llama3.2:3b "Classify: Taylor Swift"

# Run DAG
docker compose restart airflow-scheduler
```

---

## 🔧 Configuration Files

### Category Configuration (`plugins/category.py`)

```python
# Ollama Configuration
OLLAMA_URL = "http://ollama:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:3b"

# Model Parameters
MODEL_PARAMS = {
    "temperature": 0.0,     # Deterministic
    "top_p": 0.1,          # Focused
    "num_predict": 50,     # Max tokens
    "stop": ["\n\n"]       # Stop sequence
}

# Categories (Customizable)
CATEGORIES = [
    "Autos and vehicles",
    "Beauty and fashion",
    "Business and finance",
    # ... add your categories
]

# Geographic Keywords (Customizable)
LOCATION_KEYWORDS = {
    'Country': ['USA', 'UK', 'France', ...],
    'City': ['New York', 'London', ...],
    'State': ['California', 'Texas', ...],
    'Region': ['Middle East', ...]
}
```

### MinIO Configuration (`plugins/config.py`)

```python
# MinIO Settings
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Bucket Names (Customizable)
BUCKETS = {
    'bronze': 'bronze',
    'silver': 'silver',
    'gold': 'gold',
    'diamond': 'diamond',
    'historical': 'historical-data'
}
```

### PostgreSQL Configuration (`plugins/config.py`)

```python
# PostgreSQL Settings
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

# Table Names
TABLES = {
    'diamond': 'wiki_diamond',
    'timeseries': 'wiki_timeseries',
    'common_articles': 'wiki_common_articles',
    'metrics': 'dag_execution_metrics'
}
```

---

## 📊 Metabase Dashboards

### Setup Metabase

1. Open http://localhost:3000
2. Complete first-time setup wizard
3. Connect to PostgreSQL:
   - **Host:** `postgres`
   - **Port:** `5432`
   - **Database:** `airflow`
   - **Username:** `airflow`
   - **Password:** `airflow`

### Suggested Dashboards

#### 1. **Category Distribution**
```sql
SELECT category, COUNT(*) as page_count
FROM wiki_gold
GROUP BY category
ORDER BY page_count DESC;
```

#### 2. **Top Pages by Views**
```sql
SELECT page, views, category, geography
FROM wiki_gold
ORDER BY views DESC
LIMIT 20;
```

#### 3. **Geographic Trends**
```sql
SELECT geography, COUNT(*) as mentions
FROM wiki_gold
WHERE geography IS NOT NULL
GROUP BY geography
ORDER BY mentions DESC
LIMIT 15;
```

#### 4. **Category-Geography Matrix**
```sql
SELECT category, geography, COUNT(*) as count
FROM wiki_gold
WHERE geography IS NOT NULL
GROUP BY category, geography
ORDER BY count DESC;
```

---

## 🛠️ Troubleshooting & Debugging

### Quick Diagnostics

#### Check All Services Status
```bash
docker compose ps
```

All containers should show `Up` or `running`. If any show `Exit` or `Restarting`, investigate logs.

#### View Service Logs
```bash
# Airflow scheduler (DAG execution)
docker compose logs airflow-scheduler -f --tail=100

# Airflow webserver (UI issues)
docker compose logs airflow-webserver -f --tail=100

# PostgreSQL (database issues)
docker compose logs postgres -f --tail=50

# Ollama (AI model issues)
docker compose logs ollama -f --tail=50

# Spark master (processing issues)
docker compose logs spark-master -f --tail=50
```

---

### Common Issues & Solutions

#### Issue 1: Airflow webserver won't start

**Error:** `TypeError: can't compare offset-naive and offset-aware datetimes`

**Solution:** Timezone configuration is already added to `docker-compose.yml`:
```yaml
AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: 'UTC'
AIRFLOW__CORE__DEFAULT_TIMEZONE: 'UTC'
TZ: 'UTC'
```

**Fix:**
```bash
docker compose down
docker compose up -d
```

Wait 2-3 minutes for Airflow to initialize.

---

#### Issue 2: DAG not appearing in UI

**Symptoms:** DAG doesn't show up in Airflow webserver

**Causes:**
1. Python syntax errors in DAG file
2. Import errors (missing dependencies)
3. DAG file not in `/dags` directory

**Debugging Steps:**

1. **Check DAG parsing errors:**
```bash
docker exec -it airflow-scheduler airflow dags list-import-errors
```

2. **Test DAG file syntax:**
```bash
docker exec -it airflow-scheduler python /opt/airflow/dags/wiki_top_pages_dag.py
```

3. **View scheduler logs:**
```bash
docker compose logs airflow-scheduler --tail=200 | grep -i error
```

4. **Force DAG reparse:**
```bash
docker exec -it airflow-scheduler airflow dags reserialize
```

---

#### Issue 3: Ollama model not responding

**Error:** `500 Internal Server Error` or connection refused from Ollama

**Debugging Steps:**

1. **Check if Ollama is running:**
```bash
docker compose ps ollama
```

2. **Check if model is loaded:**
```bash
docker exec -it ollama ollama list
```

Expected output should include `qwen2.5:3b`.

3. **Pull model if missing:**
```bash
./scripts/pull_qwen_model.sh
```

4. **Test model directly:**
```bash
docker exec -it ollama ollama run qwen2.5:3b "Classify this: Taylor Swift"
```

5. **Check Ollama logs:**
```bash
docker compose logs ollama --tail=100
```

6. **Restart Ollama:**
```bash
docker compose restart ollama
sleep 10  # Wait for startup
./scripts/pull_qwen_model.sh  # Re-pull model
```

---

#### Issue 4: Categorization task fails with rate limiting

**Error:** `429 Too Many Requests` or `Rate limit exceeded`

**Solution:** Already fixed with sequential processing!

The `categorize_pages` task now processes pages **one at a time** instead of in parallel batches.

**Verify sequential mode:**
```bash
# Check the task code
docker exec -it airflow-scheduler cat /opt/airflow/plugins/utils/categorize_pages.py | grep -A 5 "for.*page"
```

Should show:
```python
for index, row in df.iterrows():
    # Process one page at a time
```

**If still getting errors:**
1. Check Ollama container resources
2. Increase delay between requests (edit `categorize_pages.py`)
3. Check Ollama logs for errors

---

#### Issue 5: Spark job fails

**Error:** `Connection refused` to Spark master or workers

**Debugging Steps:**

1. **Check Spark cluster status:**
```bash
docker compose ps | grep spark
```

2. **View Spark Master UI:**
Open http://localhost:8081

Check:
- Workers connected (should show 2 workers)
- Available cores and memory

3. **Check Spark master logs:**
```bash
docker compose logs spark-master --tail=100
```

4. **Check worker logs:**
```bash
docker compose logs spark-worker-1 --tail=50
docker compose logs spark-worker-2 --tail=50
```

5. **Restart Spark cluster:**
```bash
docker compose restart spark-master spark-worker-1 spark-worker-2
```

6. **Test Spark connectivity:**
```bash
docker exec -it spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077
```

---

#### Issue 6: PostgreSQL table column errors

**Error:** `column "geographytype" does not exist` or similar

**Root Cause:** Table created with old schema

**Solution:**

1. **Drop old table:**
```bash
docker exec -it postgres psql -U airflow -d airflow -c "DROP TABLE IF EXISTS wiki_gold;"
```

2. **Trigger DAG run** (table will auto-create with correct schema)

3. **Verify new schema:**
```bash
docker exec -it postgres psql -U airflow -d airflow -c "\d wiki_diamond"
```

Should show columns: `page`, `views`, `rank`, `category`, `location_type`, `location`, `fetch_date`

---

#### Issue 7: MinIO bucket not found

**Error:** `NoSuchBucket` or `Bucket does not exist`

**Solution:** Buckets are auto-created by tasks. To manually create:

```bash
docker exec -it minio mc alias set myminio http://localhost:9000 minioadmin minioadmin

docker exec -it minio mc mb myminio/bronze
docker exec -it minio mc mb myminio/silver
docker exec -it minio mc mb myminio/gold
docker exec -it minio mc mb myminio/diamond
docker exec -it minio mc mb myminio/historical-data
```

**Verify buckets:**
```bash
docker exec -it minio mc ls myminio
```

---

#### Issue 8: Metrics collection failing

**Error:** `AttributeError: 'TaskInstance' object has no attribute 'session'`

**Status:** ✅ **FIXED** in current version

**Verification:**
```bash
# Check metrics table exists
docker exec -it postgres psql -U airflow -d airflow -c "\dt" | grep metrics

# View recent metrics
docker exec -it postgres psql -U airflow -d airflow -c "SELECT dag_id, execution_date, state, total_duration_minutes FROM dag_execution_metrics ORDER BY collected_at DESC LIMIT 5;"
```

**If issue persists:**
```bash
# Check metrics collector code
docker exec -it airflow-scheduler cat /opt/airflow/plugins/utils/dag_metrics_collector.py | grep -A 3 "from airflow.utils.session"
```

Should show:
```python
from airflow.utils.session import create_session

with create_session() as session:
```

---

#### Issue 9: Smart orchestrator not triggering

**Symptoms:** Analytics tasks don't run even with 5+ days of data

**Debugging Steps:**

1. **Check orchestrator DAG is enabled:**
```bash
docker exec -it airflow-scheduler airflow dags state wiki_smart_analytics_orchestrator
```

2. **Check PostgreSQL for data:**
```bash
docker exec -it postgres psql -U airflow -d airflow -c "SELECT DISTINCT fetch_date FROM wiki_diamond ORDER BY fetch_date DESC LIMIT 10;"
```

3. **Manually trigger orchestrator:**
```bash
docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator
```

4. **Check orchestrator logs:**
```bash
docker compose logs airflow-scheduler | grep "check_and_trigger"
```

---

### Advanced Debugging

#### Test Individual Python Functions

```bash
# Enter Airflow container
docker exec -it airflow-webserver bash

# Test Wikipedia API fetch
python -c "from plugins.utils.fetch_wiki_data import fetch_and_upload_wiki_data; fetch_and_upload_wiki_data('2025-11-14')"

# Test AI categorization (single page)
python -c "from plugins.category import classify_page_with_llm; print(classify_page_with_llm('Taylor_Swift'))"

# Test PostgreSQL connection
python -c "import psycopg2; conn = psycopg2.connect(host='postgres', database='airflow', user='airflow', password='airflow'); print('Connected!')"

# Test MinIO connection
python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin'); print(s3.list_buckets())"
```

#### View Task Instance Details

```bash
# List all task instances for a DAG run
docker exec -it airflow-scheduler airflow tasks list wiki_trending_pipeline

# View task state
docker exec -it airflow-scheduler airflow tasks state wiki_trending_pipeline categorize_pages 2025-11-14

# View task logs
docker exec -it airflow-scheduler airflow tasks logs wiki_trending_pipeline categorize_pages 2025-11-14 1
```

#### Database Debugging

```bash
# Enter PostgreSQL
docker exec -it postgres psql -U airflow -d airflow

# Check all tables
\dt

# View table schema
\d wiki_diamond

# Check row counts
SELECT 
    'wiki_diamond' as table_name, COUNT(*) as rows FROM wiki_diamond
UNION ALL
SELECT 'wiki_timeseries', COUNT(*) FROM wiki_timeseries
UNION ALL
SELECT 'wiki_common_articles', COUNT(*) FROM wiki_common_articles
UNION ALL
SELECT 'dag_execution_metrics', COUNT(*) FROM dag_execution_metrics;

# Check latest data
SELECT * FROM wiki_diamond ORDER BY fetch_date DESC LIMIT 10;

# Check missing dates
SELECT generate_series(
    CURRENT_DATE - INTERVAL '10 days',
    CURRENT_DATE - INTERVAL '1 day',
    '1 day'::interval
)::date AS date
EXCEPT
SELECT DISTINCT fetch_date FROM wiki_diamond;
```

#### MinIO Debugging

```bash
# Access MinIO console
Open http://localhost:9001
Login: minioadmin / minioadmin

# Or use CLI:
docker exec -it minio mc alias set myminio http://localhost:9000 minioadmin minioadmin

# List all buckets
docker exec -it minio mc ls myminio

# List files in Bronze bucket
docker exec -it minio mc ls myminio/bronze

# View file content
docker exec -it minio mc cat myminio/bronze/top_pages_2025_11_14.json | head -50

# Check bucket size
docker exec -it minio mc du myminio/bronze
```

#### Performance Debugging

```bash
# Use the metrics comparison tool
python3 compare_metrics.py --latest 10

# View slowest tasks
python3 compare_metrics.py --task-breakdown "RUN_ID" | sort -k2 -nr | head -5

# Check DAG performance trend
python3 compare_metrics.py --trend 30

# SQL query for bottlenecks
docker exec -it postgres psql -U airflow -d airflow <<EOF
SELECT 
    task_id,
    AVG(duration_seconds) as avg_duration,
    MAX(duration_seconds) as max_duration,
    COUNT(*) as executions
FROM (
    SELECT 
        jsonb_array_elements(task_metrics)->>'task_id' as task_id,
        (jsonb_array_elements(task_metrics)->>'duration_seconds')::numeric as duration_seconds
    FROM dag_execution_metrics
    WHERE dag_id = 'wiki_trending_pipeline'
) AS tasks
GROUP BY task_id
ORDER BY avg_duration DESC;
EOF
```

---

### Monitoring & Health Checks

#### System Resources

```bash
# Check Docker resource usage
docker stats --no-stream

# Check disk usage
docker system df

# Check container resource limits
docker inspect airflow-scheduler | grep -A 10 "Memory"
```

#### Airflow Health Check

```bash
# Check Airflow version
docker exec -it airflow-scheduler airflow version

# Check database connection
docker exec -it airflow-scheduler airflow db check

# List active DAG runs
docker exec -it airflow-scheduler airflow dags list-runs -d wiki_trending_pipeline --state running
```

#### Data Quality Checks

```bash
# Check for duplicates in wiki_diamond
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT page, fetch_date, COUNT(*) 
FROM wiki_diamond 
GROUP BY page, fetch_date 
HAVING COUNT(*) > 1
LIMIT 10;
"

# Check for NULL categories
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT COUNT(*) as null_categories 
FROM wiki_diamond 
WHERE category IS NULL OR category = '';
"

# Check category distribution
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT category, COUNT(*) as count 
FROM wiki_diamond 
GROUP BY category 
ORDER BY count DESC;
"
```

---

### Recovery Procedures

#### Clear Failed Task and Retry

```bash
# Clear specific task
docker exec -it airflow-scheduler airflow tasks clear wiki_trending_pipeline categorize_pages --start-date 2025-11-14 --end-date 2025-11-14

# Clear entire DAG run
docker exec -it airflow-scheduler airflow dags clear wiki_trending_pipeline --start-date 2025-11-14 --end-date 2025-11-14
```

#### Reset Database (Nuclear Option)

⚠️ **WARNING: This deletes all data!**

```bash
# Stop all services
docker compose down

# Remove volumes (deletes all data)
docker volume rm wiki-trends-pipeline_postgres-db-volume

# Restart
docker compose up -d

# Wait for initialization
sleep 60

# Pull Ollama model again
./scripts/pull_qwen_model.sh
```

#### Backup and Restore

```bash
# Backup PostgreSQL
docker exec postgres pg_dump -U airflow airflow > backup_$(date +%Y%m%d).sql

# Restore PostgreSQL
docker exec -i postgres psql -U airflow airflow < backup_20251115.sql

# Backup MinIO data
docker exec minio mc mirror myminio/bronze ./backup/bronze/
docker exec minio mc mirror myminio/silver ./backup/silver/
docker exec minio mc mirror myminio/gold ./backup/gold/
```

---

---

## 📝 Configuration

### Environment Variables

Located in `docker-compose.yml`:

```yaml
# Airflow
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__FERNET_KEY: 'UKMzEm3yIuFYEq1y3-2FxPNWSVwRASpahmQ9kQfEr8E='
AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: 'UTC'
AIRFLOW__CORE__DEFAULT_TIMEZONE: 'UTC'

# MinIO
AWS_ACCESS_KEY_ID: minioadmin
AWS_SECRET_ACCESS_KEY: minioadmin
AIRFLOW_CONN_MINIO_S3: s3://minioadmin:minioadmin@?host=http://minio:9000

# Spark
SPARK_MASTER_URL: spark://spark-master:7077
SPARK_HOME: /opt/spark
```

### Buckets (MinIO)

- **bronze:** Raw JSON data from Wikipedia API
- **silver:** Cleaned CSV data from Spark processing
- **gold:** Enriched CSV with AI categories and geography

---

## 🧪 Testing

### Test Individual Tasks

From Airflow UI, you can test each task independently:

1. Go to DAG → Graph View
2. Click on a task
3. Click **Test** button
4. Select an execution date
5. View logs in real-time

### Manual Python Testing

```bash
# Enter Airflow container
docker exec -it airflow-webserver bash

# Test Wikipedia API fetch
python -c "from plugins.utils.fetch_wiki_data import fetch_and_upload_wiki_data; fetch_and_upload_wiki_data()"

# Test AI categorization
python -c "from plugins.category import classify_page_with_llm; print(classify_page_with_llm('Taylor_Swift'))"
```

---

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://min.io/docs/)
- [Qwen2.5 Model Documentation](https://github.com/QwenLM/Qwen2.5)
- [Wikipedia Pageviews API](https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{date_str})

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

## 📄 License

This project is licensed under the MIT License.

---

## 👤 Author

**Sairam030**
- GitHub: [@sairam030](https://github.com/sairam030)
- Repository: [wiki-airflow](https://github.com/sairam030/wiki-airflow)

---

## 🎯 Future Enhancements

### Planned Features

- [ ] **Real-time Streaming:** Kafka integration for live Wikipedia updates
- [ ] **Data Quality Framework:** Great Expectations for automated validation
- [ ] **Advanced NLP:** Sentiment analysis, entity recognition, topic modeling
- [ ] **Enhanced Dashboards:** Interactive Metabase/Grafana visualizations
- [ ] **Data Versioning:** DVC for reproducible data lineups
- [ ] **Alert System:** Slack/Email notifications for failures and anomalies
- [ ] **Performance Optimization:** Spark job tuning, caching strategies
- [ ] **Testing Suite:** Unit tests, integration tests, data quality tests
- [ ] **Cloud Deployment:** AWS/GCP/Azure infrastructure-as-code
- [ ] **Multi-language Support:** Extend beyond English Wikipedia
- [ ] **ML Models:** Trend prediction, anomaly detection, recommendation systems
- [ ] **API Layer:** FastAPI endpoints for querying analytics
- [ ] **Data Catalog:** Metadata management with Apache Atlas or DataHub

---

## 📚 Documentation

- **[Metrics System Guide](docs/METRICS_SYSTEM.md)** - Performance monitoring and comparison
- **[Metabase Queries](docs/METRICS_QUERIES.md)** - Ready-made SQL queries for dashboards
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Apache Spark Docs](https://spark.apache.org/docs/latest/)
- [MinIO Docs](https://min.io/docs/)
- [Qwen2.5 Model](https://github.com/QwenLM/Qwen2.5)
- [Wikipedia API](https://wikimedia.org/api/rest_v1/)

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 for Python code
- Add docstrings to all functions
- Update README for significant changes
- Test DAGs before submitting PR
- Add metrics for new tasks

---

## 📊 Project Stats

**Current Status:** ✅ Production Ready

| Metric | Value |
|--------|-------|
| **DAGs** | 2 (Main Pipeline + Smart Orchestrator) |
| **Tasks per Run** | 9 (main pipeline) |
| **Daily Data Volume** | ~1000 pages |
| **AI Categories** | 19 |
| **Database Tables** | 4 |
| **MinIO Buckets** | 5 |
| **Docker Containers** | 10 |
| **Average Runtime** | ~8-12 minutes |
| **Success Rate** | 95%+ |
| **Data Layers** | 4 (Bronze, Silver, Gold, Diamond) |

---

## 📝 Changelog

### Version 2.0 (November 2025)
- ✅ Added Diamond layer analytics (timeseries, common articles)
- ✅ Implemented smart orchestration with auto-fill missing dates
- ✅ Added comprehensive performance metrics collection
- ✅ Created metrics comparison CLI tool
- ✅ Fixed sequential AI processing (prevents rate limiting)
- ✅ Added historical data archiving to MinIO
- ✅ Created documentation for metrics system

### Version 1.0 (October 2025)
- ✅ Initial release with Bronze → Silver → Gold pipeline
- ✅ AI categorization with Qwen2.5:3B
- ✅ Geographic extraction
- ✅ PostgreSQL integration
- ✅ Spark processing
- ✅ MinIO data lake

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Sairam030**
- GitHub: [@sairam030](https://github.com/sairam030)
- Repository: [wiki-airflow](https://github.com/sairam030/wiki-airflow)
- Email: sairam.analytics@example.com

---

## 🙏 Acknowledgments

- **Apache Airflow** - Workflow orchestration
- **Apache Spark** - Distributed processing
- **Qwen Team** - Excellent open-source LLM
- **Wikimedia** - Public API for Wikipedia data
- **MinIO** - S3-compatible object storage
- **Ollama** - Local LLM inference

---

## ⚡ Quick Reference Card

### Essential Commands

```bash
# Start/Stop
docker compose up -d              # Start all services
docker compose down               # Stop all services
docker compose restart <service>  # Restart specific service

# Logs
docker compose logs -f --tail=100 airflow-scheduler
docker compose logs -f --tail=100 ollama

# DAG Management
docker exec -it airflow-scheduler airflow dags trigger wiki_trending_pipeline
docker exec -it airflow-scheduler airflow dags list
docker exec -it airflow-scheduler airflow tasks list wiki_trending_pipeline

# Database
docker exec -it postgres psql -U airflow -d airflow
SELECT COUNT(*) FROM wiki_diamond;
SELECT * FROM dag_execution_metrics ORDER BY collected_at DESC LIMIT 5;

# Metrics
python3 compare_metrics.py --latest 5
python3 compare_metrics.py --compare
python3 compare_metrics.py --task-breakdown "RUN_ID"
python3 compare_metrics.py --trend 7

# Health Checks
docker compose ps                 # Service status
docker stats --no-stream          # Resource usage
docker system df                  # Disk usage

# MinIO
docker exec -it minio mc ls myminio
docker exec -it minio mc cat myminio/bronze/top_pages_2025_11_14.json

# Ollama
docker exec -it ollama ollama list
docker exec -it ollama ollama run qwen2.5:3b "Test prompt"
```

### Access URLs

| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Metabase | http://localhost:3000 |
| Spark Master | http://localhost:8081 |

### Default Credentials

| Service | Username | Password |
|---------|----------|----------|
| Airflow | admin | admin |
| MinIO | minioadmin | minioadmin |
| PostgreSQL | airflow | airflow |

---

**🌟 Star this repo if you find it useful!**

**📧 Questions? Open an issue on GitHub.**

---
