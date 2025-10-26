# 📊 Wikipedia Trends Analytics Pipeline

A comprehensive data engineering pipeline that extracts, processes, and analyzes trending Wikipedia pages using Apache Airflow, Apache Spark, MinIO (S3), and AI-powered categorization.

---

## 🎯 Project Overview

This project implements a **fully automated ETL pipeline** that:

1. **Extracts** trending Wikipedia page data from the Wikimedia API
2. **Stores** raw data in a MinIO-based data lake (Bronze layer)
3. **Transforms** and cleans data using Apache Spark (Silver layer)
4. **Enriches** pages with AI-powered categorization and geographic extraction (Gold layer)
5. **Loads** final data into PostgreSQL for analytics
6. **Visualizes** insights through Metabase dashboards

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
    [Load to PostgreSQL]
        ↓
   PostgreSQL Database (wiki_gold table)
        ↓
    [Visualize]
        ↓
   Metabase Dashboards
```

---

## 🏗️ Architecture

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          ORCHESTRATION LAYER                             │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    Apache Airflow (LocalExecutor)                 │   │
│  │  - Scheduler: Manages DAG execution                              │   │
│  │  - Webserver: UI for monitoring (Port 8080)                      │   │
│  │  - PostgreSQL: Metadata store                                    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                            INGESTION LAYER                               │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              Wikipedia Pageviews API (Wikimedia)                  │   │
│  │  - Fetch top 1000 pages by views for a given date               │   │
│  │  - Returns: Page title, view count, rank                         │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA LAKE (MinIO S3)                             │
│  ┌───────────────┐  ┌───────────────┐  ┌────────────────┐              │
│  │ Bronze Bucket │  │ Silver Bucket │  │  Gold Bucket   │              │
│  │               │  │               │  │                │              │
│  │ Raw JSON      │→ │ Cleaned CSV   │→ │ Enriched CSV   │              │
│  │ (As-is data)  │  │ (Deduped)     │  │ (Categorized)  │              │
│  └───────────────┘  └───────────────┘  └────────────────┘              │
│                          MinIO (Port 9000, Console 9001)                │
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
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                           AI ENRICHMENT LAYER                            │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                  Ollama (Qwen2.5:3B Model)                        │   │
│  │                    Port 11434                                     │   │
│  │                                                                   │   │
│  │  Capabilities:                                                    │   │
│  │    1. Category Classification (19 categories)                    │   │
│  │       - Sports, Technology, Politics, Entertainment, etc.        │   │
│  │                                                                   │   │
│  │    2. Geographic Extraction                                       │   │
│  │       - Country, City, State, Region                             │   │
│  │       - Location type and name                                    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                          ANALYTICS LAYER                                 │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              PostgreSQL Database (Port 5432)                      │   │
│  │                                                                   │   │
│  │  Table: wiki_gold                                                 │   │
│  │  Columns: page, views, rank, category,                           │   │
│  │           geography_type, geography                               │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                Metabase BI Tool (Port 3000)                       │   │
│  │                                                                   │   │
│  │  Dashboards:                                                      │   │
│  │    • Category distribution                                        │   │
│  │    • Geographic trends                                            │   │
│  │    • Top pages by views                                           │   │
│  │    • Time-series analysis                                         │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
wiki-trends-pipeline/
│
├── dags/                                    # Airflow DAG definitions
│   ├── __init__.py
│   └── wiki_top_pages_dag.py               # Main pipeline DAG
│
├── plugins/                                 # Custom Airflow plugins
│   ├── __init__.py
│   ├── config.py                           # Configuration (MinIO, buckets)
│   ├── category.py                         # AI categorization logic (Qwen2.5)
│   │
│   ├── operators/                          # Custom Airflow operators
│   │   ├── __init__.py
│   │   └── spark_submit_operator.py       # Spark job submission
│   │
│   └── utils/                              # Utility functions
│       ├── __init__.py
│       ├── fetch_wiki_data.py             # Wikipedia API extraction
│       ├── categorize_pages.py            # AI-powered categorization
│       └── load_postgres.py               # PostgreSQL loading
│
├── scripts/                                # Helper scripts
│   └── pull_qwen_model.sh                 # Download Qwen2.5 model
│
├── logs/                                   # Airflow logs
│   ├── dag_id=wiki_trending_pipeline/
│   ├── dag_processor_manager/
│   └── scheduler/
│
├── docker/                                 # Docker configurations
│
├── Dockerfile                              # Custom Airflow+Spark image
├── docker-compose.yml                      # Multi-container orchestration
├── requirements.txt                        # Python dependencies
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

### Main DAG: `wiki_trending_pipeline`

**Schedule:** Daily at midnight UTC (`0 0 * * *`)

**Execution Date Logic:** Runs for **yesterday's data** (D-1)

#### Task Flow

```
[START]
   ↓
[fetch_trending_pages]
   ↓
[process_bronze_to_silver]  (Spark Job)
   ↓
[categorize_pages]  (AI with Qwen2.5)
   ↓
[load_gold_to_postgres]
   ↓
[END]
```

### Task Details

#### 1. **fetch_trending_pages**
- **Type:** PythonOperator
- **Function:** `fetch_and_upload_wiki_data()`
- **Input:** Execution date (D-1)
- **Process:**
  - Calls Wikipedia Pageviews API
  - Fetches top 1000 pages by views
  - Saves raw JSON to MinIO Bronze bucket
- **Output:** `bronze/top_pages_YYYY_MM_DD.json`
- **Duration:** ~10-20 seconds

#### 2. **process_bronze_to_silver**
- **Type:** Custom SparkSubmitOperator
- **Spark Job:** PySpark transformation
- **Process:**
  - Reads JSON from Bronze layer
  - Deduplicates pages
  - Standardizes format
  - Validates data quality
- **Output:** `silver/top_pages_YYYY_MM_DD.csv`
- **Duration:** ~30-60 seconds
- **Resources:** 2 executors, 2GB RAM each

#### 3. **categorize_pages**
- **Type:** PythonOperator
- **Function:** `categorize_and_upload()`
- **AI Model:** Qwen2.5:3B via Ollama API
- **Process:**
  - Reads cleaned CSV from Silver layer
  - For each page:
    - **Category Classification:** Assigns 1 of 19 categories
    - **Geographic Extraction:** Extracts location (Country/City/State/Region)
  - Batch processing: 10 pages at a time
- **Categories:**
  - Autos and vehicles
  - Beauty and fashion
  - Business and finance
  - Climate
  - Entertainment
  - Food and drink
  - Games
  - Health
  - Hobbies and leisure
  - Jobs and education
  - Law and government
  - Pets and animals
  - Politics
  - Science
  - Shopping
  - Sports
  - Technology
  - Travel and transportation
  - Other
- **Output:** `gold/categorized_YYYY_MM_DD.csv`
- **Duration:** ~5-10 minutes (depends on model and batch size)

#### 4. **load_gold_to_postgres**
- **Type:** PythonOperator
- **Function:** `load_gold_to_postgres()`
- **Process:**
  - Reads categorized CSV from Gold layer
  - Inserts into PostgreSQL `wiki_gold` table
  - Handles duplicates with UPSERT logic
- **Output:** Rows in `wiki_gold` table
- **Duration:** ~5-10 seconds

### DAG Diagram

![DAG Visualization](docs/dag_diagram.png)
*Add your DAG screenshot here from Airflow UI*

---

## 🚀 Getting Started

### Prerequisites

- Docker (24.x or higher)
- Docker Compose (2.x or higher)
- 8GB RAM minimum (16GB recommended)
- 20GB free disk space

### Installation Steps

#### 1. Clone the Repository

```bash
git clone https://github.com/sairam030/wiki-airflow.git
cd wiki-trends-pipeline
```

#### 2. Set File Permissions

```bash
sudo chown -R $USER .
chmod +x scripts/pull_qwen_model.sh
```

#### 3. Start All Services

```bash
docker compose up -d
```

This will start all containers:
- ✅ PostgreSQL (metadata database)
- ✅ MinIO (data lake)
- ✅ Spark Master + 2 Workers
- ✅ Ollama (AI inference)
- ✅ Airflow Webserver + Scheduler
- ✅ Metabase

**Wait 2-3 minutes** for all services to initialize.

#### 4. Verify Services

```bash
docker compose ps
```

All containers should show status: `Up` or `running`

#### 5. Pull the AI Model

```bash
./scripts/pull_qwen_model.sh
```

This downloads the Qwen2.5:3B model (~2GB) into the Ollama container.

**Expected output:**
```
🔄 Pulling Qwen2.5:3b model...
pulling manifest
pulling <layers>...
✅ Qwen2.5:3b model pulled successfully!
```

#### 6. Access Airflow UI

Open browser: http://localhost:8080

**Login:**
- Username: `admin`
- Password: `admin`

#### 7. Enable the DAG

In Airflow UI:
1. Find `wiki_trending_pipeline` DAG
2. Toggle the switch to **ON**
3. Click **Trigger DAG** (play button) to run manually

#### 8. Monitor Execution

Watch the DAG run in real-time:
- **Graph View:** Visual task dependencies
- **Grid View:** Historical runs
- **Logs:** Click on any task → View Log

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

### Categorization Prompts

The model uses two specialized prompts:

#### Category Classification Prompt
- Input: Wikipedia page title
- Output: One of 19 predefined categories
- Logic: Based on content type, profession, industry

#### Geographic Extraction Prompt
- Input: Wikipedia page title
- Output: JSON with location details
- Logic: Identifies country, city, state, or region

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

## 🛠️ Troubleshooting

### Issue: Airflow webserver won't start

**Error:** `TypeError: can't compare offset-naive and offset-aware datetimes`

**Solution:** Timezone configuration is already added to `docker-compose.yml`:
```yaml
AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE: 'UTC'
AIRFLOW__CORE__DEFAULT_TIMEZONE: 'UTC'
TZ: 'UTC'
```

Restart services:
```bash
docker compose down
docker compose up -d
```

### Issue: Ollama model not responding

**Error:** `500 Internal Server Error` from Ollama

**Solution:**
1. Check if model is pulled:
   ```bash
   docker exec -it ollama ollama list
   ```
2. If missing, pull again:
   ```bash
   ./scripts/pull_qwen_model.sh
   ```

### Issue: Spark job fails

**Error:** `Connection refused` to Spark master

**Solution:**
1. Check Spark master is running:
   ```bash
   docker compose ps spark-master
   ```
2. View Spark UI: http://localhost:8081
3. Restart Spark cluster:
   ```bash
   docker compose restart spark-master spark-worker-1 spark-worker-2
   ```

### Issue: PostgreSQL column not found

**Error:** `column "geographytype" does not exist`

**Solution:** (Already fixed) Drop and recreate table:
```bash
docker exec -it postgres psql -U airflow -d airflow -c "DROP TABLE IF EXISTS wiki_gold;"
```

Table will auto-recreate on next DAG run with correct schema.

### Issue: MinIO bucket not found

**Solution:** Buckets are auto-created by tasks. If missing, create manually:

```bash
docker exec -it minio mc mb minio/bronze
docker exec -it minio mc mb minio/silver
docker exec -it minio mc mb minio/gold
```

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

- [ ] Add real-time streaming with Kafka
- [ ] Implement data quality checks with Great Expectations
- [ ] Add more sophisticated NLP analysis
- [ ] Create advanced Metabase dashboards
- [ ] Implement data versioning with DVC
- [ ] Add alert notifications (Slack, Email)
- [ ] Optimize Spark jobs for larger datasets
- [ ] Add unit and integration tests
- [ ] Deploy to cloud (AWS/GCP/Azure)

---
