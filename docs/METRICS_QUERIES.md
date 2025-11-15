# DAG Execution Metrics Queries

## Overview
This document contains SQL queries for analyzing DAG execution metrics in Metabase.
All queries use the `dag_execution_metrics` table which stores comprehensive performance data.

---

## 1. Daily Performance Overview

**Use Case:** Track daily DAG performance - execution times, success rates, throughput

```sql
SELECT 
    execution_date::date as run_date,
    dag_id,
    COUNT(*) as total_runs,
    ROUND(AVG(total_duration_minutes), 2) as avg_duration_min,
    ROUND(MAX(total_duration_minutes), 2) as max_duration_min,
    ROUND(MIN(total_duration_minutes), 2) as min_duration_min,
    ROUND(AVG(success_rate), 2) as avg_success_rate,
    SUM(total_retries) as total_retries,
    ROUND(AVG(throughput_records_per_second), 2) as avg_throughput_rps
FROM dag_execution_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY execution_date::date, dag_id
ORDER BY run_date DESC, dag_id;
```

**Visualization:** Line chart or table
- X-axis: run_date
- Y-axis: avg_duration_min, avg_success_rate, avg_throughput_rps
- Group by: dag_id

---

## 2. Latency Trend Analysis

**Use Case:** Monitor pipeline latency over time, identify slowdowns

```sql
SELECT 
    execution_date::date as run_date,
    dag_id,
    ROUND(AVG(total_duration_minutes), 2) as avg_latency_min,
    ROUND(STDDEV(total_duration_minutes), 2) as latency_stddev,
    ROUND(MIN(total_duration_minutes), 2) as min_latency,
    ROUND(MAX(total_duration_minutes), 2) as max_latency,
    COUNT(*) as runs
FROM dag_execution_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
AND state = 'success'
GROUP BY execution_date::date, dag_id
ORDER BY run_date DESC;
```

**Visualization:** Line chart with confidence bands
- X-axis: run_date
- Y-axis: avg_latency_min (main line), min_latency/max_latency (bands)
- Filter by: dag_id

---

## 3. Throughput Analysis

**Use Case:** Track records processed per second, identify bottlenecks

```sql
SELECT 
    execution_date::date as run_date,
    dag_id,
    ROUND(AVG(throughput_records_per_second), 2) as avg_throughput_rps,
    ROUND(AVG(throughput_records_per_minute), 2) as avg_throughput_rpm,
    ROUND(MAX(throughput_records_per_second), 2) as peak_throughput_rps,
    SUM((data_volumes->>'total_records')::int) as total_records_processed
FROM dag_execution_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
AND throughput_records_per_second IS NOT NULL
GROUP BY execution_date::date, dag_id
ORDER BY run_date DESC;
```

**Visualization:** Area chart or combo chart
- X-axis: run_date
- Y-axis (left): avg_throughput_rps
- Y-axis (right): total_records_processed
- Group by: dag_id

---

## 4. Success Rate & Reliability

**Use Case:** Monitor DAG reliability, task failures, retry patterns

```sql
SELECT 
    execution_date::date as run_date,
    dag_id,
    COUNT(*) as total_runs,
    SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(
        SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END)::numeric / 
        COUNT(*)::numeric * 100, 
        2
    ) as success_rate_pct,
    ROUND(AVG(success_rate), 2) as avg_task_success_rate,
    SUM(total_retries) as total_task_retries
FROM dag_execution_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY execution_date::date, dag_id
ORDER BY run_date DESC;
```

**Visualization:** Stacked bar chart
- X-axis: run_date
- Y-axis: successful_runs, failed_runs
- Color by: state
- Add metric: success_rate_pct as trend line

---

## 5. Task-Level Performance Breakdown

**Use Case:** Deep dive into individual task performance

```sql
SELECT 
    dag_id,
    execution_date::date as run_date,
    jsonb_array_elements(task_metrics)->>'task_id' as task_id,
    COUNT(*) as executions,
    ROUND(AVG((jsonb_array_elements(task_metrics)->>'duration_seconds')::numeric), 2) as avg_duration_sec,
    ROUND(MAX((jsonb_array_elements(task_metrics)->>'duration_seconds')::numeric), 2) as max_duration_sec,
    ROUND(MIN((jsonb_array_elements(task_metrics)->>'duration_seconds')::numeric), 2) as min_duration_sec,
    SUM((jsonb_array_elements(task_metrics)->>'try_number')::int - 1) as total_retries
FROM dag_execution_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
AND task_metrics IS NOT NULL
GROUP BY dag_id, execution_date::date, jsonb_array_elements(task_metrics)->>'task_id'
ORDER BY dag_id, run_date DESC, avg_duration_sec DESC;
```

**Visualization:** Horizontal bar chart
- X-axis: avg_duration_sec
- Y-axis: task_id
- Color by: dag_id
- Tooltip: executions, total_retries

---

## 6. Slowest Tasks Identification

**Use Case:** Find tasks that slow down the pipeline

```sql
WITH task_performance AS (
    SELECT 
        dag_id,
        jsonb_array_elements(task_metrics)->>'task_id' as task_id,
        (jsonb_array_elements(task_metrics)->>'duration_seconds')::numeric as duration_sec,
        execution_date::date as run_date
    FROM dag_execution_metrics
    WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
    AND task_metrics IS NOT NULL
)
SELECT 
    task_id,
    dag_id,
    COUNT(*) as executions,
    ROUND(AVG(duration_sec), 2) as avg_duration_sec,
    ROUND(MAX(duration_sec), 2) as max_duration_sec,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_sec), 2) as p95_duration_sec,
    ROUND(STDDEV(duration_sec), 2) as duration_stddev
FROM task_performance
WHERE duration_sec IS NOT NULL
GROUP BY task_id, dag_id
ORDER BY avg_duration_sec DESC
LIMIT 20;
```

**Visualization:** Table with heatmap
- Sort by: avg_duration_sec (descending)
- Highlight: p95_duration_sec > threshold

---

## 7. Day-over-Day Comparison

**Use Case:** Compare today's performance vs previous days

```sql
WITH daily_metrics AS (
    SELECT 
        execution_date::date as run_date,
        dag_id,
        ROUND(AVG(total_duration_minutes), 2) as avg_duration,
        ROUND(AVG(success_rate), 2) as avg_success_rate,
        ROUND(AVG(throughput_records_per_second), 2) as avg_throughput,
        SUM(total_retries) as retries
    FROM dag_execution_metrics
    WHERE execution_date >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY execution_date::date, dag_id
)
SELECT 
    run_date,
    dag_id,
    avg_duration,
    LAG(avg_duration) OVER (PARTITION BY dag_id ORDER BY run_date) as prev_day_duration,
    ROUND(
        (avg_duration - LAG(avg_duration) OVER (PARTITION BY dag_id ORDER BY run_date)) / 
        NULLIF(LAG(avg_duration) OVER (PARTITION BY dag_id ORDER BY run_date), 0) * 100,
        2
    ) as duration_change_pct,
    avg_success_rate,
    avg_throughput,
    retries
FROM daily_metrics
ORDER BY dag_id, run_date DESC;
```

**Visualization:** Table with conditional formatting
- Green: duration_change_pct < 0 (faster)
- Red: duration_change_pct > 10 (slower)
- Metrics: avg_duration, duration_change_pct, avg_throughput

---

## 8. Pipeline Health Dashboard

**Use Case:** Executive dashboard showing overall pipeline health

```sql
SELECT 
    dag_id,
    COUNT(*) as total_runs_last_7_days,
    ROUND(AVG(total_duration_minutes), 2) as avg_duration_min,
    ROUND(AVG(success_rate), 2) as avg_success_rate,
    SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(AVG(throughput_records_per_second), 2) as avg_throughput_rps,
    SUM(total_retries) as total_retries,
    MAX(execution_date) as last_run
FROM dag_execution_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dag_id
ORDER BY dag_id;
```

**Visualization:** Scorecard/KPI cards
- Cards: total_runs, avg_success_rate, avg_duration_min, avg_throughput_rps
- Status indicators based on thresholds

---

## 9. Anomaly Detection

**Use Case:** Detect abnormal execution times or failure spikes

```sql
WITH stats AS (
    SELECT 
        dag_id,
        AVG(total_duration_minutes) as mean_duration,
        STDDEV(total_duration_minutes) as stddev_duration,
        AVG(success_rate) as mean_success_rate
    FROM dag_execution_metrics
    WHERE execution_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY dag_id
)
SELECT 
    m.dag_id,
    m.run_id,
    m.execution_date,
    m.total_duration_minutes,
    s.mean_duration,
    s.stddev_duration,
    ROUND(
        (m.total_duration_minutes - s.mean_duration) / NULLIF(s.stddev_duration, 0),
        2
    ) as z_score,
    m.success_rate,
    CASE 
        WHEN ABS((m.total_duration_minutes - s.mean_duration) / NULLIF(s.stddev_duration, 0)) > 2 
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END as status
FROM dag_execution_metrics m
JOIN stats s ON m.dag_id = s.dag_id
WHERE m.execution_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY ABS(z_score) DESC;
```

**Visualization:** Scatter plot
- X-axis: execution_date
- Y-axis: total_duration_minutes
- Color by: status (ANOMALY in red)
- Reference line: mean_duration ± 2*stddev_duration

---

## 10. Weekly Performance Summary

**Use Case:** Weekly report for stakeholders

```sql
SELECT 
    DATE_TRUNC('week', execution_date) as week_start,
    dag_id,
    COUNT(*) as total_runs,
    ROUND(AVG(total_duration_minutes), 2) as avg_duration_min,
    ROUND(MIN(total_duration_minutes), 2) as best_time_min,
    ROUND(MAX(total_duration_minutes), 2) as worst_time_min,
    ROUND(AVG(success_rate), 2) as avg_success_rate,
    SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    SUM((data_volumes->>'total_records')::int) as total_records_processed,
    ROUND(AVG(throughput_records_per_second), 2) as avg_throughput
FROM dag_execution_metrics
WHERE execution_date >= CURRENT_DATE - INTERVAL '12 weeks'
GROUP BY DATE_TRUNC('week', execution_date), dag_id
ORDER BY week_start DESC, dag_id;
```

**Visualization:** Multi-metric table
- Group by: week_start
- Sort by: week_start DESC
- Highlight: Best/worst performance metrics

---

## Metabase Dashboard Layout Recommendations

### Dashboard 1: Real-Time Operations
- **Top Row:** Pipeline Health Dashboard (Query 8) - KPI cards
- **Second Row:** Latest Daily Performance (Query 1) - table
- **Third Row:** Success Rate chart (Query 4) - stacked bar

### Dashboard 2: Performance Trends
- **Top Row:** Latency Trend (Query 2) - line chart with bands
- **Second Row:** Throughput Analysis (Query 3) - area chart
- **Third Row:** Day-over-Day Comparison (Query 7) - table

### Dashboard 3: Deep Dive
- **Top Row:** Task Performance Breakdown (Query 5) - horizontal bar
- **Second Row:** Slowest Tasks (Query 6) - table with heatmap
- **Third Row:** Anomaly Detection (Query 9) - scatter plot

### Dashboard 4: Weekly Reports
- **Full Page:** Weekly Performance Summary (Query 10) - detailed table

---

## Alerting Thresholds

Set up alerts in Metabase for:

1. **Success Rate Alert:**
   - Trigger when: `avg_success_rate < 95%` in last 24 hours
   
2. **Latency Alert:**
   - Trigger when: `total_duration_minutes > mean + 2*stddev`
   
3. **Failure Alert:**
   - Trigger when: `state = 'failed'` for any run
   
4. **Throughput Alert:**
   - Trigger when: `throughput_records_per_second < threshold`

---

## Usage Instructions

1. **Import to Metabase:**
   - Navigate to Metabase > New Question > Native Query
   - Paste any query above
   - Save with descriptive name

2. **Create Dashboard:**
   - Create new dashboard
   - Add saved questions
   - Arrange using layout recommendations

3. **Schedule Reports:**
   - Set up email subscriptions for weekly summaries
   - Configure Slack alerts for anomalies

4. **Customize:**
   - Adjust time intervals (`INTERVAL '30 days'`) as needed
   - Modify thresholds based on your pipeline SLAs
   - Add filters for specific DAGs or date ranges
