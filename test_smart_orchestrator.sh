#!/bin/bash
# Test Smart Orchestrator System

echo "🧪 Testing Smart Orchestrator System"
echo "======================================"
echo ""

echo "1️⃣ Testing Date Checker Module..."
docker exec -it airflow-scheduler python -c "
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from utils.date_checker import get_available_dates_in_diamond, find_missing_dates
print('✅ Date checker import successful')

dates = get_available_dates_in_diamond()
print(f'📅 Found {len(dates)} dates in Diamond bucket')

result = find_missing_dates(5)
print(f'📊 Last 5 days coverage: {result[\"coverage_pct\"]:.1f}%')
" || echo "❌ Date checker test failed"

echo ""
echo "2️⃣ Testing Analytics Module..."
docker exec -it airflow-scheduler python -c "
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from utils.analytics_trends import get_last_n_days_files
print('✅ Analytics module import successful')

files = get_last_n_days_files(5)
print(f'📁 Found {len(files)} analytics-ready files')
" || echo "❌ Analytics module test failed"

echo ""
echo "3️⃣ Checking DAGs..."
docker exec -it airflow-scheduler airflow dags list | grep -E "(wiki_trending|wiki_analytics|wiki_smart)" || echo "⚠️ DAGs not loaded yet"

echo ""
echo "4️⃣ Checking Database Tables..."
docker exec -it postgres psql -U airflow -d airflow -c "
SELECT table_name 
FROM information_schema.tables 
WHERE table_name LIKE 'wiki_%' 
ORDER BY table_name;
" || echo "❌ Database check failed"

echo ""
echo "📊 Summary of New Components:"
echo "======================================"
echo "✅ Date Checker: plugins/utils/date_checker.py"
echo "✅ DAG Trigger: plugins/utils/dag_trigger.py"
echo "✅ Analytics: plugins/utils/analytics_trends.py"
echo "✅ Smart Orchestrator DAG: dags/wiki_smart_orchestrator_dag.py"
echo "✅ CLI Tool: check_and_fetch_data.py"
echo ""
echo "🎯 Quick Commands:"
echo "======================================"
echo "# Check data completeness:"
echo "  docker exec -it airflow-scheduler python /opt/airflow/check_and_fetch_data.py"
echo ""
echo "# Run analytics manually:"
echo "  docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py"
echo ""
echo "# Trigger smart orchestrator:"
echo "  docker exec -it airflow-scheduler airflow dags trigger wiki_smart_analytics_orchestrator"
echo ""
echo "# View analytics report:"
echo "  docker exec -it airflow-scheduler python /opt/airflow/show_analytics_report.py"
echo ""
