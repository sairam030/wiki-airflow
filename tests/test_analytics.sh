#!/bin/bash
# Test Analytics Module

echo "🧪 Testing Wikipedia Trends Analytics Module"
echo "=============================================="
echo ""

# Check if we have Diamond bucket data
echo "📦 Checking Diamond bucket contents..."
docker exec -it minio mc ls minio/diamond/ | head -10

echo ""
echo "🐍 Testing Python imports..."
docker exec -it airflow-scheduler python -c "
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from utils.analytics_trends import get_last_n_days_files
print('✅ Import successful')

files = get_last_n_days_files(5)
print(f'📅 Found {len(files)} days of data')
" || echo "❌ Import test failed"

echo ""
echo "💡 To run analytics manually:"
echo "   docker exec -it airflow-scheduler python /opt/airflow/run_analytics.py"
echo ""
echo "💡 To run via Airflow:"
echo "   docker exec -it airflow-scheduler airflow dags trigger wiki_analytics_trends"
echo ""
echo "💡 To query analytics tables:"
echo "   docker exec -it postgres psql -U airflow -d airflow -c 'SELECT COUNT(*) FROM wiki_trends_growth;'"
