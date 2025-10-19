Recommended project layout (minimal, corporate-friendly)

.
├── dags/                 # Airflow DAG definitions (thin: import tasks from src)
├── src/                  # Application/business logic as an importable package
│   ├── __init__.py
│   └── fetch_top_pages.py
├── scripts/              # Small executable scripts (CI, local runs)
├── plugins/              # Airflow plugins (if needed)
├── logs/
├── docker-compose.yml
└── README.md

Notes:
- Keep heavy imports inside functions to avoid DAG import-time failures.
- Use PYTHONPATH (configured in docker-compose) so DAGs import src.* modules.
- Use scripts/ for standalone runs and CI tasks.
# wiki-airflow
# wiki-airflow
# wiki-airflow
# wiki-airflow
# wiki-airflow
