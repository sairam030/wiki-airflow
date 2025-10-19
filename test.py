from dags.tasks.clean_top_pages import clean_top_pages

# Mock Airflow context
class DummyContext:
    def __init__(self, file_path):
        self.task_instance = self
        self._xcom = {"fetch_top_pages": file_path}

    def xcom_pull(self, task_ids):
        return self._xcom.get(task_ids)

if __name__ == "__main__":
    # Replace with your actual downloaded JSON file path
    file_path = "/path/to/output/top_pages_2025_10_17.json"
    context = DummyContext(file_path)
    output = clean_top_pages(**{"task_instance": context})
    print(f"Output path: {output}")
