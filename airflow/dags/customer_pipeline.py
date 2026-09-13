from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2026, 9, 13),
    schedule=None,
    catchup=False,
    tags=["customer", "data-engineering"],
) as dag:

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            "cd /opt/airflow && "
            "python src/bronze/write_customers_bronze.py"
        ),
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            "cd /opt/airflow && "
            "python src/silver/clean_customers_silver.py"
        ),
    )

    delta_silver = BashOperator(
        task_id="create_delta_silver",
        bash_command=(
            "cd /opt/airflow && "
            "python src/silver/create_delta_silver.py"
        ),
    )

    gold_transformation = BashOperator(
        task_id="create_customer_gold",
        bash_command=(
            "cd /opt/airflow && "
            "python src/gold/create_customer_gold.py"
        ),
    )

    data_quality = BashOperator(
        task_id="data_quality",
        bash_command=(
            "cd /opt/airflow && "
            "python src/quality/customer_data_quality.py"
        ),
    )

    audit_reconciliation = BashOperator(
        task_id="audit_reconciliation",
        bash_command=(
            "cd /opt/airflow && "
            "python src/audit/reconciliation_audit.py"
        ),
    )

    (
        bronze_ingestion
        >> silver_transformation
        >> delta_silver
        >> gold_transformation
        >> data_quality
        >> audit_reconciliation
    )
