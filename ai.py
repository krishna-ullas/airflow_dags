from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtRunOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = "/appz/home/airflow/dags/dbt/ai_lab"  
DBT_PROFILES_DIR = "/appz/home/airflow/dags/dbt/ai_lab"  

with DAG(
    dag_id="dbt_order_update",
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "postgres"],
) as dag:
    
    run_dbt = DbtRunOperator(
        task_id="run_dbt_order_update",
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        models="transformations.order_update",
    )

    run_dbt
