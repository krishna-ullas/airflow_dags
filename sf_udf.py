from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup

SNOWFLAKE_CONN_ID = "snowflake_connection"

with DAG(
    "sf_udf",
    description="""
        Example DAG for udf in Snowflake.
    """,
    doc_md=__doc__,
    start_date=datetime(2022, 12, 1),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    schedule=None,
    # defining the directory where SQL templates are stored
    template_searchpath="/appz/home/airflow/dags/airflow_dags/",
    catchup=False,
) as dag:

  create_udf = SnowflakeOperator(
        task_id="create_udf",
        sql="test.sql",
    )
  begin = EmptyOperator(task_id="begin")
  end = EmptyOperator(task_id="end")

  chain(
      begin,
      create_udf,
      end,
 )