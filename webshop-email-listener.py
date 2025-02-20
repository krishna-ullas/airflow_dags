from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Service Account JSON Path (ensure this file exists in your DAGs folder)
SERVICE_ACCOUNT_PATH = "/appz/home/airflow/dags/service_account.json"

# Gmail API Scopes (must be included even after Google Admin setup)
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

# Authenticate Gmail API using Service Account
def authenticate_gmail():
    """Authenticate Gmail API using a service account"""
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH, scopes=SCOPES)
    return build("gmail", "v1", credentials=creds)

# Fetch Unread Emails and Push to XCom
def fetch_unread_emails(**kwargs):
    """Fetch unread emails from Gmail API"""
    service = authenticate_gmail()
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q="is:unread").execute()
    messages = results.get("messages", [])

    emails = []
    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        body = msg_data.get("snippet", "")  # Short email preview

        emails.append({"id": msg["id"], "headers": headers, "content": body})

    # Push unread emails to XCom
    kwargs['ti'].xcom_push(key="unread_emails", value=emails)

# Trigger Response DAG for Each Unread Email
def trigger_response_tasks(**kwargs):
    """Trigger 'webshop-email-respond' for each unread email"""
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")

    if unread_emails:
        for email in unread_emails:
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_response_{email['id']}",
                trigger_dag_id="webshop-email-respond",
                conf=email
            )
            trigger.execute(context=kwargs)

# Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define Airflow DAG
with DAG("webshop-email-listener",
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         catchup=False) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
        provide_context=True
    )

    trigger_email_response_task = PythonOperator(
        task_id="trigger-email-response-dag",
        python_callable=trigger_response_tasks,
        provide_context=True
    )

    fetch_emails_task >> trigger_email_response_task
