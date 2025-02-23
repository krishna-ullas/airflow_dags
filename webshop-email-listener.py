from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import base64
import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CREDENTIALS_PATH = "/appz/home/airflow/dags/credentials.json"
EMAIL_ACCOUNT = "webshop-support-agent-6981@lowtouch.ai"

def authenticate_gmail():
    """Authenticate Gmail API"""
    creds = None
    if os.path.exists(CREDENTIALS_PATH):
        creds = Credentials.from_authorized_user_file(CREDENTIALS_PATH)
    return build("gmail", "v1", credentials=creds)

def fetch_unread_emails(**kwargs):
    """Fetch unread emails and push them to XCom"""
    service = authenticate_gmail()
    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q="is:unread").execute()
    messages = results.get("messages", [])

    emails = []
    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()

        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        body = msg_data.get("snippet", "")

        emails.append({"id": msg["id"], "headers": headers, "content": body})

    kwargs['ti'].xcom_push(key="unread_emails", value=emails)

# Define DAG
with DAG("webshop-email-listener",
         default_args=default_args,
         schedule_interval=timedelta(minutes=1),
         catchup=False) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
        provide_context=True
    )

    def trigger_response_tasks(**kwargs):
        """Trigger 'webshop-email-respond' for each unread email"""
        ti = kwargs['ti']
        unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")

        if unread_emails:
            for email in unread_emails:
                TriggerDagRunOperator(
                    task_id=f"trigger_response_{email['id']}",
                    trigger_dag_id="webshop-email-respond",
                    conf=email
                ).execute(context=kwargs)

    trigger_email_response_task = PythonOperator(
        task_id="trigger-email-response-dag",
        python_callable=trigger_response_tasks,
        provide_context=True
    )

    fetch_emails_task >> trigger_email_response_task
