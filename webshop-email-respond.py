from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import base64
import os
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

def authenticate_gmail():
    """Authenticate Gmail API"""
    creds = None
    if os.path.exists(CREDENTIALS_PATH):
        creds = Credentials.from_authorized_user_file(CREDENTIALS_PATH)
    return build("gmail", "v1", credentials=creds)

def send_response(**kwargs):
    """Send auto-response"""
    email_data = kwargs['dag_run'].conf  # Get email details from trigger

    service = authenticate_gmail()
    recipient = email_data["headers"].get("From", "")

    message = f"Hi,\n\nThanks for the email. Will respond with more details soon.\n\nBest,\nWebshop Support"
    email_msg = f"From: me\nTo: {recipient}\nSubject: Re: {email_data['headers'].get('Subject', 'No Subject')}\n\n{message}"
    encoded_message = base64.urlsafe_b64encode(email_msg.encode("utf-8")).decode("utf-8")

    service.users().messages().send(
        userId="me",
        body={"raw": encoded_message}
    ).execute()

# Define DAG
with DAG("webshop-email-respond",
         default_args=default_args,
         schedule_interval=None,  # This DAG is triggered by another DAG
         catchup=False) as dag:

    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )

    send_response_task
