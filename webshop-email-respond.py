from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import os
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

try:
    raw_credentials = Variable.get("GMAIL_CREDENTIALS")
    if not raw_credentials:
        raise ValueError("GMAIL_CREDENTIALS is empty!")

    GMAIL_CREDENTIALS = json.loads(raw_credentials)
except Exception as e:
    raise ValueError(f"❌ Error retrieving GMAIL_CREDENTIALS: {e}")

EMAIL_ID = Variable.get("EMAIL_ID")

def authenticate_gmail():
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ID.lower():
        raise ValueError(f"Expected {EMAIL_ID}, but got {logged_in_email}")

    print(f"✅ Authenticated Gmail Account: {logged_in_email}")
    return service

def send_response(**kwargs):
    email_data = kwargs['dag_run'].conf
    service = authenticate_gmail()
    
    recipient = email_data["headers"].get("From", "")

    message_body = f"Hi,\n\nThanks for your inquiry. We'll get back to you soon.\n\nBest,\nWebshop Support"
    email_msg = f"From: me\nTo: {recipient}\nSubject: Re: {email_data['headers'].get('Subject', 'No Subject')}\n\n{message_body}"
    encoded_message = base64.urlsafe_b64encode(email_msg.encode("utf-8")).decode("utf-8")

    service.users().messages().send(userId="me", body={"raw": encoded_message}).execute()

with DAG("webshop-email-respond",
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    send_response_task = PythonOperator(task_id="send-response", python_callable=send_response, provide_context=True)
