from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import time
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fetch values from Airflow Variables
EMAIL_ACCOUNT = Variable.get("EMAIL_ID")  # Gmail account email
GMAIL_CREDENTIALS = json.loads(Variable.get("GMAIL_CREDENTIALS"))  # OAuth 2.0 credentials

def authenticate_gmail():
    """Authenticate Gmail API using OAuth 2.0 tokens stored in Airflow Variables."""
    creds = Credentials(
        token=GMAIL_CREDENTIALS["access_token"],
        refresh_token=GMAIL_CREDENTIALS["refresh_token"],
        token_uri=GMAIL_CREDENTIALS["token_uri"],
        client_id=GMAIL_CREDENTIALS["client_id"],
        client_secret=GMAIL_CREDENTIALS["client_secret"],
        scopes=GMAIL_CREDENTIALS["scopes"]
    )

    service = build("gmail", "v1", credentials=creds)

    # Verify authentication
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ACCOUNT.lower():
        raise ValueError(f"Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    print(f"✅ Authenticated Gmail Account: {logged_in_email}")
    return service

def fetch_unread_emails(**kwargs):
    """Fetch unread emails from Gmail."""
    service = authenticate_gmail()
    
    query = "is:unread"

    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])

    unread_emails = []

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        body = msg_data.get("snippet", "")
        unread_emails.append({"id": msg["id"], "headers": headers, "content": body})

    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)

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
        """Trigger 'webshop-email-respond' for each unread email."""
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
