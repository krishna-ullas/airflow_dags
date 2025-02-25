from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.providers.google.common.hooks.discovery_api import GoogleDiscoveryApiHook
from datetime import datetime, timedelta
import json
import time

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Configuration variables
EMAIL_ID = Variable.get("EMAIL_ID")  # Fetch from Airflow Variables
LAST_CHECK_TIMESTAMP_FILE = "/appz/cache/last_checked_timestamp.json"

def authenticate_gmail():
    """Authenticate Gmail API using Airflow's Google Cloud Connection."""
    
    hook = GoogleDiscoveryApiHook(
        gcp_conn_id="gmail_api",  # Using Airflow Connection
        api_service_name="gmail",
        api_version="v1",
        scopes=["https://www.googleapis.com/auth/gmail.readonly"]
    )
    
    service = hook.get_conn()

    # Fetch authenticated email
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ID.lower():
        raise ValueError(f"Wrong Gmail account! Expected {EMAIL_ID}, but got {logged_in_email}")

    print(f"✅ Authenticated Gmail Account: {logged_in_email}")
    return service

def get_last_checked_timestamp():
    """Retrieve the last processed timestamp, or initialize it with the current timestamp."""
    try:
        with open(LAST_CHECK_TIMESTAMP_FILE, "r") as f:
            return json.load(f).get("last_checked", int(time.time()))
    except FileNotFoundError:
        current_timestamp = int(time.time())  # Get current timestamp in seconds
        update_last_checked_timestamp(current_timestamp)
        return current_timestamp

def update_last_checked_timestamp(timestamp):
    """Update the last processed timestamp in the file."""
    with open(LAST_CHECK_TIMESTAMP_FILE, "w") as f:
        json.dump({"last_checked": timestamp}, f)

def fetch_unread_emails(**kwargs):
    """Fetch unread emails received after the last processed timestamp."""
    service = authenticate_gmail()
    
    last_checked_timestamp = get_last_checked_timestamp()
    query = f"is:unread after:{last_checked_timestamp}"

    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])

    unread_emails = []
    max_timestamp = last_checked_timestamp

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        
        # Extract email details
        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])  # Timestamp in milliseconds

        # Skip no-reply emails and previously processed emails
        if "no-reply" in sender or timestamp <= last_checked_timestamp:
            continue

        body = msg_data.get("snippet", "")
        unread_emails.append({"id": msg["id"], "headers": headers, "content": body, "timestamp": timestamp})

        if timestamp > max_timestamp:
            max_timestamp = timestamp

    # Update last checked timestamp only if new emails were found
    if unread_emails:
        update_last_checked_timestamp(max_timestamp)

    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)

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
