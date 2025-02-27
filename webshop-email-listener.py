from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import time
import logging
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

# Configuration variables
EMAIL_ACCOUNT = Variable.get("EMAIL_ID")  
GMAIL_CREDENTIALS = Variable.get("GMAIL_CREDENTIALS", deserialize_json=True)  
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/last_processed_email.json"  # Track last responded email timestamp

def authenticate_gmail():
    """Authenticate Gmail API and verify the correct email account is used."""
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ACCOUNT.lower():
        raise ValueError(f"❌ Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    logging.info(f"✅ Authenticated Gmail Account: {logged_in_email}")
    return service

def get_last_checked_timestamp():
    """Retrieve the last processed timestamp, ensuring it's in milliseconds."""
    if os.path.exists(LAST_PROCESSED_EMAIL_FILE):
        with open(LAST_PROCESSED_EMAIL_FILE, "r") as f:
            last_checked = json.load(f).get("last_processed", None)
            if last_checked:
                logging.info(f"📅 Retrieved last processed email timestamp (milliseconds): {last_checked}")
                return last_checked

    # If no previous timestamp, fetch ALL unread emails
    logging.info(f"🚀 No previous timestamp found, fetching all unread emails.")
    return None  # This tells fetch_unread_emails() to check all unread emails

def update_last_checked_timestamp(timestamp):
    """Ensure the timestamp is stored in milliseconds and prevent reprocessing."""
    new_timestamp = timestamp + 1  # ✅ Add 1ms to prevent reprocessing the same email
    os.makedirs(os.path.dirname(LAST_PROCESSED_EMAIL_FILE), exist_ok=True)
    with open(LAST_PROCESSED_EMAIL_FILE, "w") as f:
        json.dump({"last_processed": new_timestamp}, f)
    logging.info(f"✅ Updated last processed email timestamp (milliseconds): {new_timestamp}")

def fetch_unread_emails(**kwargs):
    """Fetch unread emails received after the last processed email timestamp."""
    service = authenticate_gmail()
    
    last_checked_timestamp = get_last_checked_timestamp()

    if last_checked_timestamp:
        query = f"is:unread after:{last_checked_timestamp // 1000}"  # Convert ms to s for Gmail API
    else:
        query = "is:unread"  # Fetch ALL unread emails if no timestamp exists

    logging.info(f"🔍 Fetching emails with query: {query}")

    results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
    messages = results.get("messages", [])

    logging.info(f"📬 Found {len(messages)} unread emails.")

    unread_emails = []
    max_timestamp = last_checked_timestamp or int(time.time() * 1000)  # Default to current time if no timestamp exists

    for msg in messages:
        msg_data = service.users().messages().get(userId="me", id=msg["id"]).execute()
        
        headers = {header["name"]: header["value"] for header in msg_data["payload"]["headers"]}
        sender = headers.get("From", "").lower()
        timestamp = int(msg_data["internalDate"])  # Already in milliseconds

        logging.info(f"📨 Processing email from {sender}, timestamp: {timestamp}")

        if "no-reply" in sender:
            logging.info(f"⏩ Skipping email from {sender} (timestamp: {timestamp})")
            continue

        body = msg_data.get("snippet", "")
        email_object = {
            "id": msg["id"],
            "headers": headers,  
            "content": body,
            "timestamp": timestamp
        }

        logging.info(f"✅ Adding unread email to XCom: {email_object}")
        unread_emails.append(email_object)

        if timestamp > max_timestamp:
            max_timestamp = timestamp  # ✅ Keep track of the latest email timestamp

    # ✅ Push emails to XCom BEFORE updating the timestamp
    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)

    if unread_emails:
        update_last_checked_timestamp(max_timestamp)  # ✅ Update timestamp only after pushing to XCom
        logging.info(f"✅ Updated last processed email timestamp: {max_timestamp}")

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

        logging.info(f"📥 Retrieved {len(unread_emails) if unread_emails else 0} unread emails from XCom.")

        if not unread_emails:
            logging.info("🚫 No unread emails found in XCom, skipping trigger.")
            return

        for email in unread_emails:
            task_id = f"trigger_response_{email['id'].replace('-', '_')}"  

            logging.info(f"🚀 Triggering Response DAG with email data: {email}")  

            trigger_task = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id="webshop-email-respond",
                conf={"email_data": email},
            )

            trigger_task.execute(context=kwargs)  

        # ✅ Clear XCom to prevent reprocessing the same emails
        ti.xcom_push(key="unread_emails", value=[])

    trigger_email_response_task = PythonOperator(
        task_id="trigger-email-response-dag",
        python_callable=trigger_response_tasks,
        provide_context=True
    )

    fetch_emails_task >> trigger_email_response_task
