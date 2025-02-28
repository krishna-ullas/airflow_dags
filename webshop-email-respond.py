from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import logging
import re
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client  
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

EMAIL_ACCOUNT = Variable.get("EMAIL_ID")  
GMAIL_CREDENTIALS = Variable.get("GMAIL_CREDENTIALS", deserialize_json=True)  

def authenticate_gmail():
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ACCOUNT.lower():
        raise ValueError(f" Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    return service

def get_ai_response(user_query):
    client = Client(
        host='http://agentomatic:8000',
        headers={'x-ltai-client': 'webshop-email-respond'}
    )

    try:
        response = client.chat(
            model='webshop-email:0.5',
            messages=[{"role": "user", "content": user_query}],
            stream=False
        )
        agent_response = response['message']['content']
        logging.info(f" Agent Response: {agent_response}")
        return agent_response
    except Exception as e:
        logging.error(f" AI Response Generation Failed: {e}")
        return None  # Return None to indicate failure

def send_response(**kwargs):
    email_data = kwargs['dag_run'].conf.get("email_data", {})

    logging.info(f"Received email data: {email_data}")

    if not email_data:
        logging.warning("No email data received! This DAG was likely triggered manually.")
        return  

    service = authenticate_gmail()

    sender_email = email_data["headers"].get("From", "")
    subject = f"Re: {email_data['headers'].get('Subject', 'No Subject')}"
    user_query = email_data["content"]

    ai_response_html = get_ai_response(user_query)

    # Check if AI response contains an error or is empty
    if not ai_response_html or "error" in ai_response_html.lower():
        logging.error(" Invalid AI response received! Not sending to user.")
        send_failure_alert(service, user_query, "AI response was empty or invalid.")
        return  # Stop further processing

    # Clean AI response
    ai_response_html = re.sub(r"^```(?:html)?\n?|```$", "", ai_response_html.strip(), flags=re.MULTILINE)
    display_name = "webshop:0.5 via lowtouch-ai"
    # Send the response email to the user
    msg = MIMEMultipart()
    msg["From"] = f'"{display_name}" <me>'
    msg["To"] = sender_email
    msg["Subject"] = subject
    msg.attach(MIMEText(ai_response_html, "html"))

    service.users().messages().send(userId="me", body={
        "raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
    }).execute()

def send_failure_alert(service, user_query, error_message):
    """Send an alert email to maintainers when AI response fails."""
    maintainers = Variable.get("MAINTAINERS_EMAILS", deserialize_json=True)  # List of maintainer emails

    alert_subject = " AI Email Response Failure Alert!"
    alert_body = f"""
    <p><strong>Issue Detected:</strong> AI email response failed.</p>
    <p><strong>User Query:</strong> {user_query}</p>
    <p><strong>Error Details:</strong> {error_message}</p>
    <p>Please check the system and resolve the issue.</p>
    """

    msg = MIMEMultipart()
    msg["From"] = "me"
    msg["To"] = ", ".join(maintainers)  # Send to all maintainers
    msg["Subject"] = alert_subject
    msg.attach(MIMEText(alert_body, "html"))

    service.users().messages().send(userId="me", body={
        "raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
    }).execute()

    logging.info(f" Alert sent to maintainers: {maintainers}")

with DAG("webshop-email-respond", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    send_response_task = PythonOperator(task_id="send-response", python_callable=send_response, provide_context=True)
    send_response_task
