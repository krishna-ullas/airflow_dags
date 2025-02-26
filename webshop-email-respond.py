from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import re
import logging
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
        raise ValueError(f"❌ Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    return service

def get_ai_response(user_query):
    client = Client(
        host='http://agentomatic:8080',  
        headers={'x-ltai-client': 'webshop-email-respond'}
    )

    response = client.chat(
        model='webshop-email:0.5',
        messages=[{"role": "user", "content": user_query}],
        stream=False  
    )

    return response['message']['content']  

def clean_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator="\n")

    text = text.strip()
    text = re.sub(r'^["\']{3}|["\']{3}$', '', text).strip()
    text = re.sub(r'\n{2,}', '\n', text).strip()

    return text

def send_response(**kwargs):
    email_data = kwargs['dag_run'].conf  

    logging.info(f"Received email data: {email_data}")  

    if "headers" not in email_data:
        raise KeyError("Missing 'headers' key in email data.")

    service = authenticate_gmail()

    sender_email = email_data["headers"].get("From", "")
    subject = f"Re: {email_data['headers'].get('Subject', 'No Subject')}"
    user_query = email_data["content"]

    ai_response_html = get_ai_response(user_query)
    ai_response_text = clean_html(ai_response_html)

    msg = MIMEMultipart()
    msg["From"] = "me"
    msg["To"] = sender_email
    msg["Subject"] = subject
    msg.attach(MIMEText(ai_response_text, "plain"))
    msg.attach(MIMEText(ai_response_html, "html"))

    service.users().messages().send(userId="me", body={"raw": base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")}).execute()

with DAG("webshop-email-respond", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    send_response_task = PythonOperator(task_id="send-response", python_callable=send_response, provide_context=True)
    send_response_task
