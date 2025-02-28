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

EMAIL_ACCOUNT = Variable.get("EMAIL_ID")  # Fetching the email ID from Airflow Variable
GMAIL_CREDENTIALS = Variable.get("GMAIL_CREDENTIALS", deserialize_json=True)  # Gmail API credentials

def authenticate_gmail():
    """Authenticate and return the Gmail API service."""
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ACCOUNT.lower():
        raise ValueError(f"Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    return service

def get_send_as_name(service):
    """Fetch the configured 'Send Mail As' display name dynamically from Gmail settings."""
    try:
        send_as_list = service.users().settings().sendAs().list(userId="me").execute()
        
        for send_as in send_as_list.get("sendAs", []):
            if send_as["sendAsEmail"].lower() == EMAIL_ACCOUNT.lower():
                display_name = send_as.get("displayName", EMAIL_ACCOUNT)  # Default to email if no display name is set
                logging.info(f"Fetched Send Mail As name: {display_name}")
                return display_name
        
        logging.warning("No matching 'Send Mail As' name found. Using email address instead.")
        return EMAIL_ACCOUNT  # Fallback if no match is found
    except Exception as e:
        logging.error(f"Failed to fetch 'Send Mail As' name: {str(e)}")
        return EMAIL_ACCOUNT  # Fallback in case of API failure

def get_ai_response(user_query):
    """Fetch response from AI agent."""
    client = Client(
        host='http://agentomatic:8000',
        headers={'x-ltai-client': 'webshop-email-respond'}
    )

    response = client.chat(
        model='webshop-email:0.5',
        messages=[{"role": "user", "content": user_query}],
        stream=False
    )
    agent_response = response['message']['content']
    logging.info(f"Agent Response: {agent_response}")
    return agent_response

def send_response(**kwargs):
    """Retrieve unread email data and send an AI-generated response."""
    email_data = kwargs['dag_run'].conf.get("email_data", {})  

    logging.info(f"Received email data: {email_data}")  

    if not email_data:
        logging.warning("No email data received! This DAG was likely triggered manually.")
        return  

    service = authenticate_gmail()

    # Fetch the dynamically set "Send Mail As" display name
    send_as_name = get_send_as_name(service)

    sender_email = email_data["headers"].get("From", "")
    subject = f"Re: {email_data['headers'].get('Subject', 'No Subject')}"
    user_query = email_data["content"]
    ai_response_html = get_ai_response(user_query)
    
    # Cleaning up AI response if needed
    ai_response_html = re.sub(r"^```(?:html)?\n?|```$", "", ai_response_html.strip(), flags=re.MULTILINE)

    # Construct email
    msg = MIMEMultipart()
    msg["From"] = f"{send_as_name} <{EMAIL_ACCOUNT}>"
    msg["To"] = sender_email
    msg["Subject"] = subject
    msg.attach(MIMEText(ai_response_html, "html"))

    raw_message = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")

    try:
        service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
        logging.info(f"Email successfully sent to {sender_email} from {send_as_name} <{EMAIL_ACCOUNT}>")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")

# Define DAG
with DAG("webshop-email-respond", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )
    send_response_task
