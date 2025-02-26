from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import re
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client  # AI agent integration
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fetch configuration variables from Airflow
EMAIL_ACCOUNT = Variable.get("EMAIL_ID")  # Fetch email from Airflow Variables
GMAIL_CREDENTIALS = Variable.get("GMAIL_CREDENTIALS", deserialize_json=True)  # Fetch OAuth credentials

def authenticate_gmail():
    """Authenticate Gmail API and verify the correct email account is used."""
    creds = Credentials.from_authorized_user_info(GMAIL_CREDENTIALS)
    service = build("gmail", "v1", credentials=creds)

    # Fetch authenticated email
    profile = service.users().getProfile(userId="me").execute()
    logged_in_email = profile.get("emailAddress", "")

    if logged_in_email.lower() != EMAIL_ACCOUNT.lower():
        raise ValueError(f"❌ Wrong Gmail account! Expected {EMAIL_ACCOUNT}, but got {logged_in_email}")

    print(f"✅ Authenticated Gmail Account: {logged_in_email}")
    return service

def get_ai_response(user_query):
    """Send the email content to the AI agent and return the response."""
    client = Client(
        host='http://agentomatic:8080',  # Replace with actual agent server
        headers={'x-ltai-client': 'webshop-email-respond'}
    )

    response = client.chat(
        model='webshop-email:0.5',  # Use your actual AI model name
        messages=[{"role": "user", "content": user_query}],
        stream=False  # False ensures we get the full response at once
    )

    return response['message']['content']  # Assuming the AI returns an HTML formatted response

def clean_html(html_content):
    """Remove HTML tags, triple quotes, and ensure compact formatting."""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator="\n")  # Extract text with line breaks

    # Remove leading and trailing triple quotes (both types)
    text = text.strip()
    text = re.sub(r'^["\']{3}|["\']{3}$', '', text).strip()

    # Remove excessive newlines (more than 2 consecutive)
    text = re.sub(r'\n{2,}', '\n', text).strip()

    return text

def send_response(**kwargs):
    """Fetch user query, get AI-generated response, and send formatted email."""
    email_data = kwargs['dag_run'].conf  
    service = authenticate_gmail()

    sender_email = email_data["headers"].get("From", "")
    subject = f"Re: {email_data['headers'].get('Subject', 'No Subject')}"
    
    user_query = email_data["content"]  

    # Get AI-generated response (HTML formatted)
    ai_response_html = get_ai_response(user_query)

    # Convert AI response to compact plain text (removing HTML, extra spaces, and triple quotes)
    ai_response_text = clean_html(ai_response_html)

    # Construct email
    msg = MIMEMultipart()
    msg["From"] = "me"
    msg["To"] = sender_email
    msg["Subject"] = subject
    msg.attach(MIMEText(ai_response_text, "plain"))  # Compact plain text
    msg.attach(MIMEText(ai_response_html, "html"))   # HTML for formatting

    # Encode and send the email
    encoded_message = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
    try:
        service.users().messages().send(userId="me", body={"raw": encoded_message}).execute()
        print(f"✅ Response sent to {sender_email}")
    except Exception as e:
        print(f"❌ ERROR: Failed to send response email to {sender_email}. Error: {e}")

# Define DAG
with DAG("webshop-email-respond",
         default_args=default_args,
         schedule_interval=None,  # This DAG is triggered dynamically
         catchup=False) as dag:

    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )

    send_response_task
