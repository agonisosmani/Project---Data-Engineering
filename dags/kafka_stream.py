import uuid
import logging
import json
import time
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Default arguments for the DAG
default_args = {
    'owner': 'agonis',
    'start_date': datetime(2024, 9, 3, 10, 0),
}

def fetch_user_data(api_url="https://randomuser.me/api/"):
    """Fetches user data from the specified API URL."""
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        user_data = response.json()['results'][0]
        logging.info("User data fetched successfully.")
        return user_data
    except requests.RequestException as e:
        logging.error(f"Failed to fetch user data: {e}")
        return None

def format_user_data(raw_data):
    """Formats the raw user data into the required structure."""
    if raw_data is None:
        logging.error("No data to format. Skipping formatting.")
        return None

    location = raw_data.get('location', {})
    formatted_data = {
        'id': str(uuid.uuid4()),
        'first_name': raw_data.get('name', {}).get('first', ''),
        'last_name': raw_data.get('name', {}).get('last', ''),
        'gender': raw_data.get('gender', ''),
        'address': f"{location.get('street', {}).get('number', '')} {location.get('street', {}).get('name', '')}, "
                   f"{location.get('city', '')}, {location.get('state', '')}, {location.get('country', '')}",
        'post_code': location.get('postcode', ''),
        'email': raw_data.get('email', ''),
        'username': raw_data.get('login', {}).get('username', ''),
        'dob': raw_data.get('dob', {}).get('date', ''),
        'registered_date': raw_data.get('registered', {}).get('date', ''),
        'phone': raw_data.get('phone', ''),
        'picture': raw_data.get('picture', {}).get('medium', '')
    }
    logging.info("User data formatted successfully.")
    return formatted_data

def stream_data_to_kafka(topic='users_created', kafka_servers=['broker:29092'], timeout=60):
    """Streams formatted user data to a Kafka topic."""
    producer = KafkaProducer(bootstrap_servers=kafka_servers, max_block_ms=5000)
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            raw_data = fetch_user_data()
            formatted_data = format_user_data(raw_data)

            if formatted_data:
                producer.send(topic, json.dumps(formatted_data).encode('utf-8'))
                logging.info("Data streamed to Kafka successfully.")
            else:
                logging.warning("Skipping Kafka streaming due to missing data.")

        except Exception as e:
            logging.error(f"An error occurred while streaming data: {e}")

        time.sleep(5)  # To avoid rapid requests, sleep for a few seconds between iterations

    producer.close()
    logging.info("Streaming to Kafka completed.")

# Define the Airflow DAG
with DAG(
    dag_id='user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='A DAG for automating user data streaming to Kafka',
) as dag:

    # Task for streaming data
    stream_data_task = PythonOperator(
        task_id='stream_data_to_kafka',
        python_callable=stream_data_to_kafka,
    )
