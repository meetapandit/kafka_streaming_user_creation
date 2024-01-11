import requests
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import json
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'datascale',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 27),
    'email': ['datascale@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)

}
def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, "\
          f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['resgistered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    res = get_data()
    res = format_data(res)

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], 
                             api_version=(2, 5, 0))
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
            
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG(
    'user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )