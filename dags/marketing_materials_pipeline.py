from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import easyocr
import re
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_text(image_path, **kwargs):
    reader = easyocr.Reader(['en'])
    result = reader.readtext(image_path)
    extracted_text = " ".join([res[1] for res in result])
    kwargs['ti'].xcom_push(key='extracted_text', value=extracted_text)


def extract_domains(**kwargs):
    text = kwargs['ti'].xcom_pull(key='extracted_text', task_ids='ocr_task')
    pattern = r'\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9][a-z0-9-]{0,61}[a-z0-9]\b'
    domains = re.findall(pattern, text)
    kwargs['ti'].xcom_push(key='domains', value=domains)


def enrich_data(**kwargs):
    domains = kwargs['ti'].xcom_pull(key='domains', task_ids='domain_extraction_task')
    enriched_data = []
    for domain in domains:
        response = make_brandfetch_api_request(domain)
        if response:
            enriched_data.append({'domain': domain, 'additional_info': response})
    kwargs['ti'].xcom_push(key='enriched_data', value=enriched_data)


def make_brandfetch_api_request(domain):
    api_key = 'YOUR_BRANDFETCH_API_KEY'
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        response = requests.get(f'https://api.brandfetch.io/v1/brand/domain/{domain}', headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f'Failed to fetch data for {domain}: {response.status_code}')
            return None
    except requests.exceptions.RequestException as e:
        print(f'Error during request for {domain}: {e}')
        return None


def deduplication(**kwargs):
    enriched_data = kwargs['ti'].xcom_pull(key='enriched_data', task_ids='data_enrichment_task')
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    deduplicated_data = []
    for data in enriched_data:
        cursor.execute("SELECT count(*) FROM companies WHERE domain = %s", (data['domain'],))
        result = cursor.fetchone()
        if result[0] == 0:
            deduplicated_data.append(
                {'domain': data['domain'], 'additional_info': data['additional_info'], 'action': 'insert'})
        else:
            deduplicated_data.append(
                {'domain': data['domain'], 'additional_info': data['additional_info'], 'action': 'update'})

    kwargs['ti'].xcom_push(key='deduplicated_data', value=deduplicated_data)
    cursor.close()
    conn.close()


def insertion_update(**kwargs):
    deduplicated_data = kwargs['ti'].xcom_pull(key='deduplicated_data', task_ids='deduplication_task')
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for data in deduplicated_data:
        if data['action'] == 'insert':
            insert_query = "INSERT INTO companies (domain, additional_info) VALUES (%s, %s)"
            cursor.execute(insert_query, (data['domain'], data['additional_info']))
        elif data['action'] == 'update':
            update_query = "UPDATE companies SET additional_info = %s WHERE domain = %s"
            cursor.execute(update_query, (data['additional_info'], data['domain']))

    conn.commit()
    cursor.close()
    conn.close()


with DAG('marketing_materials_pipeline', default_args=default_args, schedule_interval='@daily') as dag:
    ocr_task = PythonOperator(
        task_id='ocr_task',
        python_callable=extract_text,
        op_kwargs={'image_path': '/path/to/your/image.jpg'},
    )

    domain_extraction_task = PythonOperator(
        task_id='domain_extraction_task',
        python_callable=extract_domains,
    )

    data_enrichment_task = PythonOperator(
        task_id='data_enrichment_task',
        python_callable=enrich_data,
    )

    deduplication_task = PythonOperator(
        task_id='deduplication_task',
        python_callable=deduplication,
    )

    insertion_update_task = PythonOperator(
        task_id='insertion_update_task',
        python_callable=insertion_update,
    )

    ocr_task >> domain_extraction_task >> data_enrichment_task >> deduplication_task >> insertion_update_task
