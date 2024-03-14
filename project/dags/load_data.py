
import pandas as pd
import os

import requests
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

CSV_URL = "/tmp"
MCC = [262, 460, 310, 208, 510, 404, 250, 724, 234, 311]
FILE_URL = "https://datasets.clickhouse.com/cell_towers.csv.xz"


def download_and_extract_file():
    try:
        response = requests.get(FILE_URL)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f'Error fetching file: {e}')

    if os.path.exists(f'{CSV_URL}/cell_towers.csv.xz') and f'{CSV_URL}/cell_towers.csv.xz'.endswith(".csv.xz"):
        os.remove(f'{CSV_URL}/cell_towers.csv')

    with open(f'{CSV_URL}/cell_towers.csv.xz', 'wb') as f:
        f.write(response.content)

    df = pd.read_csv(f'{CSV_URL}/cell_towers.csv.xz', compression='xz', encoding='UTF-8', sep=',')
    df_filtered = df[df['mcc'].isin(MCC)]
    block_size = 100000
    blocks = [df_filtered[i:i + block_size] for i in range(0, df_filtered.shape[0], block_size)]
    for i, block in enumerate(blocks):
        block.to_csv(f"{CSV_URL}/data_block_{i}.csv", index=False)


def load_data_to_clickhouse(file):
    block = pd.read_csv(f'{CSV_URL}/{file}')
    client = Client('clickhouse', port=9000, user='airflow', password='airflow')
    create_table_query="""
    CREATE TABLE IF NOT EXISTS airflow.cell_towers (
    radio String,
    mcc Int32,
    net Int32,
    area Int32,
    cell Int32,
    unit Int32,
    lon Float64,
    lat Float64,
    range Int32,
    samples Int32,
    changeable Int32,
    created String,
    updated String,
    averageSignal Int32
    ) ENGINE = MergeTree()
    ORDER BY (radio, mcc, net, area, cell)"""
    client.execute(create_table_query)
    create_temporary_table="""
    CREATE TEMPORARY TABLE temp_cell_towers
    ENGINE = Memory
    AS
    SELECT * FROM airflow.cell_towers
    LIMIT 0"""
    client.execute(create_temporary_table)
    client.execute('INSERT INTO temp_cell_towers VALUES', block.values.tolist())
    insert_deduplicate="""
    INSERT INTO airflow.cell_towers
    SELECT distinct tct.* FROM temp_cell_towers tct
    where not exists
    (SELECT tct.* FROM temp_cell_towers tct
    JOIN airflow.cell_towers ct ON
    tct."radio" = ct.radio and tct."mcc" = ct.mcc and tct."net" = ct.net and tct."area" = ct.area and tct."cell" = ct.cell
    and tct."unit" = ct.unit and tct."lon" = ct.lon and tct."lat" = ct.lat and tct."range" = ct.range
    and tct."samples" = ct.samples and tct."changeable" = ct.changeable and tct."created" = ct.created
    and tct."updated" = ct.updated and tct."averageSignal" = ct.averageSignal
    )
    """
    client.execute(insert_deduplicate)
    client.disconnect()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'load_data',
    default_args=default_args,
    description='A DAG to process and load data to ClickHouse',
    schedule_interval=None,
)

download_and_extract_file = PythonOperator(
    task_id='download_and_extract_file',
    python_callable=download_and_extract_file,
    dag=dag,
    provide_context=True,
)


load_data_tasks = []
files = os.listdir(f'{CSV_URL}/')
for file in files:
    if file.startswith("data_block_"):
        task_id = f'load_data_to_clickhouse_{file}'
        load_data_task = PythonOperator(
            task_id=task_id,
            python_callable=load_data_to_clickhouse,
            op_args=[file],
            dag=dag,
        )
        load_data_tasks.append(load_data_task)


download_and_extract_file >> load_data_tasks