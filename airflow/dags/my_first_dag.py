from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
import pymongo
import json

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator


# [END import_module]


# [START instantiate_dag]


def _get_data(ti):
    df = pd.read_csv('/home/nastia12148/PycharmProjects/airflow/airflow/data/tiktok_google_play_reviews.csv')\
        .to_json()
    ti.xcom_push(key="get_data", value=df)


# Get all data from the CSV file
def _preprocessing(ti):
    df = pd.read_json(ti.xcom_pull(key="get_data"))

    # clean all unset rows
    df = df.dropna(how="all")

    # replace "null" values with "-"
    df = df.fillna("-")

    # sort data by created date
    df["at"] = pd.to_datetime(df["at"])
    df = df.sort_values(by="at")

    # remove all unnecessary symbols from the content column (for example, smiles and etc.),
    # leave just text and punctuations
    df["content"] = df["content"].replace(r'[^\w\s\,.?!+:;"*()]', '', regex=True)

    result = df.to_json()
    ti.xcom_push(key="preprocessing", value=result)


def _insert_to_mongo(ti):
    client = pymongo.MongoClient("mongodb://localhost:27017")

    df = pd.read_json(ti.xcom_pull(key="preprocessing"))

    print(df)

    db = client["tik-tok"]
    collection = db["data"]

    df.reset_index(inplace=True)
    data_dict = df.to_dict("records")

    collection.insert_many(data_dict)

with DAG(
        'tutorial',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data
    )

    preprocessing = PythonOperator(
        task_id="preprocessing",
        python_callable=_preprocessing
    )

    insert_to_mongo = PythonOperator(
        task_id="insert_to_mongo",
        python_callable=_insert_to_mongo
    )

    get_data >> preprocessing >> insert_to_mongo
