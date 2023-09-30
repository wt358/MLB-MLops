from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from airflow.utils.trigger_rule import TriggerRule

from sklearn.preprocessing import StandardScaler

import influxdb_client
import csv
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
import pandas as pd
import os

import time

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import statsapi


def pull_raw_data():
    games = statsapi.schedule(start_date='07/01/2018',end_date='07/31/2018')
    for x in games:
        print(x['game_id'])
        dict=statsapi.boxscore_data(x['game_id'])
        home_name=x['home_name']
        print(dict)
        print(home_name)
        host = "mongodb-0.mongodb-headless.mongo.svc.cluster.local:27017"
        client = MongoClient(host)
        db_raw=client['boxscore_raw']
        collection_box=db_raw[f'{home_name}']
        collection_box.create_index([("gameId",ASCENDING)],unique=True)
        try:
            result = collection_box.insert_many(dict,ordered=False)
        except Exception as e:
            print("error occured during push",e)
        client.close()
        break
    print("hello pull raw")

with DAG(
    dag_id="pull_raw_dag", # DAG의 식별자용 아이디입니다.
    description="pull raw data from local DBs", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(days=1), # 매일 00:00에 실행합니다.
    tags=["my_dags"],
    max_active_runs=3,
    ) as dag:    

    dummy1 = DummyOperator(task_id="start")
    dummy2 = DummyOperator(task_id="finished")

    t1 = PythonOperator(
        task_id="pull_raw_data",
        python_callable=eval("pull_raw_data"),
        # op_kwargs={'brand_name':i},
        # depends_on_past=True,
        depends_on_past=False,
        owner="coops2",
        retries=0,
        retry_delay=timedelta(minutes=1),
    )

    dummy1 >> t1 >> dummy2