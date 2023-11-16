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
    games = statsapi.schedule(start_date='07/01/2018',end_date='07/11/2018')
    for x in games:
        print(x['game_id'])
        dict=statsapi.boxscore_data(x['game_id'])
        home_name=x['home_name']
        print(dict)
        print(home_name)
        host = "mongodb://root:coops2022@mongodb-0.mongodb-headless.mongo.svc.cluster.local:27017, mongodb-1.mongodb-headless.mongo.svc.cluster.local:27017"
        client = MongoClient(host)
        db_raw=client['boxscore_raw']
        collection_box=db_raw[f'{home_name}']
        # collection_box.create_index([("gameId",TEXT)],unique=True)
        try:
            result = collection_box.insert_one(dict)
        except Exception as e:
            print("error occured during push",e)
        client.close()
    print("hello pull raw")

def df_mov_avg():
    import matplotlib.pyplot as plt
    host = "mongodb://root:coops2022@mongodb-0.mongodb-headless.mongo.svc.cluster.local:27017, mongodb-1.mongodb-headless.mongo.svc.cluster.local:27017"
    client=MongoClient(host)
    db_raw=client['box_score']
    db_att_y=client['mov_avg_attack_y']
    db_def_y=client['mov_avg_defend_y']
    col_list=db_raw.list_collection_names()
    for coll in col_list:
        print(coll)
        try:
            raw_data=list(db_raw[coll].find().sort("game_date",1))
        except Exception as e:
            print(e)
        raw_df=pd.DataFrame(raw_data).drop(columns=['_id'])

        print(raw_df)
        # print(raw_df[['game_date','game_id','spID']].shift())
        # raw_df['game_id'].plot()
        # plt.show()
        mov_avg_df=raw_df.drop(columns=['game_id','spID'])
        mov_avg_att=mov_avg_df.drop([col for col in raw_df.columns if '_p' in col],axis=1)
        mov_avg_att=mov_avg_att.rolling(5).mean()
        mov_avg_att['game_date']=raw_df['game_date']
        mov_avg_att['game_date']=mov_avg_att['game_date'].shift(-1)
        mov_avg_join=pd.merge(left=mov_avg_att,right=raw_df[['game_date','game_id','spID','runs']],how='inner',on='game_date').dropna()
        print(mov_avg_join)

        db_att_y[coll].create_index([("game_id",ASCENDING)],unique=True)
        db_def_y[coll].create_index([("game_id",ASCENDING)],unique=True)
        try:
            db_att_y[coll].insert_many(mov_avg_join.to_dict('records'),ordered=False)
        except Exception as e:
            print(e)
        print("defend")
        mov_avg_def=mov_avg_df.drop([col for col in mov_avg_df.columns if '_p' not in col],axis=1)
        mov_avg_def['spID']=raw_df['spID']
        list_sp=mov_avg_def['spID'].unique()
        dfs_gropuby_spID=mov_avg_def.groupby(by=['spID'])
        for sp in list_sp:
            temp_df=dfs_gropuby_spID.get_group(sp).rolling(5).mean()
            temp_df['game_date']=raw_df['game_date']
            temp_df['game_date']=temp_df['game_date'].shift(-1)
            
            # print(temp_df)
            mov_avg_def_join=pd.merge(left=temp_df,right=raw_df[['game_date','game_id','runs_p']],how='inner',on='game_date').dropna()
            print(mov_avg_def_join)
            try:
                db_def_y[coll].insert_many(mov_avg_def_join.to_dict('records'),ordered=False)
            except Exception as e:
                print(e)

        mov_avg_df=raw_df.drop(columns=['game_id','spID'])
        mov_avg_def=mov_avg_df.drop([col for col in mov_avg_df.columns if '_p' not in col],axis=1)
        mov_avg_def=mov_avg_def.rolling(5).mean()
        mov_avg_def['game_date']=raw_df['game_date']
        mov_avg_def['game_date']=mov_avg_def['game_date'].shift(-1)
        mov_avg_join=pd.merge(left=mov_avg_def,right=raw_df[['game_date','game_id','spID','runs_p']],how='inner',on='game_date').dropna()
        print(mov_avg_join)   
        try:
            db_def_y[coll].insert_many(mov_avg_join.to_dict('records'),ordered=False)
        except Exception as e:
            print(e)
        # mov_avg_def_df=mov_avg_def.groupby(by=['spID']).rolling(5).mean()
        # # mov_avg_def_df.set_index('game')
        # print(mov_avg_def_df)
        # mov_avg_def_df=mov_avg_def_df.reset_index().set_index('level_1')
        # mov_avg_def_df['game_date']=raw_df['game_date']
        # print(mov_avg_def_df)
        # mov_avg_def_df['game_date']=mov_avg_def_df['game_date'].shift(-1)
        # print(mov_avg_def_df)
        # mov_avg_def_df.to_csv('groupby.csv')
        # mov_avg_def_join=pd.merge(left=mov_avg_def_df,right=raw_df[['game_date','game_id','spID']],how='inner',on='game_date')
    client.close()
    print("hello mov avg")


with DAG(
    dag_id="pull_raw_dag", # DAG의 식별자용 아이디입니다.
    description="pull raw data from local DBs", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(days=1), # 매일 00:00에 실행합니다.
    tags=["my_dags"],
    max_active_runs=3,
    ) as dag:    

    dummy1 = DummyOperator(task_id="start")
    dummy2 = DummyOperator(task_id="pull_finished")

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
    t2 = PythonOperator(
        task_id="ETL_data",
        python_callable=eval("df_mov_avg"),
        # op_kwargs={'brand_name':i},
        # depends_on_past=True,
        depends_on_past=False,
        owner="coops2",
        retries=0,
        retry_delay=timedelta(minutes=1),
    )
    # dummy3 = DummyOperator(task_id="ETL_task")
    dummy4 = DummyOperator(task_id="ETL_finished")
    dummy1 >> t1 >> dummy2 >> t2 >> dummy4