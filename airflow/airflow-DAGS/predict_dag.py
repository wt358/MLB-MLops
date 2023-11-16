from datetime import timedelta
from datetime import datetime
from kubernetes.client import models as k8s
from airflow.kubernetes.secret import Secret
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from sklearn.preprocessing import  MinMaxScaler


from IPython.display import Image
import matplotlib.pyplot as plt


from bson import ObjectId

# import tensorflow as tf


# tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

import os
import time
from collections import Counter
from pymongo import MongoClient, ASCENDING, DESCENDING

from json import loads
import random as rn

gpu_tag='0.25'

# manual parameters
RANDOM_SEED = 42
TRAINING_SAMPLE = 50000
VALIDATE_SIZE = 0.2

# setting random seeds for libraries to ensure reproducibility
rn.seed(RANDOM_SEED)
# tf.random.set_seed(RANDOM_SEED)

rn.seed(10)

# secret_all = Secret('env', None, 'db-secret-hk8b2hk77m')
secret_all1 = Secret('env', None, 'airflow-cluster-config-envs')
secret_all2 = Secret('env', None, 'airflow-cluster-db-migrations')
secret_all3 = Secret('env', None, 'airflow-cluster-pgbouncer')
secret_all4 = Secret('env', None, 'airflow-cluster-pgbouncer-certs')
secret_all5 = Secret('env', None, 'airflow-cluster-postgresql')
secret_all6 = Secret('env', None, 'airflow-cluster-sync-users')
# secret_all7 = Secret('env', None, 'airflow-cluster-token-7wptr')
secret_all8 = Secret('env', None, 'airflow-cluster-webserver-config')
secret_alla = Secret('env', None, 'airflow-ssh-git-secret')
# secret_allb = Secret('env', None, 'default-token-8d2dz')

# define DAG with 'with' phase
with DAG(
    dag_id="predict_dag", # DAG의 식별자용 아이디입니다.
    description="Model deploy and predict", # DAG에 대해 설명합니다.
    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
    schedule_interval=timedelta(days=1), # 매일 00:00에 실행합니다.
    tags=["predict winner"],
    max_active_runs=3,
    ) as dag:
    
    dummy1 = DummyOperator(task_id="path1_")
    dummy_end = DummyOperator(task_id="end")
    
    predict_winner = KubernetesPodOperator(
        task_id="predict_winner",
        name="predict_winner",
        namespace='airflow',
        image=f'wt358/cuda:{gpu_tag}',
        # image_pull_policy="Always",
        # image_pull_policy="IfNotPresent",
        # image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["sh"],
        arguments=["command.sh", "predict_win"],
        # secrets=[ secret_all1, secret_all2, secret_all3, secret_all4, secret_all5,
        #         secret_all6, secret_all7, secret_all8,  secret_alla, secret_allb],
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        retries=3,
    )
        
    # 테스크 순서를 정합니다.
    # t1 실행 후 t2를 실행합니다.
    dummy1 >> predict_winner >> dummy_end
    
