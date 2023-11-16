from datetime import datetime, timedelta

from kubernetes.client import models as k8s
from airflow.models import DAG
from airflow.models.variable import Variable

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.kubernetes.secret import Secret
from airflow.kubernetes.pod import Resources
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


gpu_tag='0.22'
tad_tag='0.01'
vpn_tag='0.08'

dag_id = 'learning-dag'

task_default_args = {
        'owner': 'coops2',
        'retries': 0,
        'retry_delay': timedelta(minutes=1),
        'depends_on_past': False,
        #'execution_timeout': timedelta(minutes=5)
}

dag = DAG(
        dag_id=dag_id,
        description='kubernetes pod operator',
        start_date=days_ago(1),
        default_args=task_default_args,
        schedule_interval=timedelta(days=7),
        max_active_runs=3,
        catchup=True,
        # catchup=False,
)


configmaps = [
        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='airflow-cluster-pod-template'))
        ]

# secret_all = Secret('env', None, 'db-secret-hk8b2hk77m')
# secret_all1 = Secret('env', None, 'airflow-cluster-config-envs')
# secret_all2 = Secret('env', None, 'airflow-cluster-db-migrations')
# secret_all3 = Secret('env', None, 'airflow-cluster-pgbouncer')
# secret_all4 = Secret('env', None, 'airflow-cluster-pgbouncer-certs')
# secret_all5 = Secret('env', None, 'airflow-cluster-postgresql')
# secret_all6 = Secret('env', None, 'airflow-cluster-sync-users')
# secret_all7 = Secret('env', None, 'airflow-cluster-token-7wptr')
# secret_all8 = Secret('env', None, 'airflow-cluster-webserver-config')
# secret_alla = Secret('env', None, 'airflow-ssh-git-secret')
# secret_allb = Secret('env', None, 'default-token-8d2dz')

start = DummyOperator(task_id=f"start", dag=dag)

run_score_model= KubernetesPodOperator(
        task_id="score_model_pod_operator_",
        name="score_model",
        namespace='airflow',
        image=f'wt358/cuda:{gpu_tag}',
        # image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["sh" ],
        arguments=["command.sh", "score_model"],
        # secrets=[secret_all1 ,secret_all2 ,secret_all3, secret_all4, secret_all5, secret_all6, secret_all7, secret_all8,  secret_alla, secret_allb],
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        retries=3,
        )
run_loss_model= KubernetesPodOperator(
        task_id="loss_model_pod_operator_",
        name="loss_model",
        namespace='airflow',
        image=f'wt358/cuda:{gpu_tag}',
        # image_pull_secrets=[k8s.V1LocalObjectReference('regcred')],
        cmds=["sh" ],
        arguments=["command.sh", "loss_model"],
        # secrets=[secret_all1 ,secret_all2 ,secret_all3, secret_all4, secret_all5, secret_all6, secret_all7, secret_all8,  secret_alla, secret_allb ],
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=600,
        retries=3,
        )

after_ml = DummyOperator(task_id="ML_fin_", dag=dag)

start >> [run_score_model,run_loss_model] >>after_ml
