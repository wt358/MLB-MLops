#!/bin/sh

FUNC=$1

git clone https://github.com/wt358/MLB-MLops.git
mkdir py-test
cp -r ./MLB-MLops/airflow/src/* ./py-test/
# cp ./airflow-DAGS/pyfile/*.py ./py-test/


python3 ./py-test/copy_gpu_py.py ${FUNC}

# if [ -e "./disconn.sh" ]; then
#     ./disconn.sh
# fi