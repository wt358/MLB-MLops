FROM apache/airflow:2.2.5-python3.8
USER root
COPY ./requirements.txt ./requirements.txt
RUN apt-get update
RUN apt-get install -y build-essential python3-pip net-tools git
USER airflow
# install your pip package
RUN pip install -r requirements.txt