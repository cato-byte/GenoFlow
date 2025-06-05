FROM apache/airflow:2.8.0-python3.10

USER root
RUN apt-get update && apt-get install -y python3-pip

WORKDIR /opt/airflow

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ /opt/airflow/dags
COPY config/ /opt/airflow/config

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

ENTRYPOINT ["airflow"]
CMD ["scheduler"]
