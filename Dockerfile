FROM apache/airflow:2.10.3
ADD requirements.txt .
RUN pip install -r requirements.txt