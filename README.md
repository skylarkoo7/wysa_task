git clone https://github.com/skylarkoo7/wysa_task
cd wysa_task

Install the following dependencies in the virtual environment. 
Create virtual environment : python3 -m venv .venv

Activate the virtual environment : venv/scripts/activate

pip install -r requirements.txt


Airflow-Docker Setup ( Reference : https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html ) : 
cmd : docker-compose up airflow-init (for database migrations)
docker-compose up 
open the localhost:8080 port by username : airflow , password : airflow

DAG Structure
The DAG (perceived_energy_etl) is composed of the following tasks:

-> check_mongo_connection: Verifies the connection to MongoDB Atlas.
-> extract_data: Extracts mood data from MongoDB and loads activity and sleep data from CSV files.
-> transform_data: Processes the data, calculates metrics, and applies transformations using MongoDB aggregation pipelines.
-> load_data: Saves the transformed data as a JSON file in /opt/airflow/data/perceived_energy_score.json.