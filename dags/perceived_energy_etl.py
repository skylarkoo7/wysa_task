from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import pandas as pd
import json
import logging
import os
import re
from datetime import timedelta


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
}

# Initialize the DAG
dag = DAG(
    'perceived_energy_etl',
    default_args=default_args,
    description='ETL pipeline for Perceived Energy Score',
    schedule_interval='@daily',
)

MONGO_ATLAS_URI = "mongodb+srv://harsolaneel1:MuVCnQQ1Yx7pAvsP@wysadata.h1mrj.mongodb.net/test?retryWrites=true&w=majority&appName=wysadata"

def check_mongo_connection():
    try:
        client = MongoClient(MONGO_ATLAS_URI)
        client.admin.command('ping')
        logging.info("MongoDB Atlas connection established successfully.")
        return True
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB Atlas: {e}")
        raise e

def format_hours(hours):
    """Formats a decimal hour value to HH:MM:SS."""
    hours_int = int(hours)
    minutes = int((hours - hours_int) * 60)
    seconds = int((((hours - hours_int) * 60) - minutes) * 60)
    return f"{hours_int:02}:{minutes:02}:{seconds:02}"

def preprocess_time_string(time_str):
    """Ensures there is a space before AM/PM in the time string."""
    return re.sub(r"(?<=\d)(AM|PM|am|pm)$", r" \1", time_str)


def parse_time(time_str):
    """Parses time with or without seconds."""
    time_str = preprocess_time_string(time_str)
    try:
        return datetime.strptime(time_str, "%I:%M:%S %p")
    except ValueError:
        return datetime.strptime(time_str, "%I:%M %p")
    
def calculate_duration(start_time, end_time):
    """Calculates the duration in hours between start and end times."""
    start = parse_time(start_time)
    end = parse_time(end_time)
    duration = (end - start).total_seconds() / 3600
    return round(duration, 2)

def calculate_hours_in_bed(duration_in_bed):
    """Calculates hours in bed from 'DURATION IN BED' format, accounting for overnight durations."""
    
    times = duration_in_bed.split(" - ")
    if len(times) == 2:
        start = parse_time(times[0])
        end = parse_time(times[1])
        
        if end < start:
            end += timedelta(days=1)
        
        duration = (end - start).total_seconds() / 3600
        logging.info(f'duration {duration}')
        return duration
    else:
        return None
    

def extract_data():
    client = MongoClient(MONGO_ATLAS_URI)
    db = client["test"]

    collections = ["Mood", "Activity", "Sleep"]
    for collection_name in collections:
        collection = db[collection_name]
        document_count = collection.count_documents({})
        logging.info(f"Number of documents in {collection_name} collection: {document_count}")

    mood_data = list(db["Mood"].find())
    logging.info(f"Extracted mood data: {mood_data}")

    activity_data = pd.read_csv('/opt/airflow/data/activity_data.csv').to_dict(orient='records')
    sleep_data = pd.read_csv('/opt/airflow/data/sleep_data.csv').to_dict(orient='records')
    
    for activity in activity_data:
        if activity.get("Duration") == "?":
            duration_hours = calculate_duration(activity["StartTime"], activity["EndTime"])
            activity["Duration"] = format_hours(duration_hours) if duration_hours else "00:00:00"

    for sleep in sleep_data:
        if sleep.get("HOURS IN BED") == "?":
            hours_in_bed = calculate_hours_in_bed(sleep["DURATION IN BED"])
            sleep["HOURS IN BED"] = format_hours(hours_in_bed) if hours_in_bed else None
    
    logging.info(f"Extracted activity data: {activity_data}")
    return {"mood_data": mood_data, "activity_data": activity_data, "sleep_data": sleep_data}

def transform_data(**kwargs):
    client = MongoClient(MONGO_ATLAS_URI)
    db = client["test"]

    pipeline = [
        {
            "$lookup": {
                "from": "Mood",
                "localField": "_id",
                "foreignField": "user",
                "as": "mood_data"
            }
        },
        {
            "$lookup": {
                "from": "Activity",
                "localField": "_id",
                "foreignField": "User",
                "as": "activity_data"
            }
        },
        {
            "$lookup": {
                "from": "Sleep",
                "localField": "_id",
                "foreignField": "USER",
                "as": "sleep_data"
            }
        },
        {
            "$unwind": "$mood_data"
        },
        {
            "$project": {
                "user": "$_id",
                "date": { "$dateToString": { "format": "%Y-%m-%dT%H:%M:%S.%LZ", "date": "$mood_data.createdAt" } },
                "mood_score": "$mood_data.value",
                "activity": {
                    "$map": {
                        "input": "$activity_data",
                        "as": "activity",
                        "in": {
                            "activity": "$$activity.Activity",
                            "steps": "$$activity.Steps",
                            "distance": "$$activity.Distance",
                            "duration": "$$activity.Duration",
                            "endtime" : "$$activity.EndTime",
                            "starttime" : "$$activity.StartTime",
                        }
                    }
                },
                "sleep": {
                    "$map": {
                        "input": "$sleep_data",
                        "as": "sleep_entry",
                        "in": {
                            "sleep_score": "$$sleep_entry.SLEEP SCORE",
                            "hours_of_sleep": {
                                "$concat": [
                                    { "$toString": { "$floor": { "$convert": { "input": { "$arrayElemAt": [{ "$split": ["$$sleep_entry.HOURS OF SLEEP", ":"] }, 0] }, "to": "double", "onError": 0, "onNull": 0 } } } }, ":",
                                    { "$toString": { "$floor": { "$convert": { "input": { "$arrayElemAt": [{ "$split": ["$$sleep_entry.HOURS OF SLEEP", ":"] }, 1] }, "to": "double", "onError": 0, "onNull": 0 } } } }, ":",
                                    "00"
                                ]
                            },
                            "hours_in_bed": {
                                        "$concat": [
                                            # { "$toString": { "$floor": calculate_hours_in_bed("$$sleep_entry.DURATION IN BED") } }, ":",
                                            # { "$toString": { "$mod": [ { "$multiply": [ calculate_hours_in_bed("$$sleep_entry.DURATION IN BED"), 60 ] }, 60 ] } }, ":",
                                            "00"    
                                        ]
                            },
                            "duration_in_bed": "$$sleep_entry.DURATION IN BED",
                        }
                    }
                }
            }
        }
    ]

    
    try:
        results = list(db.User.aggregate(pipeline))
        
        logging.info(f"Transformed data: {results}")
        
        for user in results:
            user_id = user['_id']
            print(f"User: {user_id}")
            for entry in user['sleep']:
                duration_in_bed = entry['duration_in_bed']
                duration_hours = calculate_hours_in_bed(duration_in_bed)
                print(f"Duration in bed for '{duration_in_bed}': {duration_hours:.2f} hours")
                entry['hours_in_bed'] = format_hours(duration_hours) if duration_hours else "00:00:00"
                del entry['duration_in_bed'] 
                
        for user in results:
            user_id = user['_id']
            print(f"User: {user_id}")
            for entry in user['activity']:
                starttime = entry['starttime']
                endtime = entry['endtime']
                durationTime = calculate_duration(starttime,endtime)
                print(f"Duration in activity {durationTime} , {starttime} ,{endtime}")
                entry['duration'] = format_hours(durationTime) if durationTime else "00:00:00"
                del entry['starttime']
                del entry['endtime']
        
        logging.info(f'final output {results}')
   
    except Exception as e:
        logging.error(f"Error in aggregation pipeline: {e}")

    kwargs['ti'].xcom_push(key='transformed_data', value=results)


def load_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')
    output_path = "/opt/airflow/data/perceived_energy_score.json"
    
    with open(output_path, "w") as f:
        json.dump(transformed_data, f, default=str, indent=4)

# Define PythonOperator tasks for each function
check_mongo_task = PythonOperator(
    task_id='check_mongo_connection',
    python_callable=check_mongo_connection,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
check_mongo_task >> extract_task >> transform_task >> load_task
