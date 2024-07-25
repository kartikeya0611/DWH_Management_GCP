from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator #Used to move file from inbound to completed
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor  #Sensor to check if the inbound file has landed
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitHiveJobOperator #Hive queries to run on Dataproc cluster
from airflow.utils.dates import days_ago
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Define common variables
# CLUSTER_NAME = 'logistic-dwh-cluster'
# REGION = 'us-central1'
# PROJECT_ID = 'natural-quasar-430307-d1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1) #Since retries are set to 0, this parameter is not actively used
}

dag = DAG(
    'hive_load_airflow_dag',
    default_args=default_args,
    description='Load logistics data into Hive on GCP Dataproc',
    schedule_interval=timedelta(days=1),    #This DAG will be schedueled to run once every day
    start_date=days_ago(1),     # Start one day before the current date allowing it to pick up and process the data as soon as it is available.
    tags=['example'],
)

#This one day prior run can be used when the DAG uses current date like {{ ds }} below and we want to process files of 1 day prior
# process_data = BashOperator(
#     task_id='process_data',
#     bash_command='python /scripts/process_sales_data.py /tmp/sales_{{ ds }}.csv',
#     dag=dag
# )

# Sense the new file in GCS with given prefix ----------> TASK1
sense_logistics_file = GCSObjectsWithPrefixExistenceSensor(     #sense_logistics_file is variable name which should be used in execution order
    task_id='sense_logistics_file',         #task_id is displayed in the Airflow web UI - DAG structure, task logs to inspect logs to debug issues
    bucket='logistic_dwh_bucket',
    prefix='inbound/logistics_',
    mode='poke',#Go and check mode -> if file is arrived
    timeout=300,    #After how many seconds, the pooling should be stopped and the task should be marked as failed  -> Usually done by trial n error method 
    poke_interval=30,   #Check after every 30 seconds
    dag=dag
)


# Create Hive Database if not exists ---------> TASK2
create_hive_database = DataprocSubmitHiveJobOperator(
    task_id="create_hive_database",
    query="CREATE DATABASE IF NOT EXISTS logistics_db;",
    cluster_name='logistic-dwh-cluster',
    region='us-central1',
    project_id='natural-quasar-430307-d1',
    dag=dag
)

# Create main(External) Hive table ------------------> TASK3
create_hive_table = DataprocSubmitHiveJobOperator(
    task_id="create_hive_table",
    query="""
        CREATE EXTERNAL TABLE IF NOT EXISTS logistics_db.logistics_data (
            delivery_id INT,
            `date` STRING,
            origin STRING,
            destination STRING,
            vehicle_type STRING,
            delivery_status STRING,
            delivery_time STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION 'gs://logistic_dwh_bucket/inbound/'
        tblproperties('skip.header.line.count'='1');
    """,
    cluster_name='logistic-dwh-cluster',
    region='us-central1',
    project_id='natural-quasar-430307-d1',
    dag=dag
)


# Create partitioned Hive table------------------>TASK4
create_partitioned_table = DataprocSubmitHiveJobOperator(
    task_id="create_partitioned_table",
    query="""
        CREATE TABLE IF NOT EXISTS logistics_db.logistics_data_partitioned (
            delivery_id INT,
            origin STRING,
            destination STRING,
            vehicle_type STRING,
            delivery_status STRING,
            delivery_time STRING
        )
        PARTITIONED BY (`date` STRING)
        STORED AS TEXTFILE;
    """,
    cluster_name='logistic-dwh-cluster',
    region='us-central1',
    project_id='natural-quasar-430307-d1',
    dag=dag
)

# Set Hive properties for dynamic partitioning and load data ---------> TASK 5
set_hive_properties_and_load_partitioned = DataprocSubmitHiveJobOperator(
    task_id="set_hive_properties_and_load_partitioned",
    query=f"""
        SET hive.exec.dynamic.partition = true;
        SET hive.exec.dynamic.partition.mode = nonstrict;

        INSERT INTO logistics_db.logistics_data_partitioned PARTITION(`date`)
        SELECT delivery_id, origin,destination, vehicle_type, delivery_status, delivery_time, `date` FROM logistics_db.logistics_data;
    """,
    cluster_name='logistic-dwh-cluster',
    region='us-central1',
    project_id='natural-quasar-430307-d1',
    dag=dag
)

# Move processed files to archive bucket ------------> Finally move
archive_processed_file = BashOperator(
    task_id='archive_processed_file',
    bash_command=f"gsutil -m mv gs://logistic_dwh_bucket/inbound/logistics_*.csv gs://logistic_dwh_bucket/archive/",
#   -m: This flag enables multi-threading, allowing gsutil to perform multiple operations in parallel -> speed up the process when dealing with many files.
#   use of f(f-string) is to allow embedded variable or expression within the string following it:-
#   bucket_name = "logistic-dwh-cluster"
#   bash_command = f"gsutil -m mv gs://{bucket_name}/inbound/logistics_*.csv gs://{bucket_name}/completed/"
#   current_time = f"The current time is {datetime.datetime.now()}"
#   message = f"The operation was {'successful' if status == 'success' else 'unsuccessful'}"
    dag=dag
)

# trigger_same_dag = TriggerDagRunOperator(
#     task_id = 'trigger_same_dag',
#     trigger_dag_id = 'hive_load_airflow_dag',
#     dag = dag
# )

sense_logistics_file >> create_hive_database >> create_hive_table >> create_partitioned_table >> set_hive_properties_and_load_partitioned >> archive_processed_file

