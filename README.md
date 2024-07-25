# Near_Real_Time_DWH_Management

# Tech Stack
1. Google Cloud
2. GCP Dataproc
3. Google Cloud Storage (GCS)
4. Airflow (GCP Composer)
5. Hive Operators
6. Hive



# GCP Dataproc
**Gcloud CLI command to create and spinup Dataproc cluster: -**

gcloud dataproc clusters create logistic-dwh-cluster --enable-component-gateway --region us-central1 --no-address --master-machine-type n1-standard-2 --master-boot-disk-type pd-balanced --master-boot-disk-size 32 --num-workers 2 --worker-machine-type n2-standard-2 --worker-boot-disk-type pd-balanced --worker-boot-disk-size 32 --image-version 2.2-debian12 --optional-components JUPYTER --project <your_project_id>

**Alternatively, Creating Dataproc cluster using GCP console: -**
![image](https://github.com/user-attachments/assets/faa6dd71-47a1-41e6-a0c7-fd8da9919313)


Manager Node configurations: -

![image](https://github.com/user-attachments/assets/43156d6a-aa93-4767-a2a6-d054b1543411)


Worker Node configurations: -

![image](https://github.com/user-attachments/assets/53b78dfa-2101-48fa-ac0b-ca1259d4429f)


No Secondary worker nodes used: -

![image](https://github.com/user-attachments/assets/d59a84a4-e79c-4931-9eac-7b18dd933659)

Note - Set Private Google Access to ‘On’ for VPC network in the reion of your Dataproc cluster



# Google Cloud Storage(GCS)

**->Create bucket logistic_dwh_bucket: -**

gcloud storage buckets create gs://logistic_dwh_bucket --location us-central1

**->Create inboud and archive directories: -**

gsutil cp /dev/null gs://logistic_dwh_bucket/inbound/

gsutil cp /dev/null gs://logistic_dwh_bucket/archive/

Directory Structure: -

![image](https://github.com/user-attachments/assets/5ce501b9-86a2-4bb4-9110-0339ee0727d8)

Everyday a file with format logistics_YYYY_MM_DD.csv will arrive in inbound path.



# Composer

**Gcloud CLI command to create and spinup Composer: -** 
gcloud composer environments create logistic-airflow --location us-central1 --image-version composer-3-airflow-2.7.3

**Alternatively, Creating Composer cluster using GCP console: -**

![image](https://github.com/user-attachments/assets/840cffb7-6731-44b3-a047-3fad1319f317)

![image](https://github.com/user-attachments/assets/b94556fb-2818-4b39-86f5-bcb7e44afa5d)


Upload the **hive_load_airflow.py** file in the DAG folder of logistic-airflow composer: -

![image](https://github.com/user-attachments/assets/c60a8e99-bced-43f6-8575-bde55588f6bc)



Now, the DAG should appear in the Airflow console: -

![image](https://github.com/user-attachments/assets/a1063d63-2156-45de-bfd9-7694b16769eb)

It should be already running with 1st task : **sense_logistics_file**
The reason why it's already runnin is because we've kept start_date=days_ago(1) for our DAG 

![image](https://github.com/user-attachments/assets/2a41c3c0-d8fb-4ac4-83fc-9bc36781da95)


Now upload the logistics_2023_09_01.csv file in the inbound directory.
Ideally, the DAG should be complete all the tasks wihout failure: -

![image](https://github.com/user-attachments/assets/d1db1d8e-2639-4df6-a8ab-5a126d59d117)



# Validations

Validate the data in hive table, using SSH on Master node VM of Dataproc cluster: -

![image](https://github.com/user-attachments/assets/7b12b773-9d6c-4865-9285-d8d33749da19)

![image](https://github.com/user-attachments/assets/d693b649-9371-404f-b4d4-bf4c03d1bf8c)


Finally, check if the file was moved from inbound to archive path in GCS: -

![image](https://github.com/user-attachments/assets/8e85f4dc-a5b1-492f-9472-f4474b5b28d9)
