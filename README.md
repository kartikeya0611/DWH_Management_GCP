# Near_Real_Time_DWH_Management

# Tech Stack
1. Google Cloud
2. GCP Dataproc
3. Google Cloud Storage (GCS)
4. Airflow (GCP Composer)
5. Hive Operators
6. Hive

# GCP Dataproc
**Cloud CLI command to create and spinup Dataproc cluster: -**

gcloud dataproc clusters create logistic-dwh-cluster --enable-component-gateway --region us-central1 --no-address --master-machine-type n1-standard-2 --master-boot-disk-type pd-balanced --master-boot-disk-size 32 --num-workers 2 --worker-machine-type n2-standard-2 --worker-boot-disk-type pd-balanced --worker-boot-disk-size 32 --image-version 2.2-debian12 --optional-components JUPYTER --project <your_project_id>

**Creating Dataproc cluster using GCP console: -**
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


