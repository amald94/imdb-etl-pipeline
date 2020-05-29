# imdb-etl-pipeline

In this project movies dataset will be used to to ETL job and is stored on AWS S3. ETL jobs are written in spark and scheduled in airflow to run every 1 hour.

# ETL Flow

* Raw data is stored on s3 bucket.
* Once the data is available on the input directory, a spark job is triggered and processed data is stored in another bucket.
* An ETL job picks up data from processed zone and stages it into the Redshift staging tables.
* Using the Redshift staging tables data is loaded into destination tables.
* ETL job execution is completed once the Data Warehouse is updated.
* Airflow DAG runs the data quality check on all Warehouse tables once the ETL job execution is completed.
* Dag execution completes after these Data Quality check.

# Environment Setup

<b>Hardware suggetion </b> <br>
EMR :

    m5.xlarge
    4 vCore, 16 GiB memory, EBS only storage
    EBS Storage:64 GiB
    
Redshift:   

    Redshift 2 Node cluster with Instance Types `dc2.large`  
    
