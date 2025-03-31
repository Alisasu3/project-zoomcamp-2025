This file outlines some details of how I ingest data using a Batch pipeline. Some ingestion steps may appear repetitive with Terraform and Kestra steps. This repetition is intentional, as it allows me to apply and review various technologies I learned during the De-Zoomcamp course.

## Install Java:

    reference: https://jdk.java.net/archive/

    mkdir spark
    cd spark
    wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz

Unpack it:

    tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz

Define JAVA_HOME and add it to PATH:

    export JAVA_HOME="${HOME}/spark/jdk-11.0.1"
    export PATH="${JAVA_HOME}/bin:${PATH}"

Checking:

    run: which java
    you will see: /home/su/spark/jdk-11.0.1/bin/java
    run: java --version
    you will see: openjdk 11.0.1 2018-10-16

Remove the archive:

    rm openjdk-11.0.2_linux-x64_bin.tar.gz 

## Install Spark:

    reference: https://spark.apache.org/downloads.html
    download Apache Spark version:
    Choose Spark release: 3.5.5 (Feb 27 2025)
    Choose package type: Pre-built for Apache Hadoop 3.3 and later (Scala 2.13)

    wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz

Unpack it:

    tar xzfv spark-3.3.2-bin-hadoop3.tgz

Remove the archive:

    rm spark-3.3.2-bin-hadoop3.tgz

Add it to PATH:

    export SPARK_HOME="${HOME}/spark/spark-3.5.5-bin-hadoop3-scala2.13"
    export PATH="${SPARK_HOME}/bin:${PATH}"

Test Spark:

    run spark-shell

## Use Pyspark in Jupyter notebook for data transformation and ingestion

    DAG Visualization
    ![Spark UI](https://github.com/Alisasu3/project-zoomcamp-2025/blob/main/Batch/code/image-4.png)
    ![Spark UI](https://github.com/Alisasu3/project-zoomcamp-2025/blob/main/Batch/code/image-3.png)

## Download google cloud connector:

    reference: https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#connector-setup-on-non-dataproc-clusters

    mkdir lib
    cd lib

## Create a Local Spark Cluster

    echo $SPARK_HOME
    ./sbin/start-master.sh
    forward 8081
    copy master url
    ./sbin/start-worker.sh spark://de-zoomcamp.australia-southeast1-b.c.western-triode-447803-t3.internal:7077
    ![Spark Master UI](https://github.com/Alisasu3/project-zoomcamp-2025/blob/main/Batch/code/image-1.png)
    Should see Worker Id and Application ID in Spark Master

## Convert to python script and execute

    jupyter nbconvert --to=script 04_sql_spark_url.ipynb
    export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
    export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
    python 04_sql_spark_url.py
    localhost:8081: kill extra Application ID to release client resources

## Stop the workers and the master

    Go to spark folder
    Echo $SPARK_HOME
    ./sbin/stop-worker.sh
    ./sbin/stop-master.sh

## Create a Dataproc Cluster in google cloud

    ![Cluster Creation](https://github.com/Alisasu3/project-zoomcamp-2025/blob/main/Batch/code/image-2.png)

## Upload data to bucket

    Upload python_06 script to dataproc:
    gsutil -m cp -r 06_sql_spark_dataproc.py gs://dataset-zoomcampproject2025/code/06_sql_spark_dataproc.py
    Submit job in google cloud cluster

## Upload data to BigQuery

    Upload python_07 script to dataproc:
    gsutil cp 07_sql_spark_big_query.py gs://dataset-zoomcampproject2025/code/07_sql_spark_big_query.py

## Connect Dataproc to BigQuery using Jar

    gcloud dataproc jobs submit pyspark \
    --cluster=project-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.41.1.jar \
    --properties='spark.driver.memory=4g,spark.executor.memory=4g' \
        gs://dataset-zoomcampproject2025/code/07_sql_spark_big_query.py

## Repeat steps to upload another table to BigQuery

    gsutil cp 08_sql_spark_bq_domain.py gs://dataset- zoomcampproject2025/code/08_sql_spark_bq_domain.py

    gcloud dataproc jobs submit pyspark \
    --cluster=project-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.41.1.jar \
    --properties='spark.driver.memory=4g,spark.executor.memory=4g' \
        gs://dataset-zoomcampproject2025/code/08_sql_spark_bq_domain.py


    





