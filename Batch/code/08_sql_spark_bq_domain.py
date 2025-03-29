import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-676119152103-luutqj10')

df = spark.read.parquet('gs://dataset-zoomcampproject2025/pq/*')

df.write.format('bigquery') \
    .format("com.google.cloud.spark.bigquery.v2.Spark34BigQueryTableProvider") \
    .option('table','projectdataset.domain') \
    .save()
