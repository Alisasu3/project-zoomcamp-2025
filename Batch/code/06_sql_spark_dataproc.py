import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df = spark.read.parquet('gs://dataset-zoomcampproject2025/pq/*')

df = df.withColumnRenamed('price','price_sold')

df.registerTempTable('domain')

df_result = spark.sql("""
SELECT 
    -- Grouping 
    suburb,
    type,
    date_trunc('month', date_sold) AS month_sold,
 
    -- Aggregation 
    AVG(price_sold) AS avg_price_sold,
    count(1) AS quantity_sold,
    AVG(suburb_population) AS suburb_population,
    AVG(suburb_median_income) AS suburb_median_income,
    AVG(cash_rate) AS cash_rate

FROM
    domain
GROUP BY
    1, 2, 3
""")

df_result.coalesce(1).write.parquet('gs://dataset-zoomcampproject2025/report')



