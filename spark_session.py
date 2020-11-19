from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Column

spark = SparkSession.builder.master('local').appName('Check min date').getOrCreate()
df = spark.read.parquet(
    '/Users/gabriele.sabato/PycharmProjects/raw_data/price_elasticity_model_data/part-00022-9c42d1d2-2e54-46e0-92b5-bcc38e88d3ab-c000.snappy.parquet',
    header=True)

df.createOrReplaceTempView('tmp_table')

#df2 = spark.sql("SELECT MIN(update_date) from tmp_table")

df2 = spark.sql("SELECT MAX(item_code) from tmp_table")
print(df2.show())