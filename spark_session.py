from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL")\
    .config ()\
    .getOrCreate()

spark.builder

pfile = spark.read.parquet('/Users/gabriele.sabato/PycharmProjects/price_elasticity_aggr/filtered.snappy.parquet')

pfile.show()