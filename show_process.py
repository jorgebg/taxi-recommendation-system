from pyspark.sql.types import FloatType
from lib.process import process
from lib.spark import spark, sc

df = df_base = spark.read.csv('data/yellow_tripdata_2016-01.csv', header=True, inferSchema=True)

process(df)
