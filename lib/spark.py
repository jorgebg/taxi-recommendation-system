from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext.getOrCreate()
sc.addFile('nyc-boundaries.geojson')
spark = SparkSession(sc)
