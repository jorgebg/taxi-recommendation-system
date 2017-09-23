from lib.spark import spark, sc
from lib.plotly import py
from lib.timer import Timer
from lib.process import process
import plotly.graph_objs as go
from pyspark.ml.feature import VectorAssembler
from scipy.spatial import ConvexHull
import pickle
import numpy as np


with Timer('read', 'Reading data'):
    df = df_base = spark.read.csv('data/yellow_tripdata_2016-01.csv', header=True, inferSchema=True)



with Timer('process', 'Cleaning invalid data'):
    df = process(df_base)


from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel



from pyspark.ml.clustering import GaussianMixture


for i in range(6):
    n = 10**i

    for k in [5, 25, 50, 100, 500, 1000]:
        with Timer('limit', 'Limiting data, n={}, k={}'.format(n, k)):
            df_ik = df.limit(n)

        with Timer('clustering', 'n={}, k={}'.format(n, k)):

            gmm = GaussianMixture(k=k)

            va = VectorAssembler(inputCols=["pickup_latitude", "pickup_longitude"], outputCol="features")
            df_t = va.transform(df_ik)

            model = gmm.fit(df_t)

            df_p = model.transform(df_t)

            df_pp = df_p.select('pickup_latitude', 'pickup_longitude', 'prediction').toPandas()
