from lib.plotly import py
import plotly.graph_objs as go


df = df_base = spark.read.csv('data/yellow_tripdata_2016-01.csv', header=True, inferSchema=True)
df = df.sample(False, 0.05)

from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel

df = df.withColumn('hour', udf(lambda d: d.hour, IntegerType())(df.tpep_pickup_datetime))


count = df.orderBy(df.hour).groupBy(df.hour).count()
rows = count.collect()


import numpy as np
x, y = np.transpose(rows)

data = go.Data([go.Scatter(x=x, y=y)])

layout = go.Layout(
    xaxis=dict(
        dtick=1,
    ),
)

fig = dict(data=data, layout=layout)
py.plot(fig, filename='plots/hours.html')
