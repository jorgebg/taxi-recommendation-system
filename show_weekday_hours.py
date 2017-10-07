from lib.spark import spark
from lib.plotly import py
import plotly.graph_objs as go
import calendar
import scipy


day_names = list(calendar.day_name)


df = df_base = spark.read.csv('data/yellow_tripdata_2016-01.csv', header=True, inferSchema=True)
df = df.sample(False, 0.05)

from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel

df = df.withColumn('weekday', udf(lambda d: d.weekday(), IntegerType())(df.tpep_pickup_datetime))
df = df.withColumn('hour', udf(lambda d: d.hour, IntegerType())(df.tpep_pickup_datetime))



count = df.orderBy(df.weekday, df.hour).groupBy(df.weekday, df.hour).count()
rows = count.collect()

from itertools import groupby
import numpy as np

weekday_rows = [
    [row[1:] for row in group]  # Remove weekday (index 0)
    for k, group in groupby(rows, lambda row: row.weekday)
]

data = go.Data()
for i, row in enumerate(weekday_rows):
    x, y = np.transpose(row)
    data.append(go.Scatter(x=x, y=y, name=day_names[i]))

layout = go.Layout(
    xaxis=dict(
        dtick=1,
    ),
)

fig = dict(data=data, layout=layout)
py.plot(fig, filename='plots/weekdays_hours.html')

# ---
from pandas import DataFrame
import plotly.figure_factory as ff


corr_matrix = [['', *calendar.day_name]] + [
    [calendar.day_name[day_b]] + [float(DataFrame(data[day_a]['y']).corrwith(DataFrame(data[day_b]['y'])).round(2)) for day_a in range(7)]
    for day_b in range(7)
]



# corr_matrix = DataFrame([r['y'] for r in data]).transpose().corr()
table = ff.create_table(corr_matrix, index=True)
py.plot(table, filename="plots/weekdays_hours_table.py")
