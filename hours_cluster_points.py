from lib.plotly import py
import plotly.graph_objs as go


df = df_base = spark.read.csv('data/yellow_tripdata_2016-01.csv', header=True, inferSchema=True)
df = df.sample(False, 0.005)

from lib.process import process
df = process(df)

from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel

K = 6
N = 24//K
groups = {i: range(i*N, i*N+N) for i in range(K)}

df = df.withColumn('group', udf(lambda d: d.hour//N, IntegerType())(df.tpep_pickup_datetime))


kmeans = KMeans(k=30)

traces = []
for i in range(K):
    df_i = df.filter(df.group == i)
    va = VectorAssembler(inputCols=["pickup_latitude", "pickup_longitude"], outputCol="features")
    df_t = va.transform(df_i)

    model = kmeans.fit(df_t)

    wssse = model.computeCost(df_t)
    # error.append(wssse)

    df_p = model.transform(df_t)
    # rows = df_p.collect()

    traces.append((model, df_p, wssse))
    # Shows the result.
    # centers = model.clusterCenters()
    # print("Cluster Centers: ")
    # for center in centers:
    #     print(center)




from plotly import tools

from lib.plotly import py
import plotly.graph_objs as go

from lib import mapbox


layout = dict(
    autosize=True,
    hovermode='closest',
    # width=500,
    # height=600,
    mapbox=dict(
        accesstoken=mapbox.access_token,
        center=dict(
            lat=40.716,
            lon=-74.005
        ),
        zoom=9.2,
        style='mapbox://styles/jorgebg/cj7nkuy5ya5an2ro37rbbzm08'
    ),
)

data = []
for i, (model, df_p, error) in enumerate(traces):
    df_pp = df_p.toPandas()
    scatter = go.Scattermapbox(
            lat=df_pp.pickup_latitude,
            lon=df_pp.pickup_longitude,
            mode='markers',
            marker=go.Marker(
                color=df_pp.prediction,
                colorscale="Jet",
                size=3
            )
    )

    data.append(scatter)

    title = '%s-%s hours' % (groups[i].start, groups[i].stop)
    fig = dict(data=[scatter], layout=dict(title=title, **layout))

    filename = 'plots/clusters_points/hour_%s.html' % i
    py.plot(fig, filename=filename)
    # break
