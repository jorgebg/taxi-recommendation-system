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


kmeans = KMeans(k=1000)

traces = []
for i in range(K):
    df_i = df.filter(df.group == i)
    va = VectorAssembler(inputCols=["pickup_latitude", "pickup_longitude"], outputCol="features")
    df_t = va.transform(df_i)

    model = kmeans.fit(df_t)

    wssse = model.computeCost(df_t)

    df_p = model.transform(df_t)

    traces.append((model, df_p, wssse))


from plotly import tools

from lib.plotly import py
import plotly.graph_objs as go

from lib import mapbox


# Cluster centers

layout = dict(
    autosize=True,
    hovermode='closest',
    width=500,
    height=600,
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

cc_scatters = []
for i, (model, df_p, error) in enumerate(traces):
    centers = model.clusterCenters()
    scatter = go.Scattermapbox(
            lat=[c[0] for c in centers],
            lon=[c[1] for c in centers],
            mode='markers',
    )

    cc_scatters.append(scatter)

    title = '%s-%s hours' % (groups[i].start, groups[i].stop)
    fig = dict(data=[scatter], layout=dict(title=title, **layout))

    filename = 'plots/clusters_centers/hour_%s.html' % i
    py.plot(fig, filename=filename)


# Cluster points

layout = dict(
    autosize=True,
    hovermode='closest',
    width=500,
    height=600,
    mapbox=dict(
        accesstoken=mapbox.access_token,
        center=dict(
            lat=40.757,
            lon=-73.981
        ),
        zoom=10.5,
        style='mapbox://styles/jorgebg/cj7nkuy5ya5an2ro37rbbzm08'
    ),
)

cp_scatters = []
for i, (model, df_p, error) in enumerate(traces):
    df_pp = df_p.toPandas()
    scatter = go.Scattermapbox(
            lat=df_pp.pickup_latitude,
            lon=df_pp.pickup_longitude,
            mode='markers',
            marker=go.Marker(
                color=df_pp.prediction,
                colorscale="Jet",
                size=4
            )
    )

    cp_scatters.append(scatter)

    title = '%s-%s hours' % (groups[i].start, groups[i].stop)
    fig = dict(data=[scatter], layout=dict(title=title, **layout))

    filename = 'plots/clusters_points/hour_%s.html' % i
    py.plot(fig)
    py.plot(fig, filename=filename)
