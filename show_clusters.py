from lib.spark import spark, sc
from lib.plotly import py
from lib.timer import Timer
import plotly.graph_objs as go
from pyspark.ml.feature import VectorAssembler
from scipy.spatial import ConvexHull


with Timer('read', 'Reading data'):
    df = df_base = spark.read.csv('data/yellow_tripdata_2016-01.csv', header=True, inferSchema=True)


with Timer('sample', 'Sampling data'):
    df = df.sample(False, 0.005)

from lib.process import process

with Timer('process', 'Cleaning invalid data'):
    df = process(df)

from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel

K = 6
N = 24//K
groups = {i: range(i*N, i*N+N) for i in range(K)}


@udf(returnType=IntegerType())
def get_group(d):
    return d.hour//N


with Timer('group', 'Grouping by hours'):
    df = df.withColumn('group', get_group(df.tpep_pickup_datetime))




from pyspark.ml.clustering import GaussianMixture

gmm = GaussianMixture(k=1000)

traces = []

with Timer('clustering', 'Computing clusters'):
    for i in range(K):
        df_i = df.filter(df.group == i)
        va = VectorAssembler(inputCols=["pickup_latitude", "pickup_longitude"], outputCol="features")
        df_t = va.transform(df_i)

        model = gmm.fit(df_t)

        df_p = model.transform(df_t)
        # rows = df_p.collect()

        traces.append((model, df_p))



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

weekday = 0
result = []
for i, (model, df_p) in enumerate(traces):
    scatters = []
    df_p = df_p.withColumn('score', score(df_p.trip_time, df_p.trip_distance, df_p.passenger_count))
    df_pp = df_p.select('pickup_latitude', 'pickup_longitude', 'prediction', 'score').toPandas()
    df_scores = df_pp.groupby(['prediction'])['score'].sum().sort_values(ascending=False)
    df_points = df_pp.groupby(['prediction'])['pickup_longitude', 'pickup_latitude']
    for cluster, c_score in df_scores.items():
        points = df_points.get_group(cluster)
        if len(points) < 3:
            continue
        hull = ConvexHull(points.values)
        vertices = [points.values[vertice] for vertice in hull.vertices]
        x, y = np.transpose(vertices)
        x = np.append(x, x[0])
        y = np.append(y, y[0])
        scatters.append(go.Scattermapbox(lon=x, lat=y, fill='toself', name=round(c_score, 2)))
        result.append((weekday, i, c_score, vertices))
    filename = 'plots/clusters_points/hour_%s_clusters.html' % i
    py.plot(dict(data=scatters[:10], layout=layout), filename=filename)



with Timer('hull', 'Scoring clusters'):

    for i, (model, df_p) in enumerate(traces):

        df_scores = (
            df_p.groupBy(df_p.prediction)
            .agg({'score': 'sum'})
            .orderBy('sum(score)').toPandas()
        )

        for cluster in df_scores:
            points = (
                df_p.filter(df_p.prediction==cluster.prediction)
                .select('pickup_longitude', 'pickup_latitude')
            )
            hull = ConvexHull(df_scores)
            x, y = hull.simplices.transpose()
