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


# with Timer('sample', 'Sampling data'):
#     df = df.sample(False, 0.1)


with Timer('process', 'Cleaning invalid data'):
    df = process(df)

from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from pyspark import StorageLevel



from pyspark.ml.clustering import GaussianMixture

gmm = GaussianMixture(k=1000)


result = []
with Timer('clustering', 'Computing clusters'):
    for weekday in range(7):
        for hour in range(24):
            with Timer('clustering', 'Computing clusters for {}x{}'.format(weekday, hour)):
                df_h = df.filter(df.weekday == weekday).filter(df.hour == hour)
                va = VectorAssembler(inputCols=["pickup_latitude", "pickup_longitude"], outputCol="features")
                df_t = va.transform(df_h)

                model = gmm.fit(df_t)

                df_p = model.transform(df_t)

                df_pp = df_p.select('pickup_latitude', 'pickup_longitude', 'prediction', 'score').toPandas()
                df_scores = df_pp.groupby(['prediction'])['score'].sum().sort_values(ascending=False)
                df_points = df_pp.groupby(['prediction'])['pickup_longitude', 'pickup_latitude']
                for cluster, c_score in df_scores.items():
                    points = df_points.get_group(cluster)
                    try:
                        hull = ConvexHull(points.values)
                    except:
                        continue
                    vertices = [points.values[vertice] for vertice in hull.vertices]
                    x, y = np.transpose(vertices)
                    x = np.append(x, x[0])
                    y = np.append(y, y[0])
                    result.append((weekday, hour, c_score, vertices))

with open('result.pickle', 'wb') as f:
    pickle.dump(result, f)
