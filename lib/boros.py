import geojson
from descartes import PolygonPatch
from pyspark import SparkFiles
"""
Example:
    print(boros_data['features'][0]['geometry'])  # There are 5 neighbourhoods
    pp = PolygonPatch(boros_data['features'][0]['geometry'])
    pp.contains_point((-73.98, 40))
"""


with open(SparkFiles.get('nyc-boundaries.geojson')) as f:
    data = geojson.load(f)

polygons = [
    PolygonPatch(feature['geometry'])
    for feature in data['features']
]
