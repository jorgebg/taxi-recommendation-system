from lib import boros
from lib import mapbox
from lib.plotly import py
import plotly.graph_objs as go
import matplotlib

cmap = matplotlib.cm.get_cmap('rainbow', len(boros.polygons))

data = go.Data([go.Scattermapbox()])


layout = mapbox.layout.copy()

layout['mapbox']['layers'] = [
    dict(
        sourcetype='geojson',
        source=feature,
        type='fill',
        color='rgb' + str(cmap(i, bytes=True)[:3])
    )
    for i, feature in enumerate(boros.data['features'])
]


fig = dict(data=data, layout=layout)
py.plot(fig)
