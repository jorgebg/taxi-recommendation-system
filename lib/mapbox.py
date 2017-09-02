import plotly.graph_objs as go

style = 'mapbox://styles/jorgebg/cj7nkuy5ya5an2ro37rbbzm08'
access_token = 'pk.eyJ1Ijoiam9yZ2ViZyIsImEiOiJjajdrZWN3MmMyMmlqMndtajdlemtsZHU5In0.zFoNWNR56_iODP1vr1mmgw'

layout = go.Layout(
    autosize=True,
    hovermode='closest',
    mapbox=dict(
        accesstoken=access_token,
        center=dict(
            lat=40.716,
            lon=-74.005
        ),
        zoom=9.2,
        style=style
    ),
)
