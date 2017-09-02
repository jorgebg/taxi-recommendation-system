import plotly.offline as py

py._plot = py.plot


def new_plot(*args, **kwargs):
    kwargs['show_link'] = False
    py._plot(*args, **kwargs)


py.plot = new_plot
