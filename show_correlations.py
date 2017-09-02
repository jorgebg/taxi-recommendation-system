from lib.spark import spark
from lib.process import process
from scipy.stats import pearsonr, linregress
from plotly import tools
import plotly.figure_factory as ff
import plotly.graph_objs as go
from lib.plotly import py
from pyspark.sql.types import IntegerType

import numpy as np
import sklearn


df = df_base = spark.read.csv('data/yellow_tripdata_2016-01.csv', header=True, inferSchema=True)
df = df_base.sample(False, 0.01)

df = process(df)

# ---

subplots = (
    ('Time (minutes)', 'Trip Time', 'trip_time'),
    ('Distance (miles)', 'Trip Distance', 'trip_distance'),
    ('Passenger Count', 'Passenger count', 'passenger_count'),
)
xaxis = (
    {'title': 'Time (minutes)'},
    {'title': 'Distance (miles)'},
    {'title': 'Passenger Count', 'dtick': 1},
)

# ---

traces = []
regression_traces = []
subplot_titles = []

for title, subplot_title, column in subplots:
    df_amount = df.select(df[column], df.total_amount).orderBy('trip_time').toPandas()

    x = df_amount[df_amount.columns[0]]
    y = df_amount[df_amount.columns[1]]

    trace = go.Scatter(
        x=x,
        y=y,
        mode='markers',
        marker=go.Marker(
            size=3
        )
    )

    slope, intercept, r_value, p_value, std_err = linregress(x, y)
    line = slope*x+intercept
    regression_trace = go.Scatter(x=x, y=line)

    traces.append(trace)
    regression_traces.append(regression_trace)

    b = round(slope, 2)
    r = round(r_value, 2)
    subplot_titles.append(subplot_title + ", β={}, r={}".format(b, r))


fig = tools.make_subplots(
    rows=1, cols=len(traces), subplot_titles=subplot_titles, shared_yaxes=True)
fig['layout'].update(dict(
    yaxis=dict(
        title='Total Amount ($)',
        tick0=0,
    ),
    **{'xaxis%s' % (i+1): axis for i, axis in enumerate(xaxis)}
))


for i, trace in enumerate(traces):
    fig.append_trace(trace, 1, i+1)
    fig.append_trace(regression_traces[i], 1, i+1)


py.plot(fig, filename='plots/correlation.html')


corr_matrix = [[''] + [title for _, title, _ in subplots]] + [
    [subplots[i][1]] + [round(pearsonr(traces[i]['x'], traces[j]['x'])[0], 2)
                        for j in range(len(subplots))]
    for i in range(len(subplots))
]
table = ff.create_table(corr_matrix, index=True)
py.plot(table, filename='plots/correlation_table.html')


# ---

traces = []
regression_traces = []
subplot_titles = []

for title, subplot_title, column in subplots:
    df_amount = df.select(df[column], df.total_amount).orderBy('trip_time').toPandas()

    x = df_amount[df_amount.columns[0]]
    y = df_amount[df_amount.columns[1]]/x

    trace = go.Scatter(
        x=x,
        y=y,
        mode='markers',
        marker=go.Marker(
            size=3
        )
    )

    slope, intercept, r_value, p_value, std_err = linregress(x, y)
    line = slope*x+intercept
    regression_trace = go.Scatter(x=x, y=line)

    traces.append(trace)
    regression_traces.append(regression_trace)

    b = round(slope, 2)
    r = round(r_value, 2)
    subplot_titles.append(subplot_title + ", β={}, r={}".format(b, r))


fig = tools.make_subplots(
    rows=1, cols=len(traces), subplot_titles=subplot_titles, shared_yaxes=True)
fig['layout'].update(dict(
    **{'yaxis%s' % (i+1): {'tick0': 0, 'title': 'Total Amount ($) / ' + xaxis[i]['title']} for i, axis in enumerate(xaxis)},
    **{'xaxis%s' % (i+1): axis for i, axis in enumerate(xaxis)}
))


for i, trace in enumerate(traces):
    fig.append_trace(trace, 1, i+1)
    fig.append_trace(regression_traces[i], 1, i+1)


py.plot(fig, filename='plots/correlation_ratio.html')


corr_matrix = [[''] + [title for _, title, _ in subplots]] + [
    [subplots[i][1]] + [round(pearsonr(traces[i]['x'], traces[j]['x'])[0], 2)
                        for j in range(len(subplots))]
    for i in range(len(subplots))
]
table = ff.create_table(corr_matrix, index=True)
py.plot(table, filename='plots/correlation_ratio_table.html')

# ---


traces = []
regression_traces = []
subplot_titles = []
regression_opts = []

for title, subplot_title, column in subplots:
    df_amount_agg = (
        df.groupBy(df[column].cast(IntegerType()).alias('x'))
        .agg({'total_amount': 'sum', '*': 'count'})
        .orderBy('x').toPandas()
    )

    x = df_amount_agg[df_amount_agg.columns[0]]
    y = df_amount_agg[df_amount_agg.columns[1]]

    trace = go.Scatter(
        x=x,
        y=y,
        mode='markers',
        marker=go.Marker(
            size=3
        )
    )

    slope, intercept, r_value, p_value, std_err = linregress(x, y)
    line = slope*x+intercept
    regression_trace = go.Scatter(x=x, y=line)

    traces.append(trace)
    regression_traces.append(regression_trace)

    b = round(slope, 2)
    r = round(r_value, 2)
    subplot_titles.append(subplot_title + ", β={}, r={}".format(b, r))

    # Regression
    # all_errors = []
    # best_error = None
    # all_opts = []
    # best_opts = None
    # best_p_y = None
    # for n in range(1, 10):
    #     opts = np.polyfit(x, y, n)
    #     all_opts.append(opts)
    #     p_y = 0
    #     for e, c in enumerate(reversed(opts)):
    #         # print(c, '*', 'x', '**', e)
    #         p_y += c*x**e
    #     current_error = sklearn.metrics.mean_squared_error(y, p_y)
    #     all_errors.append((n, current_error))
    #     if best_error is None or current_error < best_error:
    #         best_error = current_error
    #         best_opts = opts
    #         best_p_y = p_y
    # regression_opts.append(best_opts)
    #
    # regression_trace = go.Scatter(x=x, y=best_p_y)
    # regression_traces.append(regression_trace)
    #
    # p = scipy.stats.chisquare(y, best_p_y).pvalue
    # subplot_titles.append(subplot_title + ", n={}, p={}".format(len(best_opts), round(p, 2)))


fig = tools.make_subplots(
    rows=1, cols=len(traces), subplot_titles=subplot_titles, shared_yaxes=True)
fig['layout'].update(dict(
    yaxis=dict(
        title='Aggregated Total Amount ($)'
    ),
    **{'xaxis%s' % (i+1): axis for i, axis in enumerate(xaxis)}
))


for i, trace in enumerate(traces):
    fig.append_trace(trace, 1, i+1)
    fig.append_trace(regression_traces[i], 1, i+1)


py.plot(fig, filename='plots/agg_correlation.html')

# ---



# ---

# from sklearn.gaussian_process import GaussianProcessRegressor
#
# x, y = traces[0].x, traces[0].y
# x_r = x.values.reshape(-1, 1)
#
# gp = GaussianProcessRegressor()
# model = gp.fit(x_r, y)
#
# y_pred, sigma = gp.predict(x_r, return_std=True)
#
# py.plot(dict(data=[go.Scatter(x=x, y=y, mode='markers'), go.Scatter(x=x, y=y_pred)], layout={'title':sigma}))





# ---
#
# df_time_amount_agg = (
#     df.groupBy(df.trip_time.cast(IntegerType()).alias('trip_time_int'))
#     .agg({'total_amount': 'sum', '*': 'count'})
#     .orderBy('trip_time_int').toPandas()
# )
#
# x = df_time_amount_agg[df_time_amount_agg.columns[0]]
# y = df_time_amount_agg[df_time_amount_agg.columns[1]]
#
# data = [go.Scatter(
#     x=x,
#     y=y,
#     mode='markers',
#     marker=go.Marker(
#         size=3
#     )
# )]
#
# corr = round(pearsonr(x, y)[0], 2)
# title = "Trip Time and Aggregated Total Amount, r={}".format(corr)
# layout = go.Layout(
#     title=title,
#     xaxis=dict(
#         title='Trip Time (minutes)',
#     ),
#     yaxis=dict(
#         title='Aggregated Total Amount ($)'
#     ),
# )
#
# fig = dict(data=data, layout=layout)
#
#
# py.plot(fig, filename='plots/agg_time_correlation.html')
