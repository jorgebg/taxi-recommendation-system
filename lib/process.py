from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, FloatType, IntegerType

from lib import boros


@udf(returnType=BooleanType())
def inside(lng, lat):
    '''
    belongs to boros?
    '''
    return all(boro.contains_point((lng, lat)) for boro in boros.polygons)


@udf(returnType=FloatType())
def trip_time(pickup_datetime, dropoff_datetime):
    '''
    trip time in minutes
    '''
    return (dropoff_datetime-pickup_datetime).seconds/60


@udf(returnType=IntegerType())
def weekday(pickup_datetime):
    '''
    0 = Monday
    '''
    return pickup_datetime.weekday()


@udf(returnType=IntegerType())
def hour(pickup_datetime):
    '''
    0-23
    '''
    return pickup_datetime.hour


@udf(returnType=FloatType())
def score(trip_time, trip_distance, passenger_count):
    '''
    Coefficientes are correlations computed at analysis.
    We divide by 10**5 so we have more human-friendly scores
    '''
    return (trip_time*422.52 + trip_distance*2728.12 + passenger_count*171616.23)/10**5


def process(df, count=False):
    """
    Run over 'data/yellow_tripdata_2016-01.csv'
        Process: Total 10906858
        Process: Filter Column<b'inside'> 10736161
        Process: Filter Column<b'(passenger_count > 0)'> 10735675
        Process: Filter Column<b'(trip_distance > 0)'> 10683655
        Process: Filter Column<b'(trip_distance < 100)'> 10683649
        Process: Filter Column<b'(total_amount > 0)'> 10680418
        Process: Filter Column<b'(tpep_pickup_datetime < tpep_dropoff_datetime)'> 10680131
        Process: Filter Column<b'(trip_time < 180)'> 10663592
    """

    df = df.withColumn('inside', inside(df.pickup_longitude, df.pickup_latitude))
    df = df.withColumn('trip_time', trip_time(df.tpep_pickup_datetime, df.tpep_dropoff_datetime))
    df = df.withColumn('weekday', weekday(df.tpep_pickup_datetime))
    df = df.withColumn('hour', hour(df.tpep_pickup_datetime))
    df = df.withColumn('score', score(df.trip_time, df.trip_distance, df.passenger_count))

    conditions = (
        df.inside,
        df.passenger_count > 0,
        df.trip_distance > 0,
        df.trip_distance < 100,  # 100 miles
        df.total_amount > 0,
        df.tpep_pickup_datetime < df.tpep_dropoff_datetime,  # trip_time > 0
        df.trip_time < 60*3  # 3 hours
    )
    if count:
        print('Process: Total', df.count())
    for condition in conditions:
        df = df.filter(condition)
        if count:
            print('Process: Filter', condition, df.count())
    return df
