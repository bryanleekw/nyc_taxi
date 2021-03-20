# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas as pd
import numpy as np
# get weekday
import calendar
import pyspark

from pyspark.sql import Row
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.functions import col


def read_datafile_2(csv_path, row_count=None):
    rdd1 = sc.textFile(csv_path).map(lambda line: line.split(",")).filter(lambda line: len(line) > 1)

    if 'dolocationid' in column_map:
        df = rdd1.map(lambda line: Row(pulocationid=line[column_map['pulocationid']],
                                       dolocationid=line[column_map['dolocationid']],
                                       pickup_datetime=line[column_map['pickup_datetime']],
                                       dropoff_datetime=line[column_map['dropoff_datetime']],
                                       trip_distance=line[column_map['trip_distance']],
                                       fare_amount=line[column_map['fare_amount']],
                                       passenger_count=line[column_map['passenger_count']])).toDF()
        df = df.withColumn("dolocationid", df["dolocationid"].cast(IntegerType()))
        df = df.withColumn("fare_amount", df["fare_amount"].cast(FloatType()))
        df = df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType()))
        df = df.withColumn("pulocationid", df["pulocationid"].cast(IntegerType()))
        df = df.withColumn("trip_distance", df["trip_distance"].cast(FloatType()))

        df = df.filter(col("dolocationid").isNotNull() & col("dropoff_datetime").isNotNull() & \
                       col("fare_amount").isNotNull() & col("passenger_count").isNotNull() & \
                       col("pulocationid").isNotNull() & col("trip_distance").isNotNull() & col("pickup_datetime").isNotNull())

    else:
        df = rdd1.map(lambda line: Row(pickup_longitude=line[column_map['pickup_longitude']],
                                       pickup_latitude=line[column_map['pickup_latitude']],
                                       dropoff_longitude=line[column_map['dropoff_longitude']],
                                       dropoff_latitude=line[column_map['dropoff_latitude']],
                                       pickup_datetime=line[column_map['pickup_datetime']],
                                       dropoff_datetime=line[column_map['dropoff_datetime']],
                                       trip_distance=line[column_map['trip_distance']],
                                       fare_amount=line[column_map['fare_amount']],
                                       passenger_count=line[column_map['passenger_count']])).toDF()
        df = df.withColumn("dropoff_longitude", df["dropoff_longitude"].cast(FloatType()))
        df = df.withColumn("dropoff_latitude", df["dropoff_latitude"].cast(FloatType()))
        df = df.withColumn("fare_amount", df["fare_amount"].cast(FloatType()))
        df = df.withColumn("passenger_count", df["passenger_count"].cast(IntegerType()))
        df = df.withColumn("pickup_longitude", df["pickup_longitude"].cast(FloatType()))
        df = df.withColumn("pickup_latitude", df["pickup_latitude"].cast(FloatType()))
        df = df.withColumn("trip_distance", df["trip_distance"].cast(FloatType()))
        df = df.filter(col("dropoff_longitude").isNotNull() & col("dropoff_datetime").isNotNull() & \
                       col("fare_amount").isNotNull() & col("passenger_count").isNotNull() & \
                       col("dropoff_longitude").isNotNull() & col("trip_distance").isNotNull() & \
                       col("pickup_latitude").isNotNull() & col("pickup_longitude").isNotNull() & \
                       col("pickup_datetime").isNotNull())
    return df


def remove_nan_values(df):
    df = df.replace(to_replace='None', value=np.nan).dropna()
    print(df.shape)
    df = df[(df != 0).all(1)]
    print(df.shape)
    return df


def print_func_name(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Function name is, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def basic_feature_extract(df):
    df_= df.copy()
    # pickup
    df_["pickup_date"] = pd.to_datetime(df_.lpep_pickup_datetime.apply(lambda x : x.split(" ")[0]))
    df_["pickup_hour"] = df_.lpep_pickup_datetime.apply(lambda x : x.split(" ")[1].split(":")[0])
    df_["pickup_year"] = df_.lpep_pickup_datetime.apply(lambda x : x.split(" ")[0].split("-")[0])
    df_["pickup_month"] = df_.lpep_pickup_datetime.apply(lambda x : x.split(" ")[0].split("-")[1])
    df_["pickup_weekday"] = df_.lpep_pickup_datetime.apply(lambda x :pd.to_datetime(x.split(" ")[0]).weekday())
    # dropoff
    # in case test data dont have dropoff_datetime feature
    try:
        df_["dropoff_date"] = pd.to_datetime(df_.lpep_dropoff_datetime.apply(lambda x : x.split(" ")[0]))
        df_["dropoff_hour"] = df_.lpep_dropoff_datetime.apply(lambda x : x.split(" ")[1].split(":")[0])
        df_["dropoff_year"] = df_.lpep_dropoff_datetime.apply(lambda x : x.split(" ")[0].split("-")[0])
        df_["dropoff_month"] = df_.lpep_dropoff_datetime.apply(lambda x : x.split(" ")[0].split("-")[1])
        df_["dropoff_weekday"] = df_.lpep_dropoff_datetime.apply(lambda x :pd.to_datetime(x.split(" ")[0]).weekday())
    except:
        pass
    return df_


def get_weekday(df):
    list(calendar.day_name)
    df_=df.copy()
    df_['pickup_week_'] = pd.to_datetime(df_.lpep_pickup_datetime).dt.weekday
    df_['pickup_weekday_'] = df_['pickup_week_'].apply(lambda x: calendar.day_name[x])
    return df_

# get trip duration
def get_duration(df):
    df_= df.copy()
    df_['trip_duration_cal'] = pd.to_datetime(df_['dropoff_datetime']) - pd.to_datetime(df_['pickup_datetime'])
    return df_

def read_data_file(csv_path, row_count=None):
    dfcolumns = pd.read_csv(csv_path, nrows = 1)
    ncols = len(dfcolumns.columns)
    if row_count:
        df = pd.read_csv(csv_path, header = None, sep= ',',
                     skiprows = 1, usecols = list(range(ncols)),
                     names = dfcolumns.columns, low_memory=False, nrows=row_count)
    else:
        df = pd.read_csv(csv_path, header = None, sep= ',',
                         skiprows = 1, usecols = list(range(ncols)),
                         names = dfcolumns.columns, low_memory=False)
    return df


def get_columns(df, col_type="relevant"):
    cols = []
    dfcols = list(df.columns)
    if col_type == "relevant":
        subs_to_check = ['time', 'location', 'passenger', 'distance',
                         'ratecode', 'fare', "longitude", "latitude"]
        for sub in subs_to_check:
            for col in dfcols:
                if sub.lower() in col.lower():
                    cols.append(col)

    elif col_type == "geolocation":
        subs_to_check = ["location", "longitude", "latitude"]
        for sub in subs_to_check:
            for col in dfcols:
                if sub.lower() in col.lower():
                    cols.append(col)
    return cols


def get_column_name(csv_path, row_count=None):
    datafile = csv_path # file with long/lat
    with open(datafile) as f:
        col_names = f.readline()
    col_names = col_names.split(",")
    return col_names




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    Tripfiles = [r'C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\TripData\green_tripdata_2013-08.csv',
             r'C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\TripData\green_tripdata_2020-01.csv']

    Reffiles = [r"C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\ReferenceData\payment_type_lookup.csv",
                r"C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\ReferenceData\rate_code_lookup.csv",
                r"C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\ReferenceData\taxi_zone_lookup.csv",
                r"C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\ReferenceData\trip_month_lookup.csv",
                r"C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\ReferenceData\trip_type_lookup.csv",
                r"C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\ReferenceData\vendor_lookup.csv"]

    df = read_data_file(Tripfiles[1])
    cols = get_columns(df)
    print(cols)

    cols = get_column_name(r'C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\TripData\green_tripdata_2013-08.csv')
    print(cols)

    df = read_datafile_2(r'C:\Users\bryanleekw\PycharmProjects\nyc_taxi\data\TripData\green_tripdata_2013-08.csv')
    df.show()



    print_func_name('NYC Taxi')





