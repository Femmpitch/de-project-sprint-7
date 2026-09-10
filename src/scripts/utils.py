from datetime import datetime, timedelta
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.types import LongType

import pyspark
from pyspark.sql import SparkSession


def input_paths(date, depth, data_dir):
    
    start_date = datetime.strptime(date, "%Y-%m-%d")
    
    result = []
    for i in range(depth):
        current_date = start_date - timedelta(days=i)
        result.append(f"{data_dir}/date={current_date.strftime('%Y-%m-%d')}")
    return result


def clean_coord(col_name):
    return F.regexp_replace(F.col(col_name), ",", ".").cast(DoubleType())


def get_distance_formula(obj_1_lat, obj_1_lon, obj_2_lat, obj_2_lon):
    lat1 = F.radians(F.col(obj_1_lat))
    lon1 = F.radians(F.col(obj_1_lon))
    lat2 = F.radians(clean_coord(obj_2_lat))
    lon2 = F.radians(clean_coord(obj_2_lon))
    
    r = 6371

    distance_formula = (
        F.lit(2 * r) * F.asin(
            F.sqrt(    
                F.pow(F.sin((lat2 - lat1) / 2), 2) + 
                F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), 2)
            )
        )
    )
    
    return distance_formula




def get_events_closest_cities(df_events, df_cities):
    df_cities_prepared = df_cities.select(
        F.col("city"),
        F.col("lat").alias("city_lat"),
        F.col("lng").alias("city_lon")
    )

    df_events_prepared = df_events.select(
        "event",
        "event_type",
        "date",
        F.col("lat").alias("event_lat"),
        F.col("lon").alias("event_lon")
    ).withColumn("row_unique_id", F.monotonically_increasing_id())
    
    df_joined = df_events_prepared.crossJoin(F.broadcast(df_cities_prepared))
    distance_formula = get_distance_formula("event", "city")
    
    df_joined = df_joined.withColumn("distance", distance_formula)
    window = Window().partitionBy('row_unique_id').orderBy("distance")

    df_messages_closest_cities = df_joined \
                .withColumn("row_number", F.row_number().over(window)) \
                .filter(F.col("row_number") == 1) \
                .drop("row_number", "city_lat", "city_lon", "row_unique_id")
    
    return df_messages_closest_cities
    
    



def get_timezone_column():
    city_to_tz_map = F.create_map([
        # Сидней
        F.lit("Sydney"), F.lit("Australia/Sydney"),
        F.lit("Canberra"), F.lit("Australia/Sydney"),
        F.lit("Newcastle"), F.lit("Australia/Sydney"),
        F.lit("Wollongong"), F.lit("Australia/Sydney"),
        F.lit("Maitland"), F.lit("Australia/Sydney"),
    
        # Мельбурн
        F.lit("Melbourne"), F.lit("Australia/Melbourne"),
        F.lit("Cranbourne"), F.lit("Australia/Melbourne"),
        F.lit("Geelong"), F.lit("Australia/Melbourne"),
        F.lit("Ballarat"), F.lit("Australia/Melbourne"),
        F.lit("Bendigo"), F.lit("Australia/Melbourne"),
    
        # Брисбен
        F.lit("Brisbane"), F.lit("Australia/Brisbane"),
        F.lit("Gold Coast"), F.lit("Australia/Brisbane"),
        F.lit("Townsville"), F.lit("Australia/Brisbane"),
        F.lit("Ipswich"), F.lit("Australia/Brisbane"),
        F.lit("Cairns"), F.lit("Australia/Brisbane"),
        F.lit("Toowoomba"), F.lit("Australia/Brisbane"),
        F.lit("Mackay"), F.lit("Australia/Brisbane"),
        F.lit("Rockhampton"), F.lit("Australia/Brisbane"),
    
        # Перт
        F.lit("Perth"), F.lit("Australia/Perth"),
        F.lit("Bunbury"), F.lit("Australia/Perth"),
    
        # Аделаида
        F.lit("Adelaide"), F.lit("Australia/Adelaide"),
    
        # Хобарт
        F.lit("Hobart"), F.lit("Australia/Hobart"),
        F.lit("Launceston"), F.lit("Australia/Hobart"),
    
        # Дарвин
        F.lit("Darwin"), F.lit("Australia/Darwin")
    ])
    return F.coalesce(
                city_to_tz_map[F.initcap(F.trim(F.col("city")))], 
                F.lit("Australia/Sydney") 
            )


