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

from .utils import get_timezone_column, get_distance_formula, get_messages_closest_cities




def get_subscriptions_users_unique_pairs(df_events):
    df_users_subscriptions = (
        df_events
        .filter(F.col("event_type") == "subscription")
        .select(
            F.col("event.user").alias("user_id"),
            F.col("event.subscription_channel")
        ).distinct()
    )
    df_user_pairs_all = df_users_subscriptions.alias("df1").join(
        df_users_subscriptions.alias("df2"),
        F.col("df1.subscription_channel") == F.col("df2.subscription_channel")
    )

    df_subscriptions_user_pairs = (
        df_user_pairs_all
        .filter(
            F.col("df1.user_id").cast(LongType()) < F.col("df2.user_id").cast(LongType())
        )
        .select(
            F.col("df1.user_id").alias("user_left"), 
            F.col("df2.user_id").alias("user_right")
        )
        .distinct()
    )
    
    return df_subscriptions_user_pairs


def get_messages_users_unique_pairs(df_events):
    
    df_messages_user_pairs = (
        df_events
        .filter(F.col("event_type") == "message")
        .filter(F.col("event.message_to").isNotNull())
        .select(
            F.col("event.message_from").alias("user_left"), 
            F.col("event.message_to").alias("user_right")
        )
        .distinct()
    )
    
    return df_messages_user_pairs


def get_users_latest_message(df_events):
    df_users_latest_message = (
        df_events
        .filter(F.col("event_type") == "message")
        .withColumn(
            "TIME_UTC", 
                F.date_format(
                    F.coalesce(
                        F.col("event.message_ts"), 
                        F.col("event.datetime")
                    ).cast("timestamp"),
                    "yyyy-MM-dd HH:mm:ss"
                ).cast("timestamp")
        )
        .withColumn(
            "timezone", get_timezone_column()
        )
        .withColumn(
            "rank", 
            F.row_number()
            .over(
                Window().partitionBy(F.col("event.message_from")).orderBy(F.col("TIME_UTC").desc())
            )
        )
        .filter(F.col("rank") == 1)
        .withColumn("local_time", F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone')))

    )
    return df_users_latest_message




def main():
    df_events = (
            spark.read
            .option("pathGlobFilter", "*part-000*.parquet") # Читаем только самый первый под-файл в каждой папке
            .parquet(events_dir)     # Заходим во все партиции
    #         .filter(F.col("date").isin(["2022-05-01", "2022-05-02", "2022-05-03"]))
    )

    df_events = df_events.sample(withReplacement=False, fraction=0.1, seed=42)


    df_cities = spark.read.csv(geo_path, sep=";", header=True, inferSchema=True)
    df_events_closest_cities.unpersist()
    df_events_closest_cities = get_messages_closest_cities(df_events, df_cities)


    df_subscriptions_user_pairs = get_subscriptions_users_unique_pairs(df_events_closest_cities)
    df_messages_user_pairs = get_messages_users_unique_pairs(df_events_closest_cities)

    # Шаг 1: Вычитаем переписки в направлении user_id_1 -> user_id_2
    df_pairs = df_subscriptions_user_pairs.join(
        df_messages_user_pairs,
        (df_subscriptions_user_pairs["user_left"] == df_messages_user_pairs["user_left"]) & 
        (df_subscriptions_user_pairs["user_right"] == df_messages_user_pairs["user_right"]),
        how="left_anti"
    )
    # Шаг 2: Из результата вычитаем переписки в обратном направлении user_id_2 -> user_id_1
    df_pairs = df_pairs.join(
        df_messages_user_pairs,
        (df_pairs["user_left"] == df_messages_user_pairs["user_right"]) & 
        (df_pairs["user_right"] == df_messages_user_pairs["user_left"]),
        how="left_anti"
    )



    df_users_latest_message = get_users_latest_message(df_events_closest_cities)


    final_pairs_df = df_pairs
    # 1. Готовим компактную табличку с координатами юзеров
    df_user_coords = df_users_latest_message.select(
        F.col("event.message_from").alias("user_id"),
        F.col("event_lat").alias("user_lat"),   # подставь свои названия колонок, если они другие
        F.col("event_lon").alias("user_lon"),
        F.col("city").alias("zone_id"),
        F.col("local_time")
    )

    # 2. Джойним координаты для ЛЕВОГО юзера (user_left)
    # Используем F.broadcast(), чтобы Spark работал молниеносно
    df_with_coord_left = final_pairs_df.join(
        F.broadcast(df_user_coords.alias("u_left")),
        final_pairs_df["user_left"] == F.col("u_left.user_id"),
        how="inner"
    ).select(
        final_pairs_df["user_left"],
        final_pairs_df["user_right"],
        F.col("u_left.user_lat").alias("lat_left"),
        F.col("u_left.user_lon").alias("lon_left"),
        "u_left.zone_id", "u_left.local_time"
    )

    # 3. Джойним координаты для ПРАВОГО юзера (user_right)
    df_with_all_coords = df_with_coord_left.join(
        F.broadcast(df_user_coords.alias("u_right")),
        df_with_coord_left["user_right"] == F.col("u_right.user_id"),
        how="inner"
    ).select(
        df_with_coord_left["user_left"],
        df_with_coord_left["user_right"],
        df_with_coord_left["lat_left"],
        df_with_coord_left["lon_left"],
        F.col("u_right.user_lat").alias("lat_right"),
        F.col("u_right.user_lon").alias("lon_right"),
        "u_left.zone_id", "u_left.local_time"

    )

    # 4. Считаем дистанцию и фильтруем пары в радиусе 1 км
    distance_formula = get_distance_formula("lat_left", "lon_left", "lat_right", "lon_right")

    close_pairs_df = (
        df_with_all_coords
        .withColumn("distance", distance_formula) 
        .filter(F.col("distance") <= 100)
        .withColumn("processed_dttm", F.current_timestamp())
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")

    )
    close_pairs_df.show()
