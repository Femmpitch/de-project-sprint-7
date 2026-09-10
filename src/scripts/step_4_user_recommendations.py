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

from utils import get_timezone_column, get_distance_formula, get_events_closest_cities, input_paths




def get_subscriptions_users_unique_pairs(df_events):

    # Сначала найдем все подписки
    df_users_subscriptions = (
        df_events
        .filter(F.col("event_type") == "subscription")
        .select(
            F.col("event.user").alias("user_id"),
            F.col("event.subscription_channel")
        ).distinct()
    )

    # Затем генерируем пары: датафрейм сам с собой (все варианты)
    df_user_pairs_all = df_users_subscriptions.alias("df1").join(
        df_users_subscriptions.alias("df2"),
        F.col("df1.subscription_channel") == F.col("df2.subscription_channel")
    )

    # Фильтруем пары: убираем перестановки и пары c одним и тем же юзером
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
    # Ищем последние сообщения пользователя (понадобится для расчета текущего расстояния и local_time)
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


def get_user_recommendations(date, maximum_distance_km, df_subscriptions_user_pairs, df_messages_user_pairs, df_users_latest_message):
    """
    Задача - сджоинить все три условия для пар юзеров.

    План:
    
    1. Находим пары, у которых есть каналы и у которых нет сообщений
    2. Находим пары из этапа 1, у которых последние сообщения были сделаны на расстоянии менее 1 км
    
    """
    
    print(" . Step 1. Getting pairs without messages, direction user_left -> user_right...")
    df_pairs_ch_m_left = df_subscriptions_user_pairs.join(
        df_messages_user_pairs,
        (df_subscriptions_user_pairs["user_left"] == df_messages_user_pairs["user_left"]) & 
        (df_subscriptions_user_pairs["user_right"] == df_messages_user_pairs["user_right"]),
        how="left_anti"
    )
    print(" . Step 2. Getting pairs without messages, direction user_right -> user_left...")
    df_pairs_ch_m_left_right = df_pairs_ch_m_left.join(
        df_messages_user_pairs,
        (df_pairs_ch_m_left["user_left"] == df_messages_user_pairs["user_right"]) & 
        (df_pairs_ch_m_left["user_right"] == df_messages_user_pairs["user_left"]),
        how="left_anti"
    )
    
    print(" . Step 3. Joining latest message coordinates to pairs...")
    df_user_coords = df_users_latest_message.select(
        F.col("event.message_from").alias("user_id"),
        F.col("event_lat").alias("user_lat"),   # подставь свои названия колонок, если они другие
        F.col("event_lon").alias("user_lon"),
        F.col("city").alias("zone_id"),
        F.col("local_time")
    )

    df_with_coord_left = df_pairs_ch_m_left_right.join(
        F.broadcast(df_user_coords.alias("u_left")),
        df_pairs_ch_m_left_right["user_left"] == F.col("u_left.user_id"),
        how="inner"
    ).select(
        df_pairs_ch_m_left_right["user_left"],
        df_pairs_ch_m_left_right["user_right"],
        F.col("u_left.user_lat").alias("lat_left"),
        F.col("u_left.user_lon").alias("lon_left"),
        "u_left.zone_id", "u_left.local_time"
    )

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

    distance_formula = get_distance_formula("lat_left", "lon_left", "lat_right", "lon_right")

    # Пишу так, потому что сделал скрипт по конкретным дням, а не последнему.
    processed_dttm = F.lit(f"{date} 23:59:59").cast("timestamp")
    
    print(" . Step 4. Calculating distance and filter it")
    df_user_recommendations = (
        df_with_all_coords
        .withColumn("distance", distance_formula) 
        .filter(F.col("distance") <= maximum_distance_km)
        .withColumn("processed_dttm", processed_dttm)
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")
    )
    
    return df_user_recommendations





def main():
     
    date = sys.argv[1]
    days_count = int(sys.argv[2])
    geo_cities_path = sys.argv[3]
    maximum_distance_km = float(sys.argv[4])
    events_base_path = sys.argv[5]
    output_base_path = sys.argv[6]    
    
    
    conf = SparkConf().setAppName(f"UserRecommendationsJob-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)
    
    
    print("Stage 0. Reading events and geo cities...")
    events_paths = input_paths(date=date, depth=days_count, data_dir=events_base_path)
    df_events = (
        sql.read
        .option("basePath", events_base_path)
        .parquet(*events_paths)
    )
    df_cities = sql.read.csv(geo_cities_path, sep=";", header=True, inferSchema=True)
        
    print("Stage 1. Searching for closest cities for every message...")
    df_events_closest_cities = get_events_closest_cities(df_events, df_cities)
    df_events_closest_cities.cache()

    print("Stage 2. Get users pairs with same subscriptions...")
    df_subscriptions_user_pairs = get_subscriptions_users_unique_pairs(df_events_closest_cities)
    print("Stage 3. Get users pairs with messages...")
    df_messages_user_pairs = get_messages_users_unique_pairs(df_events_closest_cities)
    
    print("Stage 4. Get users latest message")
    df_users_latest_message = get_users_latest_message(df_events_closest_cities)

    
    print("Stage 5. Get users recommendations")
    df_user_recommendations = get_user_recommendations(
        date, 
        maximum_distance_km, 
        df_subscriptions_user_pairs, 
        df_messages_user_pairs, 
        df_users_latest_message
    )
    
    output_path = f"{output_base_path}/user_recommendations/date={date}/days={days_count}/maximum_distance_km={maximum_distance_km}"
    print(f"Writing data to {output_path}...")
    df_user_recommendations.write.mode("overwrite").parquet(output_path)
    print(" . done.")


if __name__ == "__main__":
    main()