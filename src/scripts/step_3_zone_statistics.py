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
from utils import input_paths, get_events_closest_cities



def get_zone_stats(df_events_closest_cities):
    
    
    df_zone_prepared = (
        df_events_closest_cities
        .withColumn("month", F.month(F.col("date")))
        .withColumn("week", F.weekofyear(F.col("date")))
        .withColumn(
            "first_message_ts", 
                F.min(
                    F.when(
                        F.col("event_type") == "message",
                        F.col("event.message_ts")
                    )
                ).over(Window.partitionBy("event.message_from"))
        )

    )

    df_stats_month = (
        df_zone_prepared
        .groupBy("month", F.col("city").alias("zone_id"))
        .agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("month_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("month_reaction"),        
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("month_subscription"),
            F.sum(
                F.when(
                    (F.col("event.message_ts") == F.col("first_message_ts")), 
                    1
                ).otherwise(0)
            ).alias("month_user")
        )
    )


    df_stats_week = (
    df_zone_prepared
    .groupBy("month", "week", F.col("city").alias("zone_id"))  # Месяц здесь нужен как мостик для джойна
    .agg(
        F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
        F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
        F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
        F.sum(
                F.when(
                    (F.col("event.message_ts") == F.col("first_message_ts")), 
                    1
                ).otherwise(0)
            ).alias("week_user")
        
        )
    )
    
    
    df_zone_statistics = df_stats_week.join(
        df_stats_month, 
        on=["month", "zone_id"], 
        how="left"
    )
    return df_zone_statistics


def main():
    
    date = sys.argv[1]
    days_count = int(sys.argv[2])
    geo_cities_path = sys.argv[3]
    events_base_path = sys.argv[4]
    output_base_path = sys.argv[5]    
    
    
    conf = SparkConf().setAppName(f"CityStatisticsJob-{date}-d{days_count}")
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
    
    print("Stage 2. Searching for cities monthly and weekly statistics...")
    df_zone_statistics = get_zone_stats(df_events_closest_cities)
    
    
    output_path = f"{output_base_path}/zone_statistics/date={date}/days={days_count}"
    print(f"Writing data to {output_path}...")
    df_zone_statistics.write.mode("overwrite").parquet(output_path)
    print(" . done.")

    
    

if __name__ == "__main__":
    main()

