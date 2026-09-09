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



def get_grouped_stats(df_events_closest_cities):
    
    
    df_zone_stats = (
        df_events_closest_cities
        .withColumn("month", F.month(F.col("date")))
        .withColumn("week", F.weekofyear(F.col("date")))
        .withColumn(
            "first_message_ts", 
            F.min(F.when(F.col("event_type") == "message", F.col("event.message_ts"))).over(Window.partitionBy("event.message_from"))
        )

    )

    df_stats_month = (
        df_zone_stats
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
    df_zone_stats
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
            ).alias("month_user")
        
        )
    )
    final_df = df_stats_week.join(
        df_stats_month, 
        on=["month", "zone_id"], 
        how="left"
    )
    return final_df


def main():
    df_events = (
        spark.read
        .option("pathGlobFilter", "*part-0000*.parquet") # Читаем только самый первый под-файл в каждой папке
        .parquet(events_dir)     # Заходим во все партиции
    )

    df_events = df_events.sample(withReplacement=False, fraction=0.1, seed=42)
    # df_events = df_events.filter(F.col("date") == "2022-01-01")


    df_cities = spark.read.csv(geo_path, sep=";", header=True, inferSchema=True)

    df_events_closest_cities = get_messages_closest_cities(df_events, df_cities)
    
    df_stats = get_grouped_stats(df_zone_stats)
    
    return df_stats
    

if __name__ == "__main__":
    main()

