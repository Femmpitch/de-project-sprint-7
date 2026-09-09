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

from .utils import get_timezone_column



def get_users_act_cities(df_messages_closest_cities):
    df_act_city = df_messages_closest_cities \
        .withColumn(
            "rank_act_city", 
            F.row_number().over(
                Window().partitionBy("event.message_from").orderBy(F.col("event.message_ts").desc())
            )
        ) \
        .filter(F.col("rank_act_city") == 1) \
        
    return df_act_city.select(F.col("event.message_from").alias("user_id"), F.col("city").alias("act_city"))


def get_users_home_cities(df_messages_closest_cities, days_count=27):
    df_unique_days = df_messages_closest_cities.select(
        F.col("event.message_from").alias("user_id"),
        "city",
        "date"
    ).distinct()
    
    window_user_city = Window.partitionBy("user_id", "city").orderBy("date")
    
    # Сделали нумерацию в группах по юзерам-пользователям для каждого уникального дня
    df_indexed_days = df_unique_days.withColumn("day_index", F.row_number().over(window_user_city))
    
    # Вычитаем из дат дни, равные индексу 'day_seq'. При пропуске даты значение изменится - новая непрерывная группа
    df_continuous_sequences = df_indexed_days.withColumn("seq_start_date", F.date_sub(F.col("date"), F.col("day_index")))
    
    # Группируем по условным "датам начала последовательностей"
    df_continuous_groups = (
        df_continuous_sequences
        .groupBy("user_id", "city", "seq_start_date")
        .agg(F.count("date").alias("continuous_days"))
    )

    # Фильтруем по нужному days_count 
    df_filtered = df_continuous_groups.filter(F.col("continuous_days") >= days_count)
    
    window_latest = Window.partitionBy("user_id").orderBy(F.col("seq_start_date").desc())

    df_result = (
        df_filtered
        .withColumn("island_rank", F.row_number().over(window_latest))
        .filter(F.col("island_rank") == 1)
    )
    
    return df_result.select("user_id", F.col("city").alias("home_city"))



def get_users_travel_info(df_messages_closest_cities):
    # Шаг 1. Оставляем только уникальные дни (user + date), чтобы избавиться от дублей сообщений в один день
    df_user_days = df_messages_closest_cities.select(
        F.col("event.message_from").alias("user_id"), 
        "date", 
        "city"
    ).distinct()

    # Шаг 2. Окно для отслеживания хронологии пользователя
    window_time = Window.partitionBy("user_id").orderBy("date")

    # Шаг 3. Находим город из предыдущего дня
    df_with_prev = df_user_days.withColumn("prev_city", F.lag("city", 1).over(window_time))

    # Шаг 4. Ставим 1, если пользователь переехал в другой город (или это его первая запись)
    df_with_marker = df_with_prev.withColumn(
        "is_new_travel",
        F.when(F.col("prev_city").isNull() | (F.col("city") != F.col("prev_city")), 1).otherwise(0)
    )

    # Шаг 5. Создаем уникальный ID путешествия с помощью кумулятивной суммы
    # (Для каждого нового визита это число будет увеличиваться: 1, 2, 3...)
    df_with_travel_id = df_with_marker.withColumn(
        "travel_id", 
        F.sum("is_new_travel").over(window_time)
    )

    # Шаг 6. Схлопываем дни внутри одного путешествия. Нам нужна дата начала, чтобы сохранить хронологию визитов
    df_visits = (
        df_with_travel_id
        .groupBy("user_id", "travel_id", "city")
        .agg(F.min("date").alias("travel_start_date"))
    )

    # Шаг 7. Финальное окно, чтобы собрать города по порядку начала путешествий
    window_collect = Window.partitionBy("user_id").orderBy("travel_start_date")

    df_final_metrics = (
        df_visits
        # Собираем массив городов в правильном порядке
        .withColumn("travel_array", F.collect_list("city").over(window_collect))
        # Считаем общее количество посещений (размер массива)
        .withColumn("travel_count", F.size("travel_array"))
        # Берем последнюю строку для каждого юзера, где массивы собраны полностью
        .withColumn("row_num", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("travel_start_date"))))
        .filter(F.col("row_num") == 1)
        .select("user_id", "travel_count", "travel_array")
    )

    # Смотрим результат
    return df_final_metrics



def get_user_local_time(df_events):

    df_local_time = (
        df_events
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
    )

    df_local_time_latest = (
        df_local_time
        .select(F.col("event.message_from").alias("user_id"), "TIME_UTC", "timezone")
        .withColumn("local_time", F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone')))
        .withColumn(
            "rank", 
            F.row_number().over(
                Window().partitionBy("user_id").orderBy(F.col("TIME_UTC").desc()))
        )
        .filter(F.col("rank") == 1)
        .drop("rank", "TIME_UTC", "timezone")
    )
    
    return df_local_time_latest



def main(events_dir, geo_path, home_days_count=27):
    df_events = (
        spark.read
        .option("pathGlobFilter", "*part-000*.parquet") # Читаем только самый первый под-файл в каждой папке
        .parquet(events_dir)     # Заходим во все партиции
    )
    df_messages = df_events.filter(F.col("event_type") == "message")
    df_cities = spark.read.csv(geo_path, sep=";", header=True, inferSchema=True)
    
    ### Этап 1. Ищем ближайшие города к каждому сообщению
    df_messages_closest_cities = get_messages_closest_cities(df_messages, df_cities)
    
    ### Этап 2. Ищем последний город пользователя и последний домашний город пользователя
    df_act_cities = get_users_act_cities(df_messages_closest_cities)
    df_home_cities = get_users_home_cities(df_messages_closest_cities, days_count=home_days_count)
    
    ### Этап 3. Ищем статистику путешествий
    df_travel_info = get_users_travel_info(df_messages_closest_cities)
    
    
    ### Этап 4. Ищем местное время
    df_local_time = get_user_local_time(df_messages_closest_cities)
    
    
    df_result = (
        df_act_cities
        .join(df_home_cities, on="user_id", how="outer")
        .join(df_travel_info, on="user_id", how="outer")
        .join(df_local_time, on="user_id", how="outer")
    )
    return df_result
    

if __name__ == "__main__":
    main()