import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F


from utils import get_timezone_column, input_paths, get_events_closest_cities



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


def main():
    
    date = sys.argv[1]
    days_count = int(sys.argv[2])
    geo_cities_path = sys.argv[3]
    home_days_count = int(sys.argv[4])
    events_base_path = sys.argv[5]
    output_base_path = sys.argv[6]    
    
    
    conf = SparkConf().setAppName(f"UserLocationsJob-{date}-d{days_count}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    print("Stage 0. Reading events and geo cities...")
    events_paths = input_paths(date=date, depth=days_count, data_dir=events_base_path)
    df_events = (
        sql.read
        .option("basePath", events_base_path)
        .parquet(*events_paths)
    )
    df_messages = df_events.filter(F.col("event_type") == "message")
    df_cities = sql.read.csv(geo_cities_path, sep=";", header=True, inferSchema=True)
    print(" . done.")

    
    print("Stage 1. Searching for closest cities for every message...")
    df_messages_closest_cities = get_events_closest_cities(df_messages, df_cities)
    print(" . done.")
    
    print("Stage 2. Searching for user act_city and home_city..")
    df_act_cities = get_users_act_cities(df_messages_closest_cities)
    df_home_cities = get_users_home_cities(df_messages_closest_cities, days_count=home_days_count)
    print(" . done.")

    print("Stage 3. Searching for user travel activity...")
    df_travel_info = get_users_travel_info(df_messages_closest_cities)
    print(" . done.")
    
    print("Stage 4. Searching for user local_time...")
    df_local_time = get_user_local_time(df_messages_closest_cities)
    print(" . done.")
    
    print("Joining results ...")
    df_user_locations = (
        df_act_cities
        .join(df_home_cities, on="user_id", how="outer")
        .join(df_travel_info, on="user_id", how="outer")
        .join(df_local_time, on="user_id", how="outer")
    )
    print(" . done.")
    
    output_path = f"{output_base_path}/user_locations/date={date}/days={days_count}/home_days={home_days_count}"
    print(f"Writing data to {output_path}...")
    df_user_locations.write.mode("overwrite").parquet(output_path)
    print(" . done.")
    

if __name__ == "__main__":
    main()