from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 6, 1),
}

dag_spark = DAG(
    dag_id = "de_project_sprint_7",
    default_args=default_args,
    schedule='0 0 * * *', # или schedule='@daily'
    catchup=True, 
    max_active_runs=1
)

user_locations = SparkSubmitOperator(
    task_id='step_2_user_locations',
    dag=dag_spark,
    application ='{{ dag_run.dag.folder }}/../scripts/step_2_user_locations.py' ,
    conn_id= 'yarn_spark',
    application_args = ["{{ ds }}", 7, "/user/s18314377/data/geo/geo.csv", 3, "/user/master/data/geo/events", "/user/s18314377/analytics"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

zone_statistics = SparkSubmitOperator(
    task_id='step_3_zone_statistics',
    dag=dag_spark,
    application ='{{ dag_run.dag.folder }}/../scripts/step_3_zone_statistics.py' ,
    conn_id= 'yarn_spark',
    application_args = ["{{ ds }}", 7, "/user/s18314377/data/geo/geo.csv", "/user/master/data/geo/events", "/user/s18314377/analytics"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

user_recommendations = SparkSubmitOperator(
    task_id='step_4_user_recommendations',
    dag=dag_spark,
    application ='{{ dag_run.dag.folder }}/../scripts/step_4_user_recommendations.py' ,
    conn_id= 'yarn_spark',
    application_args = ["{{ ds }}", 7, "/user/s18314377/data/geo/geo.csv", 10, "/user/master/data/geo/events", "/user/s18314377/analytics"],
    conf={
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 1,
    executor_memory = '1g'
)

[user_locations, zone_statistics, user_recommendations]

