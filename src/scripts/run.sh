# Запуск Step 2: User Locations
/usr/lib/spark/bin/spark-submit --master local  src/scripts/step_2_user_locations.py 2022-05-01 7 /user/s18314377/data/geo/geo.csv 3 /user/master/data/geo/events /user/s18314377/analytics


# Запуск Step 3: City Statistics
/usr/lib/spark/bin/spark-submit --master local  src/scripts/step_3_zone_statistics.py 2022-05-01 7 /user/s18314377/data/geo/geo.csv /user/master/data/geo/events /user/s18314377/analytics



# Запуск Step 3: City Statistics
/usr/lib/spark/bin/spark-submit --master local  src/scripts/step_4_user_recommendations.py 2022-05-01 7 /user/s18314377/data/geo/geo.csv 10 /user/master/data/geo/events /user/s18314377/analytics







# Запуск Step 2: User Locations
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster src/scripts/step_2_user_locations.py 2022-06-21 60 /user/s18314377/data/geo/geo.csv 27 /user/master/data/geo/events /user/s18314377/analytics


# Запуск Step 3: City Statistics
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster  src/scripts/step_3_zone_statistics.py 2022-06-21 60 /user/s18314377/data/geo/geo.csv /user/master/data/geo/events /user/s18314377/analytics



# Запуск Step 3: City Statistics
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster  src/scripts/step_4_user_recommendations.py 2022-06-21 60 /user/s18314377/data/geo/geo.csv 1 /user/master/data/geo/events /user/s18314377/analytics
