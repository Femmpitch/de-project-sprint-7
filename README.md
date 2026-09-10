# Проект 7-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 7-го спринта


### Описание витрин

**Важно**: Для отладки я во все скрипты добавил параметры `date/days_count`, чтобы ограничить количество обрабатываемых данных. Часто YARN отваливался, поэтому расчеты делал на local.

С другой стороны, хотя в условиях задачи это не оговорено, иметь эти параметры очень полезно, чтобы получить статистику по разным периодам. Так что, может, это и к лучшему.

Всего в проекте нужны три витрины.

1. Информация о локации пользователей (Шаг 2 проекта)
2. Cтатистика событий по городам -  недельная и месячная (Шаг 3)
3. Рекомендации новых пользователей по близости (Шаг 3)


Рассмотрим их подробнее.

1. Cкрипт `step_2_user_locations.py` - считает все указанные метрики, складывает витрину в 

	`/user/s18314377/analytics/user_locations/date={date}/days={days_count}/home_days={home_days_count}`,

	где `home_days_count` - размер окна, за который считается "домашний" город

2. Скрипт `step_3_zone_statistics.py` - Считает статистики событий по месяцам и неделям для каждого города. Результаты складываются в 

	`/user/s18314377/analytics/user_locations/zone_statistics/date={date}/days={days_count}`.

3. Скрипт `step_4_user_recommendations.py` - ищет пары пользователей для рекомендаций (фильтры - есть общие каналы, раньше не переписывались, находятся на расстоянии не более `maximum_distance_km`). Витрина складывается в 
	`/user/s18314377/analytics/user_locations/user_recommendations/date={date}/days={days_count}/maximum_distance_km={maximum_distance_km}`.


Команды для запусков скриптов (На YARN, поставил 120 дней и последнюю дату с данными):

```
# Запуск Step 2: User Locations
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster --py-files src/scripts/utils.py src/scripts/step_2_user_locations.py 2022-06-21 120 /user/s18314377/data/geo/geo.csv 27 /user/master/data/geo/events /user/s18314377/analytics

# Запуск Step 3: City Statistics
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster --py-files src/scripts/utils.py  src/scripts/step_3_zone_statistics.py 2022-06-21 120 /user/s18314377/data/geo/geo.csv /user/master/data/geo/events /user/s18314377/analytics

# Запуск Step 4: User Recommendations
/usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster --py-files src/scripts/utils.py  src/scripts/step_4_user_recommendations.py 2022-06-21 120 /user/s18314377/data/geo/geo.csv 1 /user/master/data/geo/events /user/s18314377/analytics
```

Я сделал запуски для `date=2022-06-21/days=120` для всех трех витрин, в директории `/user/s18314377/analytics`

В DAG сделал небольшие запуски за 15 дней, чтобы проверить работоспособность. Ограничил 1 днем - последним.

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-7` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-7.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-7`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Структура репозитория
Вложенные файлы в репозиторий будут использоваться для проверки и предоставления обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре — так будет проще соотнести задания с решениями.

Внутри `src` расположены две папки:
- `/src/dags`;
- `/src/sql`.
