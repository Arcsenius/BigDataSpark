# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Одним из самых популярных фреймворков для работы с Big Data является Apache Spark. Apache Spark - мощный фреймворк, который предлагает широкий набор функциональности для простого написания ETL-пайплайнов.

Что необходимо сделать? 

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv с номерами) в модель данных звезда в PostgreSQL, а затем на основе модели данных звезда создать ряд отчетов по данным в одной из NoSQL базах данных обязательно и в нескольких других опционально (будет бонусом). Каждый отчет представляет собой отдельную таблицу в NoSQL БД.

Какие отчеты надо создать?
1. Витрина продаж по продуктам
Цель: Анализ выручки, количества продаж и популярности продуктов.
 - Топ-10 самых продаваемых продуктов.
 - Общая выручка по категориям продуктов.
 - Средний рейтинг и количество отзывов для каждого продукта.
2. Витрина продаж по клиентам
Цель: Анализ покупательского поведения и сегментация клиентов.
 - Топ-10 клиентов с наибольшей общей суммой покупок.
 - Распределение клиентов по странам.
 - Средний чек для каждого клиента.
3. Витрина продаж по времени
Цель: Анализ сезонности и трендов продаж.
 - Месячные и годовые тренды продаж.
 - Сравнение выручки за разные периоды.
 - Средний размер заказа по месяцам.
4. Витрина продаж по магазинам
Цель: Анализ эффективности магазинов.
 - Топ-5 магазинов с наибольшей выручкой.
 - Распределение продаж по городам и странам.
 - Средний чек для каждого магазина.
5. Витрина продаж по поставщикам
Цель: Анализ эффективности поставщиков.
 - Топ-5 поставщиков с наибольшей выручкой.
 - Средняя цена товаров от каждого поставщика.
 - Распределение продаж по странам поставщиков.
6. Витрина качества продукции
Цель: Анализ отзывов и рейтингов товаров.
 - Продукты с наивысшим и наименьшим рейтингом.
 - Корреляция между рейтингом и объемом продаж.
 - Продукты с наибольшим количеством отзывов.

В каких NoSQL БД должны быть эти отчеты:
1. **Clickhouse** **(обязательно)**
2. Cassandra (опционально, если будет реализация, то это бонус)
3. Neo4J (опционально, если будет реализация, то это бонус)
4. MongoDB (опционально, если будет реализация, то это бонус)
5. Valkey (опционально, если будет реализация, то это бонус)

![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете себе инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Spark (рекомендую установку через Docker. Для удобства написания кода на Python можно запустить вместе со JupyterNotebook. Для Java - подключить volume и собрать образ Docker, который будет запускать команду spark-submit с java jar-файлом при старте контейнера, сам jar файл собирается отдельно и кладется в подключенный volume)
5. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
6. Импортируете данные в БД PostgreSQL (например, через механизм импорта csv в DBeaver). Всего в таблице mock_data должно находиться 10000 строк из 10 файлов.
7. Анализируете исходные данные с помощью запросов.
8. Выявляете сущности фактов и измерений.
9. Реализуете приложение на Spark, которое по аналогии с первой лабораторной работой перекладывает исходные данные из PostgreSQL в модель снежинку/звезда в PostgreSQL. (Убедитесь в коннективности Spark и PostgreSQL, настройте сеть между Spark и PostgreSQL, если используете Docker).
10. Устанавливаете ClickHouse (рекомендую установку через Docker. Убедитесь в коннективности Spark и Clickhouse, настройте сеть между Spark и ClickHouse). **(обязательно)**
11. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в ClickHouse. **(обязательно)**
12. Устанавливаете Cassandra (рекомендую установку через Docker. Убедитесь в коннективности Spark и Cassandra, настройте сеть между Spark и Cassandra). (опционально)
13. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в Cassandra. (опционально)
14. Устанавливаете Neo4j (рекомендую установку через Docker. Убедитесь в коннективности Spark и Neo4j, настройте сеть между Spark и Neo4j). (опционально)
15. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных сущностей в Neo4j. (опционально)
16. Устанавливаете MongoDB (рекомендую установку через Docker. Убедитесь в коннективности Spark и MongoDB, настройте сеть между Spark и MongoDB). (опционально)
17. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных коллекций в MongoDB. (опционально)
18. Устанавливаете Valkey (рекомендую установку через Docker. Убедитесь в коннективности Spark и Valkey, настройте сеть между Spark и Valkey). (опционально)
19. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных записей в Valkey. (опционально)
20. Проверяете отчеты в каждой базе данных средствами языка самой БД (ClickHouse - SQL (DBeaver), Cassandra - CQL (DBeaver), Neo4J - Cipher (DBeaver), MongoDB - MQL (Compass), Valkey - redis-cli).
21. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Spark, ClickHouse **(обязательно)**, Cassandra (опционально), Neo4j (опционально), MongoDB (опционально), Valkey (опционально) и заполненными данными в PostgreSQL из файлов mock_data(*).csv.
3. Инструкция, как запускать Spark-джобы для проверки лабораторной работы.
4. Код Apache Spark трансформации данных из исходной модели в снежинку/звезду в PostgreSQL.
5. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в ClickHouse.
6. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Cassandra.
7. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Neo4j.
8. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в MongoDB.
9. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Valkey.

## Реализация в этом репозитории

Реализована обязательная часть лабораторной работы:

1. PostgreSQL с raw-таблицей `mock_data` и моделью звезда.
2. Spark ETL на PySpark.
3. ClickHouse с 6 отдельными таблицами отчетов.

Опциональные Cassandra, Neo4j, MongoDB и Valkey не реализованы.

## Запуск

Поднять PostgreSQL и ClickHouse:

```bash
docker compose up -d postgres clickhouse
```

Запустить Spark ETL:

```bash
docker compose run --rm spark /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.3 jobs/run_etl.py
```

ETL выполняет полный цикл:

1. Читает 10 CSV-файлов из папки `исходные данные`.
2. Загружает 10000 строк в PostgreSQL-таблицу `mock_data`.
3. Создает в PostgreSQL таблицы `dim_customer`, `dim_seller`, `dim_supplier`, `dim_product`, `dim_store`, `dim_date`, `fact_sales`.
4. Создает в ClickHouse таблицы `report_product_sales`, `report_customer_sales`, `report_time_sales`, `report_store_sales`, `report_supplier_sales`, `report_product_quality`.

## Подключения

PostgreSQL:

```text
host: localhost
port: 55433
database: pet_sales
user: spark
password: spark
```

ClickHouse HTTP:

```text
host: localhost
port: 8124
database: pet_sales_reports
user: spark
password: spark
```

ClickHouse native:

```text
host: localhost
port: 9002
database: pet_sales_reports
user: spark
password: spark
```

## Проверка

Проверить PostgreSQL:

```bash
docker compose exec -T postgres psql -U spark -d pet_sales -c "select count(*) as raw_rows from mock_data; select count(*) as facts from fact_sales; select sum(sale_total_price) as fact_revenue from fact_sales;"
```

Проверить ClickHouse:

```bash
docker compose exec -T clickhouse clickhouse-client --user spark --password spark --database pet_sales_reports --query "select 'report_product_sales' as table_name, count(*) as rows from report_product_sales union all select 'report_customer_sales', count(*) from report_customer_sales union all select 'report_time_sales', count(*) from report_time_sales union all select 'report_store_sales', count(*) from report_store_sales union all select 'report_supplier_sales', count(*) from report_supplier_sales union all select 'report_product_quality', count(*) from report_product_quality"
```

Дополнительные SQL-запросы для ручной проверки лежат в `sql/check_postgres.sql` и `sql/check_clickhouse.sql`.

## Чеклист готовности

1. В репозитории есть 10 исходных CSV-файлов по 1000 записей.
2. Есть `docker-compose.yml` с PostgreSQL, Spark и ClickHouse.
3. Есть инструкция запуска Spark job.
4. Есть Spark-код трансформации исходных данных в модель звезда в PostgreSQL: `jobs/run_etl.py`.
5. Есть Spark-код построения 6 отчетов в ClickHouse: `jobs/run_etl.py`.
6. PostgreSQL после запуска содержит `mock_data` на 10000 строк и `fact_sales` на 10000 строк.
7. ClickHouse после запуска содержит 6 непустых таблиц отчетов.
8. Cassandra, Neo4j, MongoDB и Valkey не входят в реализованную обязательную часть.
