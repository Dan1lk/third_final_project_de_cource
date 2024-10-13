from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, count, round, when
from pyspark.sql.types import DoubleType, IntegerType, DateType, BooleanType
import zipfile
import urllib.request
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'main',
    default_args=default_args,
    description='A simple DAG to interact with PostgreSQL with postgresql-42.7.3.jar',
    schedule_interval=None,
)

def extract_data():
    # Скачиваем файл и достаем данные из архива
    if not os.path.exists('Аэропорты'):
        url = 'https://getfile.dokpub.com/yandex/get/https://disk.yandex.ru/d/TXZjQ6bbuWCo_g'
        urllib.request.urlretrieve(url, filename="zip_file")

        with zipfile.ZipFile("zip_file", 'r') as zf:
            zf.extractall()

        # Удаляем ненужные файлы и папки
        os.remove('zip_file')

        print('Файл загружен')

def pyspark_job(**context):

    #Создаем подключение к спарку
    spark = SparkSession.builder.appName('MySparkApp') \
        .master('local[*]') \
        .getOrCreate()

    # Загрузка данных в dataframe
    airlines_df = spark.read.csv('Аэропорты/airlines.csv', header=True, inferSchema=True)
    airports_df = spark.read.csv('Аэропорты/airports.csv', header=True, inferSchema=True)
    flights_pak_df = spark.read.csv('Аэропорты/flights_pak.csv', header=True, inferSchema=True)
    print(f'Общее количество строк в airlines_df : {airlines_df.count()}')
    print(f'Общее количество строк в airports_df : {airports_df.count()}')
    print(f'Общее количество строк в flights_pak_df : {flights_pak_df.count()}')

    # Преобразуем столбцы в формат даты и чисел

    # Для airpors_df:
    airports_df = airports_df.withColumn('Latitude', airports_df.Latitude.cast(DoubleType()))
    airports_df = airports_df.withColumn('Longitude', airports_df.Latitude.cast(DoubleType()))

    # Для flights_pak_df:
    flights_pak_df = flights_pak_df.withColumn('DATE', flights_pak_df.DATE.cast(DateType()))
    flights_pak_df = flights_pak_df.withColumn('FLIGHT_NUMBER', flights_pak_df.FLIGHT_NUMBER.cast(IntegerType()))
    flights_pak_df = flights_pak_df.withColumn('DEPARTURE_DELAY', flights_pak_df.DEPARTURE_DELAY.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('ARRIVAL_DELAY', flights_pak_df.ARRIVAL_DELAY.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('DIVERTED', flights_pak_df.DIVERTED.cast(BooleanType()))
    flights_pak_df = flights_pak_df.withColumn('CANCELLED', flights_pak_df.CANCELLED.cast(BooleanType()))
    flights_pak_df = flights_pak_df.withColumn('AIR_SYSTEM_DELAY', flights_pak_df.AIR_SYSTEM_DELAY.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('SECURITY_DELAY', flights_pak_df.SECURITY_DELAY.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('AIRLINE_DELAY', flights_pak_df.AIRLINE_DELAY.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('LATE_AIRCRAFT_DELAY', flights_pak_df.LATE_AIRCRAFT_DELAY.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('WEATHER_DELAY', flights_pak_df.WEATHER_DELAY.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('DEPARTURE_HOUR', flights_pak_df.DEPARTURE_HOUR.cast(DoubleType()))
    flights_pak_df = flights_pak_df.withColumn('ARRIVAL_HOUR', flights_pak_df.ARRIVAL_HOUR.cast(DoubleType()))

    # Проверка схем датафреймов:
    print("Схемы датафреймов: ")

    airlines_df.printSchema()
    airports_df.printSchema()
    flights_pak_df.printSchema()


    # Средняя задержка рейса:
    flights_pak_df_with_avg_delay_fligth = flights_pak_df.withColumn('AVG_DELAY_FLIGTH', (flights_pak_df.DEPARTURE_DELAY + flights_pak_df.ARRIVAL_DELAY) / 2)

    # Топ 5 авиалиний с наибольшей средней задержкой:
    top5_airlines_with_highest_average_delay = flights_pak_df_with_avg_delay_fligth \
      .groupBy(col('AIRLINE').alias('AIRLINE_CODE')).agg(round(mean(col('AVG_DELAY_FLIGTH')), 2).alias('AVG_DELAY_AIRLINE'))

    # Джойним airlines_df и flights_pak_df_with_avg_delay_fligth и выбираем нужные столбцы:
    top5_airlines_with_highest_average_delay = airlines_df.join(top5_airlines_with_highest_average_delay, top5_airlines_with_highest_average_delay.AIRLINE_CODE
                                                                == airlines_df['IATA CODE'], 'inner') \
                                                                .select(col('AIRLINE'), col('AVG_DELAY_AIRLINE')) \
                                                                .orderBy(col('AVG_DELAY_AIRLINE'), ascending=False)

    print('Топ 5 авиалиний с наибольшей средней задержкой: ')
    top5_airlines_with_highest_average_delay.show(truncate=False)


    # Посчитаем количество отмененных рейсов для каждого аэропорта:
    number_of_cancelled_flights = flights_pak_df.filter(col('CANCELLED') == True) \
      .groupBy(col('ORIGIN_AIRPORT')).agg(count(col('CANCELLED')).alias('COUNT_CANCELLED'))

    # Найдем общее количество рейсов для каждого аэропорта:
    number_flights = flights_pak_df.groupBy(col('ORIGIN_AIRPORT')).agg(count(col('*')).alias('COUNT_FLIGHTS'))

    # Джойним две предыдущие датафреймы и выбираем нужные столбцы:
    percent_of_cancelled_flights_for_airport = number_of_cancelled_flights.join(number_flights, 'ORIGIN_AIRPORT', 'inner') \
      .select(col('ORIGIN_AIRPORT'), round((col('COUNT_CANCELLED') * 100) / col('COUNT_FLIGHTS'), 2).alias('PERCENT_OF_CANCELLED_FLIGHTS')) \
      .orderBy(col('PERCENT_OF_CANCELLED_FLIGHTS'), ascending=False)

    print('Процент отмененных рейсов для каждого аэропорта: ')
    percent_of_cancelled_flights_for_airport.show()


    # В какое время суток (утро, день, вечер, ночь) чаще всего связано с задержками рейсов:

    # Добавим столбец с временем суток в flights_pak_df:
    flights_pak_df = flights_pak_df.withColumn('DAY_PART', when((col('DEPARTURE_HOUR') > 3.0) & (col('DEPARTURE_HOUR') <= 9.0), 'Morning') \
                                                          .when((col('DEPARTURE_HOUR') > 9.0) & (col('DEPARTURE_HOUR') <= 15.0), 'Afternoon') \
                                                          .when((col('DEPARTURE_HOUR') > 15.0) & (col('DEPARTURE_HOUR') <= 21.0), 'Evening') \
                                                          .otherwise('Nigth')
                                              )
    # Посчитаем количество отмененных рейсов по времени суток:
    number_of_cancelled_flights_by_time_of_day = flights_pak_df.filter(col('CANCELLED') == True) \
      .groupBy(col('DAY_PART')).agg(count(col('CANCELLED')).alias('COUNT_CANCELLED_FLIGHTS'))

    # Найдем количество рейсов по времени суток:
    number_of_flights_by_time_of_day = flights_pak_df.groupBy(col('DAY_PART')).agg(count(col('*')).alias('COUNT_FLIGHTS'))

    # Джойним две предыдущие датафреймы и выбираем нужные столбцы:
    percent_of_cancelled_flights_by_time_of_day = number_of_cancelled_flights_by_time_of_day.join(number_of_flights_by_time_of_day, 'DAY_PART', 'inner') \
      .select(col('DAY_PART'), round((col('COUNT_CANCELLED_FLIGHTS') * 100) / col('COUNT_FLIGHTS'), 2) \
              .alias('PERCENT_OF_CANCELLED_FLIGHTS')).orderBy(col('PERCENT_OF_CANCELLED_FLIGHTS'), ascending=False)

    print('В какое время суток (утро, день, вечер, ночь) чаще всего связано с задержками рейсов: ')
    percent_of_cancelled_flights_by_time_of_day.show()
    print(f"Значит чаще всего отмен во время суток - {percent_of_cancelled_flights_by_time_of_day.collect()[0][0]}")


    # Добавим в таблицу флаг, указывающий, является ли рейс дальнемагистральным (если расстояние больше 1000 миль):
    flights_pak_df = flights_pak_df.withColumn('IS_LONG_HAUL', when(col('DISTANCE') > 1000.0, 1).otherwise(0))

    print('Пример получившихся данных: ')
    flights_pak_df.show()


    # Преобразовываем DataFrame в csv:
    flights_pak_df.write.csv('opt/airflow/xcom/flights_pak_df.csv', mode='overwrite', header=True)
    airports_df.write.csv('opt/airflow/xcom/airports_df.csv', mode='overwrite', header=True)

    # Добавляем данные в XCom для передачи их в task conn_and_load_clickhouse
    context['ti'].xcom_push(key='flights_pak_csv', value='opt/airflow/xcom/flights_pak_df.csv')
    context['ti'].xcom_push(key='airports_df_csv', value='opt/airflow/xcom/airports_df.csv')
    spark.stop()

def conn_and_load_postgresql(**context):
    # Забираем данные из XCom
    flights_pak_csv = context['ti'].xcom_pull(key='flights_pak_csv')
    airports_csv = context['ti'].xcom_pull(key='airports_df_csv')

    # Создаем спарк сессию с драйвером postgresql-42.7.3.jar
    spark = SparkSession.builder \
        .appName("PostgreSQL Connection with PySpark") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Получаем pyspark dataframe из csv файла
    flights_pak_df = spark.read.csv(flights_pak_csv, header=True, inferSchema=True)
    airports_df = spark.read.csv(airports_csv, header=True, inferSchema=True)

    # Загружаем 10000 строк flights_pak_df в postgresql
    try:

        flights_pak_df.limit(10000).write.mode("overwrite") \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
            .option("dbtable", "flights_pak") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .save()
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
    print("Данные загружены")

    # Общее время полетов для компаний:

    # Регистрируем временную таблицу:
    flights_pak_df.createOrReplaceTempView("flights_pak")
    airports_df.createOrReplaceTempView("airports")

    spark.sql("""
              WITH t1 AS (
    	            SELECT ORIGIN_AIRPORT, 
     	                CASE 
     		                WHEN ARRIVAL_HOUR >= DEPARTURE_HOUR THEN ARRIVAL_HOUR - DEPARTURE_HOUR
     		                ELSE ARRIVAL_HOUR - DEPARTURE_HOUR + 24.0
     	                END AS TIME_FLIGHTS
     	            FROM flights_pak)
              SELECT ORIGIN_AIRPORT, sum(TIME_FLIGHTS) AS TIME_FLIGHTS
              FROM t1
              GROUP BY ORIGIN_AIRPORT
            """).createOrReplaceTempView("time_flights")

    time_flights = spark.sql(
        """
        SELECT Airport, TIME_FLIGHTS
        FROM time_flights tf
            INNER JOIN airports a ON tf.ORIGIN_AIRPORT == a.`IATA CODE`
        ORDER BY TIME_FLIGHTS DESC   
        """
    )

    print("Общее время налета самолетов по аэропортам: ")
    time_flights.show(truncate=False)

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

task_query_pyspark = PythonOperator(
    task_id='query_pyspark',
    python_callable=pyspark_job,
    provide_context=True,
    dag=dag
)

task_conn_and_load_postgresql = PythonOperator(
    task_id='conn_and_load_clickhouse',
    python_callable=conn_and_load_postgresql,
    provide_context=True,
    dag=dag
)


task_extract_data >> task_query_pyspark >> task_conn_and_load_postgresql
