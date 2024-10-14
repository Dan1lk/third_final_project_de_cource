from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, count, round, when
from pyspark.sql.types import DoubleType, IntegerType, DateType, BooleanType
import os
import datetime
import random
import csv
from faker import Faker

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


#file_path = '/opt/files/sales.csv'
def data_generation():

    if not os.path.exists('/opt/files/sales.csv'):
        fake = Faker()
        num_records = 1000000
        regions = ['North', 'South', 'East', 'West']
        file_path = '/opt/files/sales.csv'
        start_date = datetime.date(year=2024, month=1, day=1)
        end_date = datetime.datetime.now()

        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['sale_id', 'customer_id', 'product_id', 'quantity', 'sale_date', 'sale_amount', 'region'])

            for i in range(1, num_records + 1):
                sale_id = i
                customer_id = fake.random_int(1, 100000)
                product_id = fake.random_int(1, 1000)
                quantity = fake.random_int(1, 20)
                sale_date = fake.date_between_dates(start_date, end_date)
                sale_amount = quantity * fake.random_int(1, 1000)
                region = random.choice(regions)
                row = [sale_id, customer_id, product_id, quantity, sale_date, sale_amount, region]
                writer.writerow(row)

def pyspark_job():

    #Создаем подключение к спарку
    spark = SparkSession.builder.appName('MySparkApp') \
        .master('local[*]') \
        .getOrCreate()

    # Загрузка данных в dataframe
    sales_df = spark.read.csv('/opt/files/sales.csv', header=True, inferSchema=True)
    print(f'Общее количество строк в sales_df : {sales_df.count()}')
    sales_df = sales_df.drop_duplicates(['customer_id', 'product_id', 'quantity', 'sale_date', 'sale_amount', 'region'])
    print(f'Общее количество строк после удаления дубликатов в sales_df : {sales_df.count()}')

    # Сохраняем очищенные данные в формат csv:
    sales_df.write.mode('overwrite').options(header='True').csv('/opt/files/clean_sales.csv')


def conn_and_load_postgresql():

    # Создаем спарк сессию с драйвером postgresql-42.7.3.jar
    spark = SparkSession.builder \
        .appName("PostgreSQL Connection with PySpark") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Получаем pyspark dataframe из csv файла
    clean_sales_df = spark.read.csv('/opt/files/clean_sales.csv', header=True, inferSchema=True)

    # Загружаем clean_sales_df в postgresql
    try:
        clean_sales_df.write.mode("overwrite") \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
            .option("dbtable", "clean_sales") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .save()
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
    print("Данные загружены")
    spark.stop()

def conn_and_extract_postgresql():

    # Создаем спарк сессию с драйвером postgresql-42.7.3.jar
    spark = SparkSession.builder \
        .appName("PostgreSQL Connection with PySpark") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Читаем clean_sales_df из postgresql

    sales_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
        .option("dbtable", "clean_sales") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    print("Данные прочитаны")

    # Смотрим на данные:
    sales_df.show()

    # Регистрируем временную таблицу:
    sales_df.createOrReplaceTempView('sales')

    # Подсчитаем общее количество продаж и сумму продаж для каждого региона и каждого продукта и средний чек (average_sale_amount) по регионам и продуктам.
    sales_by_region = spark.sql("""
                        SELECT region, product_id, count(*) as count_sales, sum(sale_amount) as sum_sale_amount, 
                            avg(sum(sale_amount)) OVER (PARTITION BY region) as avg_sale_amount
                        FROM sales
                        GROUP BY region, product_id
                    """)

    # Загрузим агрегированные данные в postgresql:
    sales_by_region.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
        .option("dbtable", "sales_by_region") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .save()

    print("Данные загружены")
    spark.stop()

def data_migration():

    # Создаем спарк сессию с драйвером postgresql-42.7.3.jar
    spark = SparkSession.builder \
        .appName("PostgreSQL Connection with PySpark") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()

    # Читаем sales_by_region_df из postgresql
    sales_by_region_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/test") \
        .option("dbtable", "sales_by_region") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    print("Данные прочитаны")

    # Сохраняем в виде csv на хосте:
    sales_by_region_df.write.mode('overwrite').options(header='True').csv('/opt/files/sales_by_region.csv')
    # Останавливаем спарк сессию
    spark.stop()

    # Создаем спарк сессию с драйвером clickhouse-data-0.7.0.jar
    spark = SparkSession.builder \
        .appName("PostgreSQL Connection with PySpark") \
        .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.7.0.jar") \
        .getOrCreate()

    # Получаем pyspark dataframe из csv файла
    sales_region_df = spark.read.csv('/opt/files/sales_by_region.csv', header=True, inferSchema=True)

    # Записываем в ClickHouse:
    sales_region_df.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://host.docker.internal:8123/default") \
        .option("dbtable", "sales_by_region") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .save()

    print("Данные загружены")
    spark.stop()

task_data_generation = PythonOperator(
    task_id='data_generation',
    python_callable=data_generation,
    dag=dag
)

task_query_pyspark = PythonOperator(
    task_id='query_pyspark',
    python_callable=pyspark_job,
    dag=dag
)

task_conn_and_load_postgresql = PythonOperator(
    task_id='conn_and_load_clickhouse',
    python_callable=conn_and_load_postgresql,
    dag=dag
)

task_conn_and_extract_postgresql = PythonOperator(
    task_id="conn_and_extract_postgresql",
    python_callable=conn_and_extract_postgresql,
    dag=dag
)

task_data_migration = PythonOperator(
    task_id="data_migration",
    python_callable=data_migration,
    dag=dag
)

task_data_generation >> task_query_pyspark >> task_conn_and_load_postgresql >> task_conn_and_extract_postgresql >> task_data_migration
