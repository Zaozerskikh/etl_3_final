from datetime import datetime, timedelta
import json
import logging
import os
from dateutil.parser import parse

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
import psycopg2


MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "mydatabase")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "replication_usersessions",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False
)

COLLECTION_NAME = "UserSessions"
UNIQUE_KEY = "session_id"
DATE_FIELD = "start_time"


def extract():
    """Извлечение данных из MongoDB для коллекции UserSessions."""
    client = MongoClient(MONGO_URI)
    mongo_db = client["mydatabase"]
    docs = list(mongo_db[COLLECTION_NAME].find())
    for doc in docs:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    logging.info(f"Из коллекции {COLLECTION_NAME} извлечено {len(docs)} документов")
    return docs


def transform(**kwargs):
    """
    Трансформация:
    - удаление дубликатов по полю UNIQUE_KEY,
    - добавление поля partition_date, полученного из DATE_FIELD.
    """
    ti = kwargs["ti"]
    docs = ti.xcom_pull(task_ids="extract")
    seen = set()
    transformed_docs = []
    for doc in docs:
        key_val = doc.get(UNIQUE_KEY)
        if key_val in seen:
            continue
        seen.add(key_val)
        date_val = doc.get(DATE_FIELD)
        if date_val:
            try:
                dt = parse(date_val) if isinstance(date_val, str) else date_val
                doc["partition_date"] = dt.strftime("%Y-%m-%d")
            except Exception as e:
                logging.error(f"Ошибка при обработке даты в документе {doc}: {e}")
                doc["partition_date"] = None
        else:
            doc["partition_date"] = None
        transformed_docs.append(doc)
    logging.info(f"После трансформации для {COLLECTION_NAME} осталось {len(transformed_docs)} документов")
    return transformed_docs


def load(**kwargs):
    ti = kwargs["ti"]
    docs = ti.xcom_pull(task_ids="transform") or []

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        table_name = "rep_" + COLLECTION_NAME.lower()

        def get_partition_table_name(date):
            return f"{table_name}_{date.year}_{date.month:02d}"

        for doc in docs:
            partition_date = doc.get("partition_date")
            if not partition_date:
                continue

            partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
            partition_table_name = get_partition_table_name(partition_date)

            # Check if the table exists
            cursor.execute(f"SELECT to_regclass('{partition_table_name}');")
            table_exists = cursor.fetchone()[0] is not None

            if not table_exists:
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {partition_table_name} (
                        id BIGSERIAL PRIMARY KEY,
                        session_id TEXT UNIQUE NOT NULL,
                        partition_date DATE NOT NULL,
                        data JSONB NOT NULL
                    );
                """
                cursor.execute(create_table_sql)
                logging.info(f"Создана таблица {partition_table_name}")

            insert_sql = f"""
                INSERT INTO {partition_table_name} (session_id, partition_date, data) 
                VALUES (%s, %s, %s)
                ON CONFLICT (session_id) 
                DO UPDATE SET partition_date = EXCLUDED.partition_date, data = EXCLUDED.data;
            """
            cursor.execute(insert_sql, [doc.get(UNIQUE_KEY), partition_date, json.dumps(doc, default=str)])

        cursor.close()
        conn.close()
        logging.info(f"Загружено {len(docs)} документов в разделённые таблицы")

    except Exception as e:
        logging.error("Ошибка при загрузке данных: %s", e)
        raise


def quality_check():
    """Проверка качества данных: поиск дубликатов по session_id."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        table_name = "rep_" + COLLECTION_NAME.lower()

        cursor.execute(f"""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' AND tablename LIKE '{table_name}_%';
        """)
        partitions = [row[0] for row in cursor.fetchall()]

        total_duplicates = 0
        for partition in partitions:
            cursor.execute(f"""
                SELECT session_id, COUNT(*) FROM {partition}
                GROUP BY session_id
                HAVING COUNT(*) > 1;
            """)
            duplicates = cursor.fetchall()
            if duplicates:
                total_duplicates += len(duplicates)
                for session_id, count in duplicates:
                    logging.error(f"Дубликат session_id: {session_id} встречается {count} раз в {partition}")

        cursor.close()
        conn.close()

        if total_duplicates > 0:
            raise ValueError(f"Найдено {total_duplicates} дубликатов session_id!")

        logging.info("Проверка качества данных прошла успешно, дубликаты не найдены.")

    except Exception as e:
        logging.error(f"Ошибка при проверке качества данных: {e}")
        raise


extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load,
    provide_context=True,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id="quality_check",
    python_callable=quality_check,
    dag=dag
)


extract_task >> transform_task >> load_task >> quality_check_task
