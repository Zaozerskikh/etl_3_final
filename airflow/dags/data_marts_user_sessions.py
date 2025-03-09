from datetime import datetime, timedelta
import logging
import os
import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


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
    "analytics_data_mart",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    catchup=False
)


def create_daily_summary_dm():
    """
    Создание витрины "dm_daily_session_summary" – агрегированная сводка по датам:
    - Общее число сессий,
    - Число уникальных пользователей,
    - Средняя продолжительность сессии (в секундах).

    Данные агрегируются из всех разделённых таблиц (partition) сессий, созданных ETL-процессом.
    """
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

        create_dm_table_sql = """
            CREATE TABLE IF NOT EXISTS dm_daily_session_summary (
                summary_date DATE PRIMARY KEY,
                total_sessions INTEGER,
                unique_users INTEGER,
                avg_duration_seconds NUMERIC
            );
        """
        cursor.execute(create_dm_table_sql)
        logging.info("Таблица dm_daily_session_summary создана или уже существует.")

        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' AND tablename LIKE 'rep_usersessions_%';
        """)
        partitions = [row[0] for row in cursor.fetchall()]
        if not partitions:
            logging.info("Не найдены разделённые таблицы rep_usersessions")
            return

        daily_summary = {}

        for partition in partitions:
            query = f"""
                SELECT partition_date, 
                       COUNT(*) AS total_sessions, 
                       COUNT(DISTINCT (data->>'user_id')) AS unique_users, 
                       SUM(EXTRACT(EPOCH FROM ((data->>'end_time')::timestamp - (data->>'start_time')::timestamp))) AS total_duration
                FROM {partition}
                GROUP BY partition_date;
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                summary_date, sessions, users, total_duration = row
                # Если по данной дате уже есть данные, суммируем (обычно дата появляется в одной partition)
                if summary_date in daily_summary:
                    daily_summary[summary_date]['total_sessions'] += sessions
                    daily_summary[summary_date]['unique_users'] += users
                    daily_summary[summary_date]['total_duration'] += total_duration
                else:
                    daily_summary[summary_date] = {
                        'total_sessions': sessions,
                        'unique_users': users,
                        'total_duration': total_duration
                    }

        for summary_date, data in daily_summary.items():
            avg_duration = data['total_duration'] / data['total_sessions'] if data['total_sessions'] > 0 else 0
            insert_sql = """
                INSERT INTO dm_daily_session_summary (summary_date, total_sessions, unique_users, avg_duration_seconds)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (summary_date)
                DO UPDATE SET total_sessions = EXCLUDED.total_sessions,
                              unique_users = EXCLUDED.unique_users,
                              avg_duration_seconds = EXCLUDED.avg_duration_seconds;
            """
            cursor.execute(insert_sql, (summary_date, data['total_sessions'], data['unique_users'], avg_duration))
            logging.info(f"Обновлена сводка за дату {summary_date}")

        cursor.close()
        conn.close()
        logging.info("Витрина dm_daily_session_summary сформирована успешно.")
    except Exception as e:
        logging.error("Ошибка при формировании витрины dm_daily_session_summary: %s", e)
        raise


def create_device_usage_dm():
    """
    Создание витрины "dm_device_usage" – агрегированная сводка использования устройств:
    - Для каждого устройства подсчитывается количество сессий.

    Данные агрегируются из всех разделённых таблиц сессий.
    """
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

        create_dm_table_sql = """
            CREATE TABLE IF NOT EXISTS dm_device_usage (
                device_type TEXT,
                browser TEXT,
                sessions_count INTEGER,
                PRIMARY KEY (device_type, browser)
            );
        """
        cursor.execute(create_dm_table_sql)
        logging.info("Таблица dm_device_usage создана или уже существует.")

        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' AND tablename LIKE 'rep_usersessions_%';
        """)
        partitions = [row[0] for row in cursor.fetchall()]
        if not partitions:
            logging.info("Не найдены разделённые таблицы rep_usersessions")
            return

        device_usage = {}

        for partition in partitions:
            query = f"""
                SELECT data->>'device' AS device, COUNT(*) AS sessions_count
                FROM {partition}
                GROUP BY device;
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                device, count = row

                if 'Mobile' in device or 'Android' in device or 'iPhone' in device:
                    device_type = 'Mobile'
                else:
                    device_type = 'Desktop'

                if 'Chrome' in device:
                    browser = 'Chrome'
                elif 'Firefox' in device:
                    browser = 'Firefox'
                elif 'Safari' in device:
                    browser = 'Safari'
                elif 'Opera' in device:
                    browser = 'Opera'
                else:
                    browser = 'Other'

                key = (device_type, browser)
                if key in device_usage:
                    device_usage[key] += count
                else:
                    device_usage[key] = count

        for (device_type, browser), sessions_count in device_usage.items():
            insert_sql = """
                INSERT INTO dm_device_usage (device_type, browser, sessions_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (device_type, browser)
                DO UPDATE SET sessions_count = EXCLUDED.sessions_count;
            """
            cursor.execute(insert_sql, (device_type, browser, sessions_count))
            logging.info(f"Обновлены данные по устройству {device_type}, браузеру {browser}")

        cursor.close()
        conn.close()
        logging.info("Витрина dm_device_usage сформирована успешно.")
    except Exception as e:
        logging.error("Ошибка при формировании витрины dm_device_usage: %s", e)
        raise


daily_summary_task = PythonOperator(
    task_id="create_daily_summary_dm",
    python_callable=create_daily_summary_dm,
    dag=dag
)


device_usage_task = PythonOperator(
    task_id="create_device_usage_dm",
    python_callable=create_device_usage_dm,
    dag=dag
)