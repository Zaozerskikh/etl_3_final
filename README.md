# ETL 3 FINAL

## Контейнеры

### 1. **MongoDB**
   - Исходная NoSQL БД.
   - Доступна на порту `27017`.
   - Данные лежат в volume `mongo_data`.

### 2. **PostgreSQL**
   - Реляционная база данных для хранения результата работы Airflow пайплайнов.
   - Доступна на порту `5432`.
   - Данные лежат в volume `pg_data`.
   - Креды:
     - Логин: `user`
     - Пароль: `password`
     - База данных: `mydatabase`

### 3. **pgAdmin**
   - UI для PostgreSQL.
   - Доступен по адресу `http://localhost:5051`.
   - Данные лежат в volume `pg_admin_data`.
   - Креды:
     - Email: `admin@admin.com`
     - Пароль: `admin`

### 4. **Генератор данных (faker)**
   - Генерирует тестовые данные и кладет их в MongoDB.
   - Запускается после старта MongoDB, отрабатывает 1 раз по дефотлу.
   - Использует подключение:  
     `MONGO_URI=mongodb://mongo:27017/`

### 5. **Airflow**
   - UI доступен по адресу `http://localhost:8080`.
   - Работает в режиме `LocalExecutor`.
   - DAGs хранятся в `./airflow/dags`.
   - Использует следующие параметры:
     - Подключение к MongoDB:  
       `MONGO_URI=mongodb://mongo:27017/`
     - Подключение к PostgreSQL:  
       `postgresql+psycopg2://user:password@postgres/mydatabase`
   - Креды:
     - Логин: `usr`
     - Пароль: `usr`


## Запуск проекта

   ```sh
   git clone https://github.com/Zaozerskikh/etl_3_final
   cd etl_3_final
   docker-compose up -d --build
