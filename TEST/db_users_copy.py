import psycopg2
import sqlite3
import logging
from tqdm import tqdm

# Настройка логгера
logging.basicConfig(
    filename='migration.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Параметры подключения к PostgreSQL
pg_config = {
    'host': '192.168.211.224',
    'port': 5430,
    'database': 'test',
    'user': 'postgres',
    'password': 'postgres'
}

# Параметры SQLite
sqlite_db = 'db_users_copy.db'

# Маппинг типов данных
TYPE_MAPPING = {
    'integer': 'INTEGER',
    'bigint': 'INTEGER',
    'smallint': 'INTEGER',
    'real': 'REAL',
    'double precision': 'REAL',
    'numeric': 'REAL',
    'text': 'TEXT',
    'character varying': 'TEXT',
    'varchar': 'TEXT',
    'char': 'TEXT',
    'date': 'TEXT',
    'timestamp': 'TEXT',
    'boolean': 'INTEGER',
    'json': 'TEXT',
    'jsonb': 'TEXT'
}

def get_pg_tables(conn):
    """Получение списка таблиц в PostgreSQL"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        return [row[0] for row in cur.fetchall()]

def create_sqlite_table(sqlite_cur, table_name, columns):
    """Создание таблицы в SQLite"""
    columns_def = ', '.join(
        [f'"{col[0]}" {TYPE_MAPPING.get(col[1], "TEXT")}' 
        for col in columns]
    )
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def})'
    sqlite_cur.execute(sql)

def migrate_table(pg_cur, sqlite_cur, table_name):
    """Перенос данных для одной таблицы"""
    try:
        # Получение структуры таблицы
        pg_cur.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        columns = pg_cur.fetchall()
        
        # Создание таблицы в SQLite
        create_sqlite_table(sqlite_cur, table_name, columns)
        
        # Перенос данных
        pg_cur.execute(f'SELECT * FROM "{table_name}"')
        rows = pg_cur.fetchall()
        
        # Подготовка плейсхолдеров
        placeholders = ', '.join(['?'] * len(columns))
        
        # Вставка данных с прогресс-баром
        success = 0
        errors = 0
        for row in tqdm(rows, desc=f"Таблица {table_name}"):
            try:
                sqlite_cur.execute(
                    f'INSERT INTO "{table_name}" VALUES ({placeholders})',
                    row
                )
                success += 1
            except sqlite3.Error as e:
                logging.error(f"Ошибка вставки в {table_name}: {e}")
                errors += 1
                continue
        
        logging.info(
            f"Таблица {table_name}: перенесено {success} записей, "
            f"ошибок: {errors}"
        )
        return True
        
    except Exception as e:
        logging.error(f"Ошибка миграции таблицы {table_name}: {e}")
        return False

def main():
    try:
        # Подключение к PostgreSQL
        pg_conn = psycopg2.connect(**pg_config)
        pg_cur = pg_conn.cursor()
        
        # Подключение к SQLite
        sqlite_conn = sqlite3.connect(sqlite_db)
        sqlite_cur = sqlite_conn.cursor()
        
        # Получение списка таблиц
        tables = get_pg_tables(pg_conn)
        logging.info(f"Найдено таблиц для переноса: {len(tables)}")
        
        # Перенос данных
        for table in tables:
            if migrate_table(pg_cur, sqlite_cur, table):
                sqlite_conn.commit()
        
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
    finally:
        # Закрытие соединений
        if 'pg_conn' in locals(): pg_conn.close()
        if 'sqlite_conn' in locals(): sqlite_conn.close()
        logging.info("Миграция завершена")

if __name__ == '__main__':
    main()