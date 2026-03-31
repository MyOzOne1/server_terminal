import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """Создание подключения к базе SQLite"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def merge_tables():
    source_conn = create_connection('db_users_copy.db')
    target_conn = create_connection('db_users.db')
    
    if source_conn and target_conn:
        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()

        target_cur.execute("DROP TABLE IF EXISTS merged_users")
        
        create_table_sql = """
        CREATE TABLE merged_users (
            -- Users (31 колонок)
            users_id INTEGER,
            login TEXT,
            password TEXT,
            first_name TEXT,
            last_name TEXT,
            middle_name TEXT,
            gender TEXT,
            birthday TEXT,
            phone TEXT,
            email TEXT,
            work_place TEXT,
            services TEXT,
            deleted_by_user_id INTEGER,
            created_by INTEGER,
            fired_at TEXT,
            users_deleted_at TEXT,
            remember_token TEXT,
            is_blocked INTEGER,
            users_source TEXT,
            person_id INTEGER,
            users_created_at TEXT,
            users_updated_at TEXT,
            personnel_number TEXT,
            uuid TEXT,
            recognize_status TEXT,
            consent_to_date TEXT,
            is_online INTEGER,
            is_allowed_issue_permanent_pass INTEGER,
            is_foreign_citizen INTEGER,
            old_id INTEGER,
            extra_data TEXT,
            
            -- Pass (19 колонок)
            pass_id INTEGER,
            created_user_id INTEGER,
            pass_user_id INTEGER,
            type TEXT,
            code TEXT,
            code_format TEXT,
            valid_from TEXT,
            valid_to TEXT,
            block_from TEXT,
            block_to TEXT,
            is_active INTEGER,
            external_service_id INTEGER,
            pass_created_at TEXT,
            pass_updated_at TEXT,
            pass_deleted_at TEXT,
            pass_source TEXT,
            sub_type TEXT,
            status TEXT,
            return_at TEXT,
            
            PRIMARY KEY (users_id, pass_id)
        );
        """
        
        try:
            target_cur.execute(create_table_sql)
            target_conn.commit()
        except Error as e:
            print(f"Ошибка при создании таблицы: {e}")
            return

        select_sql = """
        SELECT 
            /* Users (31 колонка) */
            u.id,
            u.login,
            u.password,
            u.first_name,
            u.last_name,
            u.middle_name,
            u.gender,
            u.birthday,
            u.phone,
            u.email,
            u.work_place,
            u.services,
            u.deleted_by_user_id,
            u.created_by,
            u.fired_at,
            u.deleted_at,
            u.remember_token,
            u.is_blocked,
            u.source,
            u.person_id,
            u.created_at,
            u.updated_at,
            u.personnel_number,
            u.uuid,
            u.recognize_status,
            u.consent_to_date,
            u.is_online,
            u.is_allowed_issue_permanent_pass,
            u.is_foreign_citizen,
            u.old_id,
            u.extra_data,
            
            /* Pass (19 колонок) */
            p.id,
            p.created_user_id,
            p.user_id,
            p.type,
            p.code,
            p.code_format,
            p.valid_from,
            p.valid_to,
            p.block_from,
            p.block_to,
            p.is_active,
            p.external_service_id,
            p.created_at,
            p.updated_at,
            p.deleted_at,
            p.source,
            p.sub_type,
            p.status,
            p.return_at
            
        FROM users u
        LEFT JOIN pass p ON u.id = p.user_id
        """

        source_cur.execute(select_sql)
        total = 0
        
        placeholders = ', '.join(['?'] * 50)
        insert_sql = f"INSERT INTO merged_users VALUES ({placeholders})"

        while True:
            batch = source_cur.fetchmany(1000)
            if not batch:
                break
                
            try:
                target_cur.executemany(insert_sql, batch)
                target_conn.commit()
                total += len(batch)
                print(f"Перенесено записей: {total}", end='\r')
            except Error as e:
                print(f"\nОшибка при вставке: {e}")
                print("Проблемная запись:", batch[0]) 
                continue

        print(f"\nОбъединение завершено. Всего записей: {total}")
        
        target_cur.execute("SELECT COUNT(*) FROM merged_users")
        print(f"Проверка: в merged_users {target_cur.fetchone()[0]} записей")
        
        source_conn.close()
        target_conn.close()
    else:
        print("Ошибка подключения к базам данных")

if __name__ == '__main__':
    merge_tables()