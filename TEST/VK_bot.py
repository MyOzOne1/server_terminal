import logging
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import psycopg2
import psycopg2.pool
import random
import string

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Настройки VK
VK_TOKEN = "ваш_токен_группы_vk"
GROUP_ID = "ваш_id_группы"

# Настройки PostgreSQL
POSTGRES_CONFIG = {
    'host': '192.168.211.224',
    'port': 5430,
    'database': 'test',
    'user': 'postgres',
    'password': 'postgres'
}

# Пул соединений PostgreSQL
postgres_pool = None

def init_db_pool():
    """Инициализация пула соединений с PostgreSQL"""
    global postgres_pool
    try:
        postgres_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, **POSTGRES_CONFIG
        )
        logger.info("PostgreSQL connection pool created for VK bot")
    except Exception as e:
        logger.error("Error creating PostgreSQL connection pool: %s", e)

def get_db_connection():
    """Получить соединение с PostgreSQL из пула"""
    try:
        return postgres_pool.getconn()
    except Exception as e:
        logger.error("Error getting DB connection: %s", e)
        return None

def release_db_connection(conn):
    """Вернуть соединение в пул"""
    try:
        postgres_pool.putconn(conn)
    except Exception as e:
        logger.error("Error releasing DB connection: %s", e)

def generate_verification_code(length=6):
    """Генерация проверочного кода"""
    return ''.join(random.choices(string.digits, k=length))

def handle_start(user_id, vk):
    """Обработчик команды начала работы"""
    message = (
        "👋 Привет! Я бот для двухфакторной аутентификации.\n\n"
        "Для привязки аккаунта используйте команду:\n"
        "привязать <ваш_код_карты>\n\n"
        "Пример: привязать 000,00000\n\n"
        "Для получения кода авторизации:\n"
        "код"
    )
    vk.messages.send(
        user_id=user_id,
        message=message,
        random_id=random.randint(1, 1000000)
    )

def handle_link_account(user_id, card_code, vk):
    """Привязать VK аккаунт к карте"""
    conn = None
    
    if not card_code:
        vk.messages.send(
            user_id=user_id,
            message="❗ Укажите код карты после команды",
            random_id=random.randint(1, 1000000)
        )
        return
    
    try:
        conn = get_db_connection()
        if not conn:
            vk.messages.send(
                user_id=user_id,
                message="⚠️ Ошибка подключения к базе данных",
                random_id=random.randint(1, 1000000)
            )
            return
        
        logger.info("Linking attempt: %s -> %s", user_id, card_code)
        
        with conn.cursor() as cursor:
            # 1. Проверить существующую привязку пользователя
            cursor.execute("""
                SELECT card_code 
                FROM vk_bindings 
                WHERE vk_id = %s
            """, (user_id,))
            existing_binding = cursor.fetchone()
            
            if existing_binding:
                if existing_binding[0] == card_code:
                    vk.messages.send(
                        user_id=user_id,
                        message="ℹ️ Эта карта уже привязана к вашему аккаунту",
                        random_id=random.randint(1, 1000000)
                    )
                else:
                    vk.messages.send(
                        user_id=user_id,
                        message=f"❌ Ваш аккаунт уже привязан к карте: {existing_binding[0]}\nДля смены обратитесь к администратору",
                        random_id=random.randint(1, 1000000)
                    )
                return

            # 2. Проверить привязку карты к другим пользователям
            cursor.execute("""
                SELECT vk_id 
                FROM vk_bindings 
                WHERE card_code = %s
            """, (card_code,))
            card_owner = cursor.fetchone()
            
            if card_owner:
                vk.messages.send(
                    user_id=user_id,
                    message="❌ Эта карта уже привязана к другому аккаунту\nДля перепривязки обратитесь к администратору",
                    random_id=random.randint(1, 1000000)
                )
                return

            # 3. Получить ID карты из таблицы pass
            cursor.execute("""
                SELECT id
                FROM pass 
                WHERE code = %s
            """, (card_code,))
            pass_row = cursor.fetchone()
            
            if not pass_row:
                vk.messages.send(
                    user_id=user_id,
                    message="❌ Карта не найдена в системе",
                    random_id=random.randint(1, 1000000)
                )
                return
                
            pass_id = pass_row[0]

            # 4. Создать новую привязку
            cursor.execute("""
                INSERT INTO vk_bindings (pass_id, card_code, vk_id)
                VALUES (%s, %s, %s)
            """, (pass_id, card_code, user_id))
            
            if cursor.rowcount > 0:
                logger.info("Successfully linked: %s -> %s", user_id, card_code)
                vk.messages.send(
                    user_id=user_id,
                    message="✅ Аккаунт успешно привязан!",
                    random_id=random.randint(1, 1000000)
                )
            else:
                logger.warning("Binding failed for: %s", card_code)
                vk.messages.send(
                    user_id=user_id,
                    message="❌ Ошибка привязки",
                    random_id=random.randint(1, 1000000)
                )

            conn.commit()
            
    except psycopg2.Error as e:
        logger.error("Database error: %s", str(e), exc_info=True)
        vk.messages.send(
            user_id=user_id,
            message="⚠️ Ошибка базы данных, попробуйте позже",
            random_id=random.randint(1, 1000000)
        )
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error("General error: %s", str(e), exc_info=True)
        vk.messages.send(
            user_id=user_id,
            message="⚠️ Ошибка привязки аккаунта",
            random_id=random.randint(1, 1000000)
        )
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def handle_get_code(user_id, vk):
    """Отправка проверочного кода"""
    conn = None
    
    try:
        conn = get_db_connection()
        if not conn:
            vk.messages.send(
                user_id=user_id,
                message="⚠️ Ошибка подключения к базе данных",
                random_id=random.randint(1, 1000000)
            )
            return
        
        with conn.cursor() as cursor:
            # Проверить, привязан ли аккаунт
            cursor.execute("""
                SELECT card_code 
                FROM vk_bindings 
                WHERE vk_id = %s
            """, (user_id,))
            binding = cursor.fetchone()
            
            if not binding:
                vk.messages.send(
                    user_id=user_id,
                    message="❌ Ваш аккаунт не привязан к карте\nИспользуйте команду: привязать <код_карты>",
                    random_id=random.randint(1, 1000000)
                )
                return
            
            # Генерация и отправка кода
            verification_code = generate_verification_code()
            
            # Сохранить код в базу (можно создать отдельную таблицу для кодов)
            cursor.execute("""
                INSERT INTO verification_codes (vk_id, code, created_at)
                VALUES (%s, %s, NOW())
            """, (user_id, verification_code))
            
            conn.commit()
            
            # Отправить код пользователю
            vk.messages.send(
                user_id=user_id,
                message=f"🔐 Ваш проверочный код: {verification_code}\nКод действителен 5 минут",
                random_id=random.randint(1, 1000000)
            )
            
    except Exception as e:
        logger.error("Error generating code: %s", str(e), exc_info=True)
        vk.messages.send(
            user_id=user_id,
            message="⚠️ Ошибка генерации кода",
            random_id=random.randint(1, 1000000)
        )
    finally:
        if conn:
            release_db_connection(conn)

def main():
    """Основная функция запуска бота"""
    # Инициализация пула соединений
    init_db_pool()
    
    # Инициализация VK API
    vk_session = vk_api.VkApi(token=VK_TOKEN)
    vk = vk_session.get_api()
    longpoll = VkBotLongPoll(vk_session, GROUP_ID)
    
    logger.info("VK Bot started")
    
    # Основной цикл обработки сообщений
    for event in longpoll.listen():
        if event.type == VkBotEventType.MESSAGE_NEW:
            message = event.object.message
            user_id = message['from_id']
            text = message['text'].lower().strip()
            
            logger.info("Received message from %s: %s", user_id, text)
            
            if text.startswith('привет') or text.startswith('start') or text.startswith('/start'):
                handle_start(user_id, vk)
            elif text.startswith('привязать'):
                card_code = text.replace('привязать', '').strip()
                handle_link_account(user_id, card_code, vk)
            elif text.startswith('код'):
                handle_get_code(user_id, vk)
            else:
                vk.messages.send(
                    user_id=user_id,
                    message="❓ Неизвестная команда. Используйте:\n- привязать <код_карты>\n- код",
                    random_id=random.randint(1, 1000000)
                )

if __name__ == '__main__':
    main()
