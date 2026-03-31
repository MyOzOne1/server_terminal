import logging
import requests
import psycopg2
import psycopg2.pool
from flask import Flask, request, jsonify

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Настройки MAX API
MAX_API_TOKEN = "ваш_токен_бота_max"
MAX_API_URL = "https://api.max-messenger.com/bot"  # пример URL

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

# Инициализация Flask приложения
app = Flask(__name__)

def init_db_pool():
    """Инициализация пула соединений с PostgreSQL"""
    global postgres_pool
    try:
        postgres_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10, **POSTGRES_CONFIG
        )
        logger.info("PostgreSQL connection pool created for MAX bot")
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

def send_message(chat_id, text, photo_path=None):
    """Отправка сообщения через MAX API"""
    url = f"{MAX_API_URL}{MAX_API_TOKEN}/sendMessage"
    
    if photo_path:
        # Если нужно отправить фото
        url = f"{MAX_API_URL}{MAX_API_TOKEN}/sendPhoto"
        try:
            with open(photo_path, 'rb') as photo:
                files = {'photo': photo}
                data = {'chat_id': chat_id, 'caption': text}
                response = requests.post(url, files=files, data=data)
                return response.json()
        except Exception as e:
            logger.error("Error sending photo: %s", e)
            return None
    else:
        # Отправка текстового сообщения
        data = {'chat_id': chat_id, 'text': text}
        response = requests.post(url, data=data)
        return response.json()

def handle_start(chat_id):
    """Обработчик команды /start"""
    message = ("👋 Привет! Для привязки аккаунта используйте команду:\n"
              "/link <ваш_код_карты>\n\n"
              "Пример: /link 000,00000")
    
    # Отправка сообщения с фото
    send_message(chat_id, message, 'static/CARDS.jpg')

def handle_link(chat_id, user_id, card_code):
    """Привязать MAX аккаунт к карте"""
    conn = None
    
    try:
        conn = get_db_connection()
        if not conn:
            send_message(chat_id, "⚠️ Ошибка подключения к базе данных")
            return
        
        logger.info("Linking attempt: %s -> %s", user_id, card_code)
        
        with conn.cursor() as cursor:
            # 1. Проверить существующую привязку пользователя
            cursor.execute("""
                SELECT card_code 
                FROM max_bindings 
                WHERE max_id = %s
            """, (user_id,))
            existing_binding = cursor.fetchone()
            
            if existing_binding:
                if existing_binding[0] == card_code:
                    send_message(chat_id, "ℹ️ Эта карта уже привязана к вашему аккаунту")
                else:
                    send_message(chat_id,
                        f"❌ Ваш аккаунт уже привязан к карте: {existing_binding[0]}\n"
                        "Для смены обратитесь к администратору"
                    )
                return

            # 2. Проверить привязку карты к другим пользователям
            cursor.execute("""
                SELECT max_id 
                FROM max_bindings 
                WHERE card_code = %s
            """, (card_code,))
            card_owner = cursor.fetchone()
            
            if card_owner:
                send_message(chat_id,
                    "❌ Эта карта уже привязана к другому аккаунту\n"
                    "Для перепривязки обратитесь к администратору"
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
                send_message(chat_id, "❌ Карта не найдена в системе")
                return
                
            pass_id = pass_row[0]

            # 4. Создать новую привязку
            cursor.execute("""
                INSERT INTO max_bindings (pass_id, card_code, max_id)
                VALUES (%s, %s, %s)
            """, (pass_id, card_code, user_id))
            
            if cursor.rowcount > 0:
                logger.info("Successfully linked: %s -> %s", user_id, card_code)
                send_message(chat_id, "✅ Аккаунт успешно привязан!")
            else:
                logger.warning("Binding failed for: %s", card_code)
                send_message(chat_id, "❌ Ошибка привязки")

            conn.commit()
            
    except psycopg2.Error as e:
        logger.error("Database error: %s", str(e), exc_info=True)
        send_message(chat_id, "⚠️ Ошибка базы данных, попробуйте позже")
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error("General error: %s", str(e), exc_info=True)
        send_message(chat_id, "⚠️ Ошибка привязки аккаунта")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

@app.route('/webhook', methods=['POST'])
def webhook():
    """Обработчик входящих сообщений от MAX"""
    data = request.json
    
    # Предполагаемая структура сообщения от MAX
    chat_id = data.get('chat', {}).get('id')
    user_id = data.get('from', {}).get('id')
    text = data.get('text', '').strip()
    
    if not text:
        return jsonify({'status': 'error', 'message': 'No text provided'})
    
    # Обработка команд
    if text.startswith('/start'):
        handle_start(chat_id)
    elif text.startswith('/link'):
        parts = text.split()
        if len(parts) < 2:
            send_message(chat_id, "❗ Укажите код карты после команды")
        else:
            card_code = ' '.join(parts[1:])
            handle_link(chat_id, user_id, card_code)
    else:
        send_message(chat_id, "Неизвестная команда. Используйте /start или /link")
    
    return jsonify({'status': 'ok'})

def main():
    """Основная функция запуска бота"""
    # Инициализация пула соединений
    init_db_pool()
    
    logger.info("MAX Bot started")
    
    # Запуск Flask приложения
    app.run(host='0.0.0.0', port=5000)

if __name__ == '__main__':
    main()