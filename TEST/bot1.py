import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
import psycopg2
import psycopg2.pool
import certifi
import os
os.environ['SSL_CERT_FILE'] = certifi.where()


# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = "8142838834:AAEA42xudSOnnqaIZX6PjT77-VGcLDDYW04"

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
        logger.info("PostgreSQL connection pool created for bot")
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    with open('static/CARDS.jpg', 'rb') as photo:
        await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=photo,
            caption="👋 Привет! Для привязки аккаунта используйте команду:\n"
                    "/link <ваш_код_карты>\n\n"
                    "Пример: /link 000,00000"
        )

async def link_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Привязать Telegram аккаунт к карте"""
    user_id = update.effective_user.id
    args = context.args
    conn = None
    
    if not args:
        await update.message.reply_text("❗ Укажите код карты после команды")
        return
    
    card_code = ' '.join(args).strip()
    
    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("⚠️ Ошибка подключения к базе данных")
            return
        
        logger.info("Linking attempt: %s -> %s", user_id, card_code)
        
        with conn.cursor() as cursor:
            # 1. Проверить существующую привязку пользователя
            cursor.execute("""
                SELECT card_code 
                FROM telegram_bindings 
                WHERE telegram_id = %s
            """, (user_id,))
            existing_binding = cursor.fetchone()
            
            if existing_binding:
                if existing_binding[0] == card_code:
                    await update.message.reply_text("ℹ️ Эта карта уже привязана к вашему аккаунту")
                else:
                    await update.message.reply_text(
                        f"❌ Ваш аккаунт уже привязан к карте: {existing_binding[0]}\n"
                        "Для смены обратитесь к администратору"
                    )
                return

            # 2. Проверить привязку карты к другим пользователям
            cursor.execute("""
                SELECT telegram_id 
                FROM telegram_bindings 
                WHERE card_code = %s
            """, (card_code,))
            card_owner = cursor.fetchone()
            
            if card_owner:
                await update.message.reply_text(
                    "❌ Эта карта уже привязана к другому аккаунту\n"
                    "Для перепривязки обратитесь к администратору"
                )
                return

            # 3. Получить ID карты из таблицы pass (только чтение, без изменения)
            cursor.execute("""
                SELECT id
                FROM pass 
                WHERE code = %s
            """, (card_code,))
            pass_row = cursor.fetchone()
            
            if not pass_row:
                await update.message.reply_text("❌ Карта не найдена в системе")
                return
                
            pass_id = pass_row[0]

            # 4. Создать новую привязку
            cursor.execute("""
                INSERT INTO telegram_bindings (pass_id, card_code, telegram_id)
                VALUES (%s, %s, %s)
            """, (pass_id, card_code, user_id))
            
            if cursor.rowcount > 0:
                logger.info("Successfully linked: %s -> %s", user_id, card_code)
                await update.message.reply_text("✅ Аккаунт успешно привязан!")
            else:
                logger.warning("Binding failed for: %s", card_code)
                await update.message.reply_text("❌ Ошибка привязки")

            conn.commit()
            
    except psycopg2.Error as e:
        logger.error("Database error: %s", str(e), exc_info=True)
        await update.message.reply_text("⚠️ Ошибка базы данных, попробуйте позже")
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error("General error: %s", str(e), exc_info=True)
        await update.message.reply_text("⚠️ Ошибка привязки аккаунта")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)

def main():
    """Основная функция запуска бота"""
    # Инициализация пула соединений
    init_db_pool()
    
    # Создание приложения
    application = Application.builder().token(TOKEN).build()
    
    # Регистрация обработчиков
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("link", link_account))
    
    logger.info("Bot started")
    application.run_polling()

if __name__ == '__main__':
    main()