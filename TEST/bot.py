import sqlite3
import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = "8142838834:AAEA42xudSOnnqaIZX6PjT77-VGcLDDYW04"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with open('static\CARDS.jpg', 'rb') as photo:
        await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=photo,
            caption="👋 Привет! Для привязки аккаунта используйте команду:\n"
                    "/link <ваш_код_карты>\n\n"
                    "Пример: /link 000,00000"
        )

async def link_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = context.args
    conn = None
    
    if not args:
        await update.message.reply_text("❗ Укажите код карты после команды")
        return
    
    card_code = ' '.join(args).strip()
    
    try:
        conn = sqlite3.connect('db_users.db', timeout=20)
        cursor = conn.cursor()
        
        logger.info("Linking attempt: %s -> %s", user_id, card_code)
        
        with conn:
            # Проверка 1: Существующая привязка пользователя
            cursor.execute(
                "SELECT code FROM merged_users WHERE telegram_id = ?",
                (str(user_id),)
            )
            existing_link = cursor.fetchone() 
            if existing_link:
                await update.message.reply_text(
                    f"❌ Ваш аккаунт уже привязан к карте: {existing_link[0]}\n"
                    "Для смены обратитесь к администратору"
                )
                return

            # Проверка 2: Занятость карты
            cursor.execute(
                "SELECT telegram_id FROM merged_users WHERE code = ?",
                (card_code,)
            )
            current_telegram_id = cursor.fetchone()
            
            if current_telegram_id and current_telegram_id[0] is not None:
                if current_telegram_id[0] != str(user_id):
                    await update.message.reply_text(
                        "❌ Эта карта уже привязана к другому аккаунту\n"
                        "Для перепривязки обратитесь к администратору"
                    )
                    return

            # Проверка 3: Существование карты
            cursor.execute(
                "SELECT 1 FROM merged_users WHERE code = ?",
                (card_code,)
            )
            if not cursor.fetchone():
                await update.message.reply_text("❌ Карта не найдена")
                return

            # Обновление привязки
            cursor.execute(
                "UPDATE merged_users SET telegram_id = ? WHERE code = ?",
                (str(user_id), card_code)
            )
            
            if cursor.rowcount > 0:
                logger.info("Successfully linked: %s -> %s", user_id, card_code)
                await update.message.reply_text("✅ Аккаунт успешно привязан!")
            else:
                logger.warning("Update failed: %s", card_code)
                await update.message.reply_text("❌ Ошибка привязки")

    except sqlite3.OperationalError as e:
        logger.error("Database error: %s", str(e))
        await update.message.reply_text("⚠️ Ошибка базы данных, попробуйте позже")
    except Exception as e:
        logger.error("General error: %s", str(e))
        await update.message.reply_text("⚠️ Ошибка привязки аккаунта")
    finally:  # Добавлен обязательный блок finally
        if conn:
            conn.close()

def main():
    application = Application.builder().token(TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("link", link_account))
    
    logger.info("Bot started")
    application.run_polling()

if __name__ == '__main__':
    main()