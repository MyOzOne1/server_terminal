from flask import Flask, render_template, session, redirect, url_for, request, jsonify, Response
import psycopg2.extras
import psycopg2
import psycopg2.pool
import secrets
import time
from datetime import datetime, date, timedelta
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import logging
import pdfplumber
import pandas as pd
from io import BytesIO
import base64
from base64 import b64encode
import json
import uuid 
import os
import sys
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import telegram
from telegram.ext import Application, ContextTypes
import random
import string
import threading

sys.path.append(r'C:\Users\Azyabin\AppData\Local\Programs\Python\Python310\Lib\site-packages')
from flask_cors import CORS



app = Flask(__name__)
CORS(app, supports_credentials=True, resources={r"/*": {"origins": "*"}})
app.config.update(
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=False,
    SESSION_COOKIE_HTTPONLY=True,
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=15),
    SESSION_REFRESH_EACH_REQUEST=True
)

# Настройка TG бота
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['TELEGRAM_BOT_TOKEN'] = '8142838834:AAEA42xudSOnnqaIZX6PjT77-VGcLDDYW04'

# Настройка VK бота
app.config['VK_BOT_TOKEN'] = 'vk1.a.FPDePh1s0WC-5HZvM-_6o8491Yae_v49v9H1aeUS2V8E4plvIuo8A-08vVqKr9nO-9kCF-oCUJdGRys12NB9SNfRnGNc9VavOSLrdADiBEJKAN3Xvm2CYB822M8Ot-_gQ_G6F6y024PX3nf9FNI_BYCp5vJwpm3JPBt_dtaTU4SC4kZMyRo7Hr7Ne6xSnE7WaZpXdHybIRAaXYVyVYPQog'
app.config['VK_GROUP_ID'] = 'public170226687'

# Настройка PostgreSQL
app.config['POSTGRES_HOST'] = '192.168.211.224'
app.config['POSTGRES_PORT'] = 5430
app.config['POSTGRES_DB'] = 'test'
app.config['POSTGRES_USER'] = 'postgres'
app.config['POSTGRES_PASSWORD'] = 'postgres'

# Настройка 1С вход
app.config['ONE_C_USERNAME'] = 'базуеввв'
app.config['ONE_C_PASSWORD'] = 'gjkrjdybr'
app.config['ONE_C_PAYSLIP_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/PaySlip'
app.config['ONE_C_VACATIONS_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/Vacations'
app.config['ONE_C_PERSONAL_DATA_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/PersonalData'
app.config['ONE_C_TIMESHEET_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/Timesheet'

# Ззаголовок авторизации
auth_str = f"{app.config['ONE_C_USERNAME']}:{app.config['ONE_C_PASSWORD']}"
app.config['ONE_C_AUTH_HEADER'] = 'Basic ' + b64encode(auth_str.encode('utf-8')).decode('ascii')

# Глобальные переменные для VK бота
vk_bot_thread = None
vk_session = None
vk = None
longpoll = None

# Добавляем CORS middleware
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response

class IgnoreAuthStatusFilter(logging.Filter):
    def filter(self, record):
        return '/api/auth_status' not in record.getMessage()

log = logging.getLogger('werkzeug')
# логировать только ошибки, либо добавляем фильтр
# log.setLevel(logging.ERROR)
log.addFilter(IgnoreAuthStatusFilter())


# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("server.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Пул соединений PostgreSQL
postgres_pool = None
try:
    postgres_pool = psycopg2.pool.SimpleConnectionPool(
        5, 30,
        host=app.config['POSTGRES_HOST'],
        port=app.config['POSTGRES_PORT'],
        database=app.config['POSTGRES_DB'],
        user=app.config['POSTGRES_USER'],
        password=app.config['POSTGRES_PASSWORD']
    )
    logger.info("PostgreSQL connection pool created")
except Exception as e:
    logger.error("Error creating PostgreSQL connection pool: %s", e)

codes = {}  # {card_code: (code, timestamp)}

def get_db_connection():
    try:
        conn = postgres_pool.getconn()
        conn.cursor_factory = psycopg2.extras.DictCursor
        return conn
    except Exception as e:
        logger.error("Error getting DB connection: %s", e)
        return None

def release_db_connection(conn):
    try:
        postgres_pool.putconn(conn)
    except Exception as e:
        logger.error("Error releasing DB connection: %s", e)

def init_tables():
    """Инициализировать таблицы в PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        with conn.cursor() as cursor:
            # Проверка существования таблицы users
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    login TEXT,
                    password TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    middle_name TEXT,
                    gender TEXT,
                    birthday DATE,
                    phone TEXT,
                    email TEXT,
                    work_place TEXT,
                    services TEXT,
                    deleted_by_user_id INTEGER,
                    created_by INTEGER,
                    fired_at TIMESTAMP,
                    deleted_at TIMESTAMP,
                    remember_token TEXT,
                    is_blocked INTEGER DEFAULT 0,
                    source TEXT,
                    person_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    personnel_number TEXT,
                    uuid TEXT,
                    recognize_status TEXT,
                    consent_to_date DATE,
                    is_online INTEGER DEFAULT 0,
                    is_allowed_issue_permanent_pass INTEGER DEFAULT 0,
                    is_foreign_citizen INTEGER DEFAULT 0,
                    old_id INTEGER,
                    extra_data JSONB,
                    telegram_id TEXT,
                    vk_id TEXT
                )
            """)
            
            # Проверка существования таблицы pass
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pass (
                    id SERIAL PRIMARY KEY,
                    created_user_id INTEGER,
                    user_id INTEGER REFERENCES users(id),
                    type TEXT,
                    code TEXT UNIQUE,
                    code_format TEXT,
                    valid_from DATE,
                    valid_to DATE,
                    block_from TIMESTAMP,
                    block_to TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    external_service_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    deleted_at TIMESTAMP,
                    source TEXT,
                    sub_type TEXT,
                    status TEXT,
                    return_at TIMESTAMP
                )
            """)
            
            # Таблица для логов доступа
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS access_logs (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица для привязок VK и Telegram аккаунтов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS telegram_bindings (
                    id SERIAL PRIMARY KEY,
                    pass_id INTEGER NOT NULL REFERENCES pass(id),
                    card_code VARCHAR(50) NOT NULL,
                    telegram_id BIGINT,
                    vk_id INTEGER,
                    vk_verification_code VARCHAR(10),
                    vk_code_created_at TIMESTAMP,
                    vk_code_expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(telegram_id),
                    UNIQUE(vk_id),
                    UNIQUE(card_code)
                )
            """)
            
            # Индексы для оптимизации
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_bindings_vk_id ON telegram_bindings(vk_id);
                CREATE INDEX IF NOT EXISTS idx_telegram_bindings_telegram_id ON telegram_bindings(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_telegram_bindings_card_code ON telegram_bindings(card_code);
            """)
            
            conn.commit()
            logger.info("Tables initialized successfully")
    except Exception as e:
        logger.error("Error initializing tables: %s", e)
        conn.rollback()
    finally:
        release_db_connection(conn)

def generate_verification_code(length=6):
    """Генерация проверочного кода"""
    return ''.join(random.choices(string.digits, k=length))

def send_telegram_code(card_code):
    """Отправить код подтверждения через Telegram с детальными ошибками"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get database connection")
            return {
                'success': False,
                'error': 'Ошибка подключения к базе данных'
            }
        
        logger.info("Starting send_telegram_code for card: %s", card_code)
        
        with conn.cursor() as cursor:
            # 1. Сначала пытаемся найти по чистому цифровому коду
            logger.debug("Querying Telegram binding for code: %s", card_code)
            cursor.execute("""
                SELECT tb.telegram_id
                FROM telegram_bindings tb
                WHERE tb.card_code = %s
            """, (card_code,))
            binding = cursor.fetchone()
            
            # 2. Если не найдено, ищем по формату с запятой
            if not binding:
                formatted_code = f"{card_code[:3]},{card_code[3:]}"
                logger.debug("Querying Telegram binding for formatted code: %s", formatted_code)
                cursor.execute("""
                    SELECT tb.telegram_id
                    FROM telegram_bindings tb
                    WHERE tb.card_code = %s
                """, (formatted_code,))
                binding = cursor.fetchone()
                
                if binding:
                    logger.info("Updating card code format for: %s -> %s", formatted_code, card_code)
                    cursor.execute("""
                        UPDATE telegram_bindings
                        SET card_code = %s
                        WHERE card_code = %s
                    """, (card_code, formatted_code))
                    conn.commit()
        
        if not binding:
            logger.error("No Telegram binding found for card: %s", card_code)
            return {
                'success': False,
                'error': 'Карта не привязана к Telegram аккаунту'
            }

        telegram_id = binding[0]
        logger.info("Found Telegram ID: %s for card: %s", telegram_id, card_code)

        # Генерация кода
        code = str(secrets.randbelow(10**6)).zfill(6)
        codes[card_code] = (code, time.time())
        logger.info("Generated code: %s for %s", code, card_code)

        # Отправка сообщения
        message = f"🔐 Ваш код доступа: {code}\n⏳ Срок действия: 5 минут"
        logger.info("Preparing to send Telegram message - chat_id: %s, message preview: %s", telegram_id, message[:50] + "..." if len(message) > 50 else message)
        
        url = f"https://api.telegram.org/bot{app.config['TELEGRAM_BOT_TOKEN']}/sendMessage"
        logger.debug("Sending POST to URL: %s", url)
        
        response = requests.post(
            url,
            json={
                'chat_id': telegram_id,
                'text': message,
                'parse_mode': 'Markdown'
            },
            timeout=10,
            verify=False  # Отключить SSL-верификацию для обхода проблемы с сертификатами (временное решение!)
        )
        
        logger.info("Telegram response received - status: %s", response.status_code)
        if response.text:
            logger.debug("Telegram response text (first 200 chars): %s", response.text[:200])
        
        if not response.ok:
            error_data = response.json() if response.text else {}
            logger.error("Telegram API error details: status=%s, data=%s", response.status_code, error_data)
            return {
                'success': False,
                'error': f'Ошибка Telegram API: {error_data.get("description", "Unknown error")}'
            }

        logger.info("Telegram message sent successfully for code: %s", code)
        return {
            'success': True,
            'message': 'Код отправлен в Telegram'
        }

    except requests.exceptions.Timeout as e:
        logger.error("Telegram API timeout - details: %s", str(e), exc_info=True)
        return {
            'success': False,
            'error': 'Таймаут подключения к Telegram'
        }
    except requests.exceptions.ConnectionError as e:
        logger.error("Telegram API connection error - details: %s", str(e), exc_info=True)
        return {
            'success': False,
            'error': 'Ошибка подключения к Telegram'
        }
    except Exception as e:
        logger.error("Unexpected error in send_telegram_code: %s", str(e), exc_info=True)
        return {
            'success': False,
            'error': f'Внутренняя ошибка: {str(e)}'
        }
    finally:
        if conn:
            release_db_connection(conn)
            logger.debug("Database connection released")


def send_vk_code(card_code):
    """Отправить код подтверждения через VK с детальными ошибками"""
    try:
        conn = get_db_connection()
        if not conn:
            return {
                'success': False,
                'error': 'Ошибка подключения к базе данных'
            }
        
        with conn.cursor() as cursor:
            # Ищем привязку VK для карты
            cursor.execute("""
                SELECT vk_id
                FROM telegram_bindings
                WHERE card_code = %s AND vk_id IS NOT NULL
            """, (card_code,))
            binding = cursor.fetchone()
            
            if not binding:
                return {
                    'success': False,
                    'error': 'Карта не привязана к VK аккаунту'
                }

            vk_id = binding[0]

            # Генерация кода
            code = generate_verification_code()
            #codes[card_code] = (code, time.time())
            logger.info("Generated VK code: %s for %s", code, card_code)

            # Сохраняем код в telegram_bindings
            cursor.execute("""
                UPDATE telegram_bindings 
                SET vk_verification_code = %s,
                    vk_code_created_at = NOW(),
                    vk_code_expires_at = NOW() + INTERVAL '5 minutes'
                WHERE card_code = %s
            """, (code, card_code))
            conn.commit()

            # Отправка сообщения через VK API
            message = f"🔐 Ваш проверочный код: {code}\nКод действителен 5 минут"
            
            try:
                vk.messages.send(
                    user_id=vk_id,
                    message=message,
                    random_id=random.randint(1, 1000000)
                )
                return {
                    'success': True,
                    'message': 'Код отправлен в VK'
                }
            except Exception as e:
                logger.error("VK API error: %s", str(e))
                return {
                    'success': False,
                    'error': f'Ошибка VK API: {str(e)}'
                }

    except Exception as e:
        logger.error("Error in send_vk_code: %s", str(e))
        return {
            'success': False,
            'error': f'Внутренняя ошибка: {str(e)}'
        }
    finally:
        if conn:
            release_db_connection(conn)

def start_vk_bot():
    """Запуск VK бота в отдельном потоке"""
    global vk_session, vk, longpoll
    
    try:
        vk_session = vk_api.VkApi(token=app.config['VK_BOT_TOKEN'])
        vk = vk_session.get_api()
        longpoll = VkBotLongPoll(vk_session, app.config['VK_GROUP_ID'])
        
        logger.info("VK Bot started")
        
        def vk_bot_loop():
            while True:
                try:
                    for event in longpoll.listen():
                        if event.type == VkBotEventType.MESSAGE_NEW:
                            message = event.object.message
                            user_id = message['from_id']
                            text = message['text'].lower().strip()
                            
                            logger.info("Received VK message from %s: %s", user_id, text)
                            
                            if text.startswith('привет') or text.startswith('start') or text.startswith('/start'):
                                handle_vk_start(user_id, vk)
                            elif text.startswith('привязать'):
                                card_code = text.replace('привязать', '').strip()
                                handle_vk_link_account(user_id, card_code, vk)
                            elif text.startswith('код'):
                                handle_vk_get_code(user_id, vk)
                            else:
                                vk.messages.send(
                                    user_id=user_id,
                                    message="❓ Неизвестная команда. Используйте:\n- привязать <код_карты>\n- код",
                                    random_id=random.randint(1, 1000000)
                                )
                except Exception as e:
                    logger.error("VK bot error: %s", str(e))
                    time.sleep(5)
        
        # Запускаем бота в отдельном потоке
        bot_thread = threading.Thread(target=vk_bot_loop, daemon=True)
        bot_thread.start()
        
    except Exception as e:
        logger.error("Error starting VK bot: %s", str(e))

def handle_vk_start(user_id, vk):
    """Обработчик команды начала работы VK"""
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

def handle_vk_link_account(user_id, card_code, vk):
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
        
        logger.info("VK linking attempt: %s -> %s", user_id, card_code)
        
        with conn.cursor() as cursor:
            # 1. Проверить существующую привязку пользователя
            cursor.execute("""
                SELECT card_code 
                FROM telegram_bindings 
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
                FROM telegram_bindings 
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
                INSERT INTO telegram_bindings (pass_id, card_code, vk_id)
                VALUES (%s, %s, %s)
            """, (pass_id, card_code, user_id))
            
            if cursor.rowcount > 0:
                logger.info("Successfully linked VK: %s -> %s", user_id, card_code)
                vk.messages.send(
                    user_id=user_id,
                    message="✅ Аккаунт успешно привязан!",
                    random_id=random.randint(1, 1000000)
                )
            else:
                logger.warning("VK binding failed for: %s", card_code)
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

def handle_vk_get_code(user_id, vk):
    """Отправка проверочного кода через VK"""
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
                FROM telegram_bindings 
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
            
            card_code = binding[0]
            
            # Генерация кода
            code = generate_verification_code()
            
            # Сохранить код в telegram_bindings
            cursor.execute("""
                UPDATE telegram_bindings 
                SET vk_verification_code = %s,
                    vk_code_created_at = NOW(),
                    vk_code_expires_at = NOW() + INTERVAL '5 minutes'
                WHERE card_code = %s
            """, (code, card_code))
            
            conn.commit()
            
            # Отправить код пользователю
            vk.messages.send(
                user_id=user_id,
                message=f"🔐 Ваш проверочный код: {code}\nКод действителен 5 минут",
                random_id=random.randint(1, 1000000)
            )
            
    except Exception as e:
        logger.error("Error generating VK code: %s", str(e), exc_info=True)
        vk.messages.send(
            user_id=user_id,
            message="⚠️ Ошибка генерации кода",
            random_id=random.randint(1, 1000000)
        )
    finally:
        if conn:
            release_db_connection(conn)

def send_code_via_channel(card_code, channel='telegram'):
    """Отправить код подтверждения через указанный канал с детальной обработкой ошибок"""
    try:
        if channel == 'telegram':
            return send_telegram_code(card_code)
        elif channel == 'vk':
            return send_vk_code(card_code)
        else:
            logger.error(f"Unknown channel: {channel}")
            return {
                'success': False,
                'error': f'Неизвестный канал: {channel}'
            }
    except Exception as e:
        logger.error(f"Error in send_code_via_channel: {str(e)}")
        return {
            'success': False,
            'error': f'Внутренняя ошибка сервера: {str(e)}'
        }

@app.before_request
def check_session():
    # Разрешить публичные эндпоинты
    public_endpoints = ['login', 'verify_code', 'resend_code', 'static']
    if request.endpoint in public_endpoints:
        return
    
    # Для API-эндпоинтов
    api_endpoints = ['get_payslip', 'get_vacations', 'get_timesheet', 
                    'get_personal_data', 'get_vacations_info', 'api_profile']
    
    if request.endpoint in api_endpoints:
        if 'user_id' not in session:
            logger.warning(f"Unauthorized access attempt to {request.endpoint}")
            return jsonify({'error': 'Unauthorized'}), 401

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        vk_configured = app.config.get('VK_BOT_TOKEN') and app.config.get('VK_GROUP_ID')
        return render_template('login.html', vk_configured=vk_configured)
    
    if request.method == 'POST':
        card_code = request.form['card_number'].strip()
        channel = request.form.get('channel', 'telegram')
        
        # Проверяем существование карты
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'success': False, 
                'error': 'Ошибка подключения к базе данных'
            })  # Убрали статус 500, возвращаем 200 с ошибкой
            
        try:
            formatted_code = f"{card_code[:3]},{card_code[3:]}"
            
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT u.id
                    FROM pass p
                    JOIN users u ON p.user_id = u.id
                    WHERE p.code = %s OR p.code = %s
                """, (card_code, formatted_code))
                user = cursor.fetchone()
            
            if not user:
                # ВАЖНО: возвращаем 200 с флагом success=False
                return jsonify({
                    'success': False, 
                    'error': 'Карта не зарегистрирована в системе'
                })  # Убрали статус 404
                
        except Exception as e:
            logger.error("Database error in login: %s", str(e))
            return jsonify({
                'success': False, 
                'error': 'Ошибка проверки карты'
            })  # Убрали статус 500
        finally:
            if conn:
                release_db_connection(conn)
        
        # Используем общую функцию отправки кода
        result = send_code_via_channel(card_code, channel)
        
        if result.get('success'):
            return jsonify({
                'success': True, 
                'message': result.get('message', 'Код отправлен'),
                'redirect': url_for('verify_code_page', card_code=card_code, channel=channel)
            })
        
        # Для ошибок отправки кода тоже возвращаем 200 с флагом false
        return jsonify({
            'success': False, 
            'error': result.get('error', 'Ошибка отправки кода')
        })  # Убрали статус 500
    
    return jsonify({'success': False, 'error': "Метод не разрешен"}), 405

@app.route('/api/check_card')
def api_check_card():
    card_id = request.args.get('cardId')
    if not card_id:
        return jsonify({'valid': False, 'error': 'Не указан код карты'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'valid': False, 'error': 'Ошибка подключения к базе данных'}), 500

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT u.id
                FROM pass p
                JOIN users u ON p.user_id = u.id
                WHERE p.code = %s
            """, (card_id,))
            user = cursor.fetchone()
        
        return jsonify({'valid': bool(user)})
    except Exception as e:
        logger.error(f"Ошибка проверки карты: {str(e)}")
        return jsonify({'valid': False, 'error': 'Внутренняя ошибка сервера'}), 500
    finally:
        release_db_connection(conn)

@app.route('/login_by_card', methods=['GET'])
def login_by_card():
    card_id = request.args.get('cardId')
    return_url = request.args.get('return_url', url_for('purchase'))
    
    # Проверка формата - должны быть только цифры
    if card_id and not card_id.isdigit():
        return render_template(
            'login_by_card.html', 
            error="Неверный формат кода карты. Должны быть только цифры.", 
            return_url=return_url
        )
    
    conn = get_db_connection()
    if not conn:
        return render_template(
            'login_by_card.html', 
            error="Ошибка подключения к базе данных", 
            return_url=return_url
        )
    
    try:
        formatted_code = f"{card_id[:3]},{card_id[3:]}"
        
        with conn.cursor() as cursor:
            # Ищем пользователя по двум форматам кода
            cursor.execute("""
                SELECT u.id
                FROM pass p
                JOIN users u ON p.user_id = u.id
                WHERE p.code = %s OR p.code = %s
            """, (card_id, formatted_code))
            user = cursor.fetchone()
        
        if not user:
            return render_template(
                'login_by_card.html', 
                error="Карта не зарегистрирована", 
                return_url=return_url
            )
        
        return render_template(
            'verify_code.html', 
            card_code=card_id, 
            channel='telegram',  # По умолчанию для карты используем Telegram
            return_url=return_url
        )
        
    except Exception as e:
        logger.error(f"Ошибка проверки карты: {str(e)}")
        return render_template(
            'login_by_card.html', 
            error="Внутренняя ошибка сервера", 
            return_url=return_url
        )
    finally:
        if conn:
            release_db_connection(conn)

@app.route('/request_code', methods=['POST'])
def request_code():
    if request.is_json:
        data = request.get_json()
        card_code_clean = data.get('cardId')
        channel = data.get('channel', 'telegram')
    else:
        card_code_clean = request.form.get('cardId')
        channel = request.form.get('channel', 'telegram')

    # Проверка формата - должны быть только цифры
    if not card_code_clean or not card_code_clean.isdigit():
        return jsonify({
            'success': False,
            'error': 'Неверный формат кода карты. Должны быть только цифры.'
        }), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({
            'success': False,
            'error': 'Ошибка подключения к базе данных'
        }), 500

    try:
        formatted_code = f"{card_code_clean[:3]},{card_code_clean[3:]}"
        
        with conn.cursor() as cursor:
            # Ищем пользователя по двум форматам кода
            cursor.execute("""
                SELECT u.id
                FROM pass p
                JOIN users u ON p.user_id = u.id
                WHERE p.code = %s OR p.code = %s
            """, (card_code_clean, formatted_code))
            user = cursor.fetchone()

        if not user:
            return jsonify({
                'success': False,
                'error': 'Карта не зарегистрирована'
            }), 404

        # Используем код в формате базы данных для отправки
        real_card_code = card_code_clean
        if user.get('code') and ',' in user['code']:
            real_card_code = user['code']

        if send_code_via_channel(real_card_code, channel):
            # Создаем временную сессию для отслеживания состояния
            temp_sid = str(uuid.uuid4())
            session['temp_sid'] = temp_sid
            session['card_code'] = real_card_code
            session['temp_user_id'] = user['id']
            session['channel'] = channel  # Сохраняем канал для повторной отправки
            
            return jsonify({
                'success': True,
                'message': 'Код отправлен',
                'sid': temp_sid  # Возвращаем временный SID
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Ошибка отправки кода'
            })
            
    except Exception as e:
        logger.error(f"Ошибка в request_code: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Внутренняя ошибка сервера'
        }), 500
    finally:
        if conn:
            release_db_connection(conn)

@app.route('/verify_code', methods=['POST'])
def verify_code():
    try:
        # Логируем входящие данные для отладки
        logger.debug("Headers: %s", request.headers)
        logger.debug("Form data: %s", request.form)

        card_code = request.form.get('card_code', '').strip()
        user_code = request.form.get('code', '').strip()
        channel = request.form.get('channel', 'telegram')  # Получаем канал из формы
        
        logger.debug("Received card_code: '%s', code: '%s', channel: '%s'", card_code, user_code, channel)

        # Проверяем формат кода карты
        if not card_code or not card_code.isdigit():
            return jsonify({
                'success': False,
                'error': 'Неверный формат кода карты. Должны быть только цифры.'
            }), 400

        # Проверяем наличие кода подтверждения
        if not user_code:
            logger.error("Missing verification code")
            return jsonify({
                'success': False,
                'error': 'Не указан код подтверждения'
            }), 400

        # Проверяем код в зависимости от канала
        if channel == 'telegram':
            # Для Telegram проверяем код в памяти
            stored = codes.get(card_code)
            if not stored:
                logger.error("Code not found for card: %s", card_code)
                return jsonify({
                    'success': False,
                    'error': 'Код устарел или не существует. Запросите новый код.'
                }), 400

            code, timestamp = stored
            logger.debug("Stored Telegram code: %s (generated at %s)", code, timestamp)

            # Проверяем срок действия кода (5 минут)
            if time.time() - timestamp > 300:
                logger.error("Code expired")
                del codes[card_code]
                return jsonify({
                    'success': False,
                    'error': 'Время действия кода истекло. Запросите новый код.'
                }), 400

            # Проверяем совпадение кодов
            if code != user_code:
                logger.error("Code mismatch. Expected: %s, Received: %s", code, user_code)
                return jsonify({
                    'success': False,
                    'error': 'Неверный код подтверждения'
                }), 400

            # Удаляем использованный код из кэша
            if card_code in codes:
                del codes[card_code]
                
        elif channel == 'vk':
            # Для VK проверяем код в базе данных
            conn = get_db_connection()
            if not conn:
                return jsonify({
                    'success': False,
                    'error': 'Ошибка подключения к базе данных'
                }), 500
                
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT vk_verification_code, vk_code_expires_at
                        FROM telegram_bindings
                        WHERE card_code = %s
                        AND vk_verification_code IS NOT NULL
                        AND vk_code_expires_at > NOW()
                    """, (card_code,))
                    result = cursor.fetchone()
                    
                    if not result:
                        logger.error("VK code not found or expired for card: %s", card_code)
                        return jsonify({
                            'success': False,
                            'error': 'Код устарел или не существует. Запросите новый код.'
                        }), 400
                    
                    stored_code = result[0]
                    expires_at = result[1]
                    
                    logger.debug("Stored VK code: %s (expires at %s)", stored_code, expires_at)
                    
                    # Проверяем совпадение кодов
                    if stored_code != user_code:
                        logger.error("VK code mismatch. Expected: %s, Received: %s", stored_code, user_code)
                        return jsonify({
                            'success': False,
                            'error': 'Неверный код подтверждения'
                        }), 400
                    
                    # Очищаем использованный код
                    cursor.execute("""
                        UPDATE telegram_bindings 
                        SET vk_verification_code = NULL,
                            vk_code_expires_at = NULL
                        WHERE card_code = %s
                    """, (card_code,))
                    conn.commit()
                    
            except Exception as e:
                logger.error("Database error in VK code verification: %s", str(e))
                return jsonify({
                    'success': False,
                    'error': 'Ошибка проверки кода'
                }), 500
            finally:
                if conn:
                    release_db_connection(conn)
        else:
            logger.error("Unknown channel: %s", channel)
            return jsonify({
                'success': False,
                'error': 'Неизвестный канал отправки кода'
            }), 400

        # Ищем пользователя в базе данных
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'success': False,
                'error': 'Ошибка подключения к базе данных'
            }), 500
            
        try:
            formatted_code = f"{card_code[:3]},{card_code[3:]}"
            
            with conn.cursor() as cursor:
                # Ищем пользователя по двум форматам кода
                cursor.execute("""
                    SELECT u.id
                    FROM pass p
                    JOIN users u ON p.user_id = u.id
                    WHERE p.code = %s OR p.code = %s
                """, (card_code, formatted_code))
                user = cursor.fetchone()

            if not user:
                logger.error("User not found in database for card: %s", card_code)
                return jsonify({
                    'success': False,
                    'error': 'Пользователь не существует'
                }), 404

            # Создаем сессию
            session_id = str(uuid.uuid4())
            session.permanent = True
            session['user_id'] = user['id']
            session['sid'] = session_id
            session['last_activity'] = datetime.now().isoformat()
            session['card_code'] = card_code
            
            logger.info(f"User authenticated: {card_code}, SID: {session_id}")

            # Определяем тип клиента
            user_agent = request.headers.get('User-Agent', '').lower()
            is_web_client = 'mozilla' in user_agent or 'webkit' in user_agent
            
            if is_web_client:
                # Для веб-клиента перенаправляем на профиль
                profile_url = url_for('profile', card_code=card_code, sid=session_id)
                response = redirect(profile_url)
                response.set_cookie(
                    'session',
                    value=session_id,
                    httponly=True,
                    secure=False,
                    samesite='Lax',
                    max_age=86400,
                    path='/'
                )
                logger.debug(f"Setting session cookie: {session_id}")
                return response
            else:
                # Для PySide клиента возвращаем JSON
                return jsonify({
                    'success': True,
                    'sid': session_id,
                    'card_code': card_code,
                    'message': 'Авторизация успешна'
                })
                
        except Exception as e:
            logger.error("Database error: %s", str(e))
            return jsonify({
                'success': False,
                'error': 'Ошибка базы данных'
            }), 500
        finally:
            if conn:
                release_db_connection(conn)

    except Exception as e:
        logger.error("Critical error in verify_code: %s", str(e), exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Внутренняя ошибка сервера'
        }), 500

@app.route('/verify_code', methods=['GET'])
def verify_code_page():
    card_code = request.args.get('card_code')
    channel = request.args.get('channel', 'telegram')
    if not card_code:
        return redirect(url_for('login'))
    return render_template('verify_code.html', card_code=card_code, channel=channel)

@app.route('/api/profile')
def api_profile():
    # Проверяем наличие sid в сессии и в запросе
    request_sid = request.args.get('sid')
    if 'sid' not in session or session['sid'] != request_sid:
        logger.warning(f"Invalid session ID in api_profile. Session SID: {session.get('sid')}, Request SID: {request_sid}")
        return jsonify({'error': 'Unauthorized'}), 401

    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    user_id = session['user_id']
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    u.id, 
                    u.first_name, 
                    u.last_name, 
                    u.middle_name, 
                    u.email, 
                    u.phone, 
                    topo.name AS work_place,
                    pos.name AS position,
                    p.code,
                    fp.image_path AS photo_path
                FROM users u
                JOIN pass p ON u.id = p.user_id
                LEFT JOIN user_topology_relations utr ON u.id = utr.user_id
                LEFT JOIN topologies topo ON utr.topology_id = topo.id
                LEFT JOIN user_positions up ON u.id = up.user_id
                LEFT JOIN positions pos ON up.position_id = pos.id
                LEFT JOIN face_patterns fp ON u.id = fp.id
                WHERE u.id = %s
            """, (user_id,))
            user = cursor.fetchone()

        if not user:
            return jsonify({'error': 'User not found'}), 404

        # Формируем полное имя, учитывая возможные NULL
        first_name = user['first_name'] or ''
        last_name = user['last_name'] or ''
        middle_name = user['middle_name'] or ''
        full_name = f"{last_name} {first_name} {middle_name}".strip()

        user_data = {
            'id': user['id'],
            'first_name': first_name,
            'last_name': last_name,
            'middle_name': middle_name,
            'email': user['email'] or '',
            'phone': user['phone'] or '',
            'work_place': user['work_place'] or '',
            'position': user['position'] or '',
            'card_code': user['code'] or '',
            'photo_path': user['photo_path'] or '',
            'full_name': full_name  # Обязательное поле для клиента
        }
        logger.debug(f"Returning user data for user_id: {user_id}, full_name: {full_name}")
        return jsonify(user_data)
    except Exception as e:
        logger.error(f"Error in api_profile: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Origin, Accept'
    
    # Явно устанавливаем куку сессии
    if 'sid' in session:
        response.set_cookie(
            'session',
            value=session['sid'],
            httponly=True,
            secure=False,
            samesite='Lax',
            max_age=86400,
            path='/'
        )
    return response

@app.route('/resend_code', methods=['POST'])
def resend_code():
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'No data provided'}), 400

    card_code = data.get('card_code')
    channel = data.get('channel')

    if not card_code or not channel:
        return jsonify({'success': False, 'error': 'Missing card_code or channel'}), 400

    try:
        if channel == 'telegram':
            success = send_telegram_code(card_code)
        elif channel == 'vk':
            success = send_vk_code(card_code)
        else:
            return jsonify({'success': False, 'error': 'Unknown channel'}), 400

        if success:
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': 'Ошибка повторной отправки кода'})
    except Exception as e:
        logger.error(f"Resend error: {str(e)}")
        return jsonify({'success': False, 'error': 'Внутренняя ошибка сервера'}), 500

@app.route('/profile/<card_code>')
def profile(card_code):
    # Универсальный способ получения session_id
    request_sid = request.args.get('sid') or request.cookies.get('session') or session.get('sid')
    
    # Проверка валидности сессии
    if not request_sid or 'sid' not in session or session['sid'] != request_sid:
        logger.warning(f"Invalid session ID: {request_sid}")
        return redirect(url_for('login'))
    
    # Проверка таймаута сессии
    if 'last_activity' in session:
        last_activity = datetime.fromisoformat(session['last_activity'])
        if (datetime.now() - last_activity).total_seconds() > app.config['PERMANENT_SESSION_LIFETIME'].total_seconds():
            session.clear()
            logger.warning("Session expired")
            return redirect(url_for('login'))
    
    # Обновление времени активности
    session['last_activity'] = datetime.now().isoformat()
    
    conn = get_db_connection()
    if not conn:
        return "Database error", 500
        
    try:
        today = date.today()
        # Форматируем код в старый формат с запятой
        formatted_code = f"{card_code[:3]},{card_code[3:]}"
        
        with conn.cursor() as cursor:
            # Ищем пользователя по двум форматам кода
            cursor.execute("""
                SELECT 
                    u.id, 
                    u.first_name, 
                    u.last_name, 
                    u.middle_name, 
                    topo.name AS work_place,
                    pos.name AS position,
                    p.code,
                    u.birthday,
                    u.is_blocked,
                    p.is_active,
                    MIN(wt.in_time) AS first_in_time,
                    MAX(wt.out_time) AS last_out_time,
                    fp.image_path AS photo_path
                FROM users u
                JOIN pass p ON u.id = p.user_id
                LEFT JOIN user_topology_relations utr ON u.id = utr.user_id
                LEFT JOIN topologies topo ON utr.topology_id = topo.id
                LEFT JOIN user_positions up ON u.id = up.user_id
                LEFT JOIN positions pos ON up.position_id = pos.id
                LEFT JOIN face_patterns fp ON u.id = fp.id
                LEFT JOIN work_time_tracking wt 
                    ON u.id = wt.user_id 
                    AND (wt.in_time::date = %s OR wt.out_time::date = %s)
                WHERE p.code = %s OR p.code = %s
                GROUP BY u.id, p.id, topo.name, pos.name, fp.image_path
            """, (today, today, card_code, formatted_code))
            user = cursor.fetchone()
            
        if not user:
            logger.error(f"User not found for card: {card_code} or {formatted_code}")
            return "User not found", 404
        
        in_time = user['first_in_time']
        out_time = user['last_out_time']
        
        if in_time and in_time.date() == today and (not out_time or out_time.date() != today):
            out_time = None
            
        # Используем код из базы данных (может быть с запятой)
        db_card_code = user['code']
            
        user_data = {
            'id': user['id'],
            'first_name': user['first_name'],
            'last_name': user['last_name'],
            'middle_name': user['middle_name'],
            'work_place': user['work_place'],
            'position': user['position'],
            'birthday': user['birthday'],
            'is_blocked': user['is_blocked'],
            'card_code': db_card_code,  # Используем код из базы
            'is_active': user['is_active'],
            'in_time': in_time,
            'out_time': out_time,
            'photo_path': user['photo_path'],
            'full_name': f"{user['last_name']} {user['first_name']} {user['middle_name']}",
            'today': today.strftime('%d.%m.%Y'),
            'vacation_dates': "не указаны",
            'days_left': "не указано",
            'email': 'не указан',
            'phone': 'не указан',
            'personnel_number': 'не указан'
        }
        
        # Получаем персональные данные из 1С
        try:
            cookies = {'session': request.cookies.get('session')}
            response = requests.get(
                'http://192.168.202.201:5050/api/get_personal_data',
                cookies=cookies,
                timeout=10
            )
            
            if response.status_code == 200:
                personal_data = response.json()
                if personal_data.get('success'):
                    user_data['email'] = personal_data.get('email', 'не указан')
                    user_data['phone'] = personal_data.get('phone', 'не указан')
                    user_data['personnel_number'] = personal_data.get('personnel_number', 'не указан')
                    
                    if personal_data.get('photo_base64'):
                        user_data['photo_path'] = f"data:image/jpeg;base64,{personal_data['photo_base64']}"
        except Exception as e:
            logger.error(f"Error fetching personal data: {str(e)}")

        # Получаем информацию об отпусках
        try:
            cookies = {'session': request.cookies.get('session')}
            response = requests.get(
                'http://192.168.202.201:5050/api/get_vacations_info',
                cookies=cookies,
                timeout=100
            )
            
            if response.status_code == 200:
                vacations_info = response.json()
                content = vacations_info.get('content', '')
                    
                # Декодируем base64 если нужно
                if vacations_info.get('is_base64', False):
                    try:
                        content = base64.b64decode(content).decode('utf-8')
                    except:
                        pass
                    
                # Обрабатываем JSON формат
                if vacations_info.get('is_json', False):
                    try:
                        data = json.loads(content)
                        # Извлекаем дату отпуска и количество дней из JSON
                        vacation_date = data.get('vacation')
                        vacation_days = data.get('NumberOfDays', '')
                            
                        if vacation_date:
                            user_data['vacation_dates'] = vacation_date
                                
                            # Добавляем количество дней
                            if vacation_days:
                                try:
                                    days = int(vacation_days)
                                    if days % 10 == 1 and days % 100 != 11:
                                        user_data['vacation_days'] = f"{days} день"
                                    elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
                                        user_data['vacation_days'] = f"{days} дня"
                                    else:
                                        user_data['vacation_days'] = f"{days} дней"
                                except:
                                    user_data['vacation_days'] = f"{vacation_days} дней"
                            else:
                                user_data['vacation_days'] = "Не указано"
                            
                            # Парсим дату
                            try:
                                start_date = datetime.strptime(vacation_date, '%d.%m.%Y').date()
                                today = datetime.now().date()
                                if start_date > today:
                                    days = (start_date - today).days
                                    if days % 10 == 1 and days % 100 != 11:
                                        user_data['days_left'] = f"{days} день"
                                    elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
                                        user_data['days_left'] = f"{days} дня"
                                    else:
                                        user_data['days_left'] = f"{days} дней"
                            except:
                                user_data['days_left'] = "Не удалось вычислить"
                    except:
                        # Если не получилось распарсить JSON, пробуем как строку
                        pass
                
                # Обрабатываем строковый формат
                if not user_data.get('vacation_dates'):
                    if content:
                        # Проверяем формат: ДД.ММ.ГГГГ-ДД.ММ.ГГГГ (период)
                        if '-' in content and len(content.split('-')) == 2:
                            start_str, end_str = content.split('-')
                            user_data['vacation_dates'] = f"{start_str} - {end_str}"
                            
                            # Парсим дату начала
                            try:
                                start_date = datetime.strptime(start_str, '%d.%m.%Y').date()
                                today = datetime.now().date()
                                if start_date > today:
                                    days = (start_date - today).days
                                    if days % 10 == 1 and days % 100 != 11:
                                        user_data['days_left'] = f"{days} день"
                                    elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
                                        user_data['days_left'] = f"{days} дня"
                                    else:
                                        user_data['days_left'] = f"{days} дней"
                            except:
                                user_data['days_left'] = "Не удалось вычислить"
                        
                        # Формат: ДД.ММ.ГГГГ (одна дата)
                        else:
                            user_data['vacation_dates'] = content
                            # Парсим дату
                            try:
                                start_date = datetime.strptime(content, '%d.%m.%Y').date()
                                today = datetime.now().date()
                                if start_date > today:
                                    days = (start_date - today).days
                                    if days % 10 == 1 and days % 100 != 11:
                                        user_data['days_left'] = f"{days} день"
                                    elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
                                        user_data['days_left'] = f"{days} дня"
                                    else:
                                        user_data['days_left'] = f"{days} дней"
                            except:
                                user_data['days_left'] = "Не указано"
        except Exception as e:
            logger.error(f"Error fetching vacations info: {str(e)}")

        return render_template('profile.html', user=user_data)
    except Exception as e:
        logger.error("Error in profile: %s", str(e))
        return "Internal server error", 500
    finally:
        if conn:
            release_db_connection(conn)

# endpoint для получения персональных данных только из 1С
@app.route('/api/get_personal_data', methods=['GET'])
def get_personal_data():
    # Проверяем специальный параметр сессии
    if 'sid' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'User ID not found'}), 401
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
        
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            url = f"{app.config['ONE_C_PERSONAL_DATA_URL']}?id_1c={one_c_id}"
            logger.info(f"Requesting personal data for 1C_ID: {one_c_id}")
            
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            logger.info(f"1C personal data response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return jsonify({
                        'success': True,
                        'email': data.get('Email', ''),
                        'phone': data.get('Phone', ''),
                        'personnel_number': data.get('PersonnelNumber', ''),
                        'photo_base64': data.get('Photo', '')
                    })
                except json.JSONDecodeError:
                    return jsonify({
                        'error': 'Invalid JSON response from 1C',
                        'response': response.text[:500]
                    }), 502
            else:
                error_text = response.text[:500] if response.text else response.content[:500].decode('utf-8', 'ignore')
                logger.error(f"1C personal data API error: {response.status_code} - {error_text}")
                return jsonify({
                    'error': '1C service error',
                    'status_code': response.status_code,
                    'message': error_text
                }), 502
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection to 1C failed: {str(e)}")
        return jsonify({'error': 'Connection to 1C server failed'}), 503
    except Exception as e:
        logger.error(f"Internal error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

# endpoint для получения расчетного листа
@app.route('/api/get_payslip', methods=['POST'])
def get_payslip():
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session['user_id']
    data = request.json
    year = data.get('year')
    month = data.get('month')
    
    if not year or not month:
        return jsonify({'error': 'Missing year or month'}), 400
    
    try:
        date_param = f"{int(year)}{int(month):02d}01"
    except ValueError:
        return jsonify({'error': 'Invalid year or month format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
        
    try:
        with conn.cursor() as cursor:
            # Получаем 1C ID сотрудника
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            # Формируем URL для конкретного сотрудника
            url = f"{app.config['ONE_C_PAYSLIP_URL']}?id_1c={one_c_id}&Date={date_param}"
            
            # Логируем параметры запроса
            logger.info(f"Requesting payslip for 1C_ID: {one_c_id}, Date: {date_param}")
            
            # Отправляем запрос с авторизацией администратора
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            # Логируем статус ответа
            logger.info(f"1C response status: {response.status_code}")
            
            # Проверяем успешный ответ
            if response.status_code == 200:
                # Вариант 1: Ответ в виде бинарного PDF
                if response.content[:4] == b'%PDF':
                    return Response(
                        response.content,
                        status=200,
                        mimetype='application/pdf'
                    )
                
                # Вариант 2: Ответ в виде base64-кодированного PDF
                if response.text.startswith(('JVBER', 'UEsDB', 'iVBOR')):
                    try:
                        pdf_data = base64.b64decode(response.text)
                        return Response(
                            pdf_data,
                            status=200,
                            mimetype='application/pdf'
                        )
                    except Exception as e:
                        logger.error(f"Base64 decoding error: {str(e)}")
                        return jsonify({
                            'error': 'Base64 decoding failed',
                            'message': 'Failed to decode PDF from base64'
                        }), 502
                
                # Вариант 3: Ответ в виде текстового PDF
                if response.text.startswith('%PDF'):
                    return Response(
                        response.text.encode(),
                        status=200,
                        mimetype='application/pdf'
                    )
                
                # Если ни один формат не распознан
                error_text = response.text[:500] if response.text else response.content[:500].decode('utf-8', 'ignore')
                logger.error(f"1C returned unrecognized content: {error_text}")
                return jsonify({
                    'error': '1C service returned unrecognized content',
                    'message': error_text
                }), 502
            else:
                # Обработка ошибок HTTP
                try:
                    error_text = response.text[:500]
                except:
                    error_text = response.content[:500].decode('utf-8', 'ignore') if response.content else "No content"
                
                logger.error(f"1C API error: {response.status_code} - {error_text}")
                return jsonify({
                    'error': '1C service error',
                    'status_code': response.status_code,
                    'message': error_text
                }), 502
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection to 1C failed: {str(e)}")
        return jsonify({'error': 'Connection to 1C server failed'}), 503
    except Exception as e:
        logger.error(f"Internal error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

# endpoint для получения данных об отпусках ПДФ
@app.route('/api/get_vacations', methods=['POST'])
def get_vacations():
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session['user_id']
    data = request.json
    year = data.get('year')
    
    if not year:
        return jsonify({'error': 'Missing year'}), 400
    
    try:
        year_int = int(year)
    except ValueError:
        return jsonify({'error': 'Invalid year format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
        
    try:
        with conn.cursor() as cursor:
            # Получаем 1C ID сотрудника
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            # Формируем URL для запроса отпусков
            url = f"{app.config['ONE_C_VACATIONS_URL']}?id_1c={one_c_id}&Date={year_int}0101&view=report"
            
            # Логируем параметры запроса
            logger.info(f"Requesting vacations for 1C_ID: {one_c_id}, Year: {year_int}")
            
            # Отправляем запрос с авторизацией администратора
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            # Логируем статус ответа
            logger.info(f"1C vacations response status: {response.status_code}")
            
            # Проверяем успешный ответ
            if response.status_code == 200:
                # Обработка PDF в base64 или бинарном формате
                if response.headers.get('Content-Type') == 'application/pdf' or response.content[:4] == b'%PDF':
                    return Response(
                        response.content,
                        status=200,
                        mimetype='application/pdf'
                    )
                
                # Обработка base64-кодированного PDF
                if response.text.startswith(('JVBER', 'UEsDB', 'iVBOR')):
                    try:
                        pdf_data = base64.b64decode(response.text)
                        return Response(
                            pdf_data,
                            status=200,
                            mimetype='application/pdf'
                        )
                    except Exception as e:
                        logger.error(f"Base64 decoding error: {str(e)}")
                        return jsonify({
                            'error': 'Base64 decoding failed',
                            'message': 'Failed to decode PDF from base64'
                        }), 502
                
                # Если ни один формат не распознан
                error_text = response.text[:500] if response.text else response.content[:500].decode('utf-8', 'ignore')
                logger.error(f"1C returned unrecognized content: {error_text}")
                return jsonify({
                    'error': '1C service returned unrecognized content',
                    'message': error_text
                }), 502
            else:
                # Обработка ошибок HTTP
                try:
                    error_text = response.text[:500]
                except:
                    error_text = response.content[:500].decode('utf-8', 'ignore') if response.content else "No content"
                
                logger.error(f"1C vacations API error: {response.status_code} - {error_text}")
                return jsonify({
                    'error': '1C service error',
                    'status_code': response.status_code,
                    'message': error_text
                }), 502
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection to 1C failed: {str(e)}")
        return jsonify({'error': 'Connection to 1C server failed'}), 503
    except Exception as e:
        logger.error(f"Internal error: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

# endpoint для получения данных об отпусках в JSON в ЛК
@app.route('/api/get_vacations_info', methods=['GET'])
def get_vacations_info():
    # Проверяем специальный параметр сессии
    if 'sid' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'User ID not found'}), 401
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
        
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            url = f"{app.config['ONE_C_VACATIONS_URL']}?id_1c={one_c_id}&Date=20250101&view=table"
            
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                content = response.text
                result = {
                    'is_base64': False,
                    'is_json': False
                }
                
                try:
                    decoded_content = base64.b64decode(content).decode('utf-8')
                    result['content'] = decoded_content
                    result['is_base64'] = True
                except:
                    result['content'] = content
                
                try:
                    json.loads(result['content'])
                    result['is_json'] = True
                except:
                    pass
                
                return jsonify(result)
            else:
                return jsonify({
                    'error': '1C service error',
                    'status_code': response.status_code
                }), 502
    except Exception as e:
        logger.error("Error getting vacations info: %s", str(e))
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

# endpoint для получения графика работы
@app.route('/api/get_timesheet', methods=['POST'])
def get_timesheet():
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session['user_id']
    data = request.json
    year = data.get('year')
    month = data.get('month')
    
    if not year or not month:
        return jsonify({'error': 'Missing year or month'}), 400
    
    try:
        date_param = f"{int(year)}{int(month):02d}01"
    except ValueError:
        return jsonify({'error': 'Invalid year or month format'}), 400
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
        
    try:
        with conn.cursor() as cursor:
            # Получаем 1C ID сотрудника
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            # Формируем URL для графика работы
            url = f"{app.config['ONE_C_TIMESHEET_URL']}?id_1c={one_c_id}&Date={date_param}"
            logger.info(f"Requesting timesheet for 1C_ID: {one_c_id}, Date: {date_param}")
            
            # Отправляем запрос с авторизацией администратора
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            # Логируем статус ответа и тип контента
            logger.info(f"1C timesheet response status: {response.status_code}")
            logger.info(f"1C response content-type: {response.headers.get('content-type', 'unknown')}")
            logger.info(f"1C response length: {len(response.content)} bytes")
            
            # Проверяем успешный ответ
            if response.status_code == 200:
                # Проверяем, является ли ответ PDF
                content_type = response.headers.get('content-type', '').lower()
                is_pdf = (
                    response.content[:4] == b'%PDF' or  # Бинарный PDF
                    response.text.startswith('%PDF') or  # Текстовый PDF
                    content_type.find('pdf') != -1 or    # Content-Type содержит pdf
                    response.text.startswith(('JVBER', 'UEsDB', 'iVBOR'))  # Base64 PDF
                )
                
                if is_pdf:
                    # Вариант 1: Ответ в виде бинарного PDF
                    if response.content[:4] == b'%PDF':
                        logger.info("Returning binary PDF")
                        return Response(
                            response.content,
                            status=200,
                            mimetype='application/pdf',
                            headers={'Content-Disposition': f'attachment; filename="timesheet_{year}_{month}.pdf"'}
                        )
                    
                    # Вариант 2: Ответ в виде base64-кодированного PDF
                    if response.text.startswith(('JVBER', 'UEsDB', 'iVBOR')):
                        try:
                            logger.info("Decoding base64 PDF")
                            pdf_data = base64.b64decode(response.text)
                            return Response(
                                pdf_data,
                                status=200,
                                mimetype='application/pdf',
                                headers={'Content-Disposition': f'attachment; filename="timesheet_{year}_{month}.pdf"'}
                            )
                        except Exception as e:
                            logger.error(f"Base64 decoding error: {str(e)}")
                            # Если декодирование не удалось, возвращаем текст как есть
                            return Response(
                                response.text,
                                status=200,
                                mimetype='text/plain',
                                headers={'Content-Disposition': f'attachment; filename="timesheet_error_{year}_{month}.txt"'}
                            )
                    
                    # Вариант 3: Ответ в виде текстового PDF
                    if response.text.startswith('%PDF'):
                        logger.info("Returning text PDF")
                        return Response(
                            response.text.encode(),
                            status=200,
                            mimetype='application/pdf',
                            headers={'Content-Disposition': f'attachment; filename="timesheet_{year}_{month}.pdf"'}
                        )
                
                # Если это не PDF, а текстовый ответ
                logger.info("1C returned text content instead of PDF")
                
                # Пытаемся определить кодировку и декодировать текст
                try:
                    # Пробуем UTF-8
                    text_content = response.content.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        # Пробуем Windows-1251 (кириллица)
                        text_content = response.content.decode('windows-1251')
                    except UnicodeDecodeError:
                        # Если не получается, используем замену ошибок
                        text_content = response.content.decode('utf-8', errors='replace')
                
                # Логируем первые 500 символов текста для отладки
                logger.info(f"Text content preview: {text_content[:500]}")
                
                # Определяем, является ли текст ошибкой или информационным сообщением
                error_indicators = ['ошибка', 'error', 'exception', 'не найден', 'not found', 'failed']
                is_error = any(indicator in text_content.lower() for indicator in error_indicators)
                
                if is_error:
                    logger.warning(f"1C returned error message: {text_content[:200]}")
                    return jsonify({
                        'error': '1C service returned error message',
                        'message': text_content,
                        'content_type': 'text'
                    }), 502
                else:
                    logger.info("1C returned informational text message")
                    return jsonify({
                        'info': '1C service returned text message instead of PDF',
                        'message': text_content,
                        'content_type': 'text'
                    }), 200
                    
            else:
                # Обработка ошибок HTTP
                try:
                    error_text = response.text[:1000] if response.text else response.content[:1000].decode('utf-8', 'ignore') if response.content else "No content"
                except:
                    error_text = "Unable to decode error response"
                
                logger.error(f"1C API error: {response.status_code} - {error_text}")
                return jsonify({
                    'error': '1C service error',
                    'status_code': response.status_code,
                    'message': error_text
                }), response.status_code if 400 <= response.status_code < 600 else 502
            
    except requests.exceptions.Timeout:
        logger.error("Timeout connecting to 1C server")
        return jsonify({'error': 'Connection to 1C server timed out'}), 504
    except requests.exceptions.ConnectionError:
        logger.error("Connection to 1C server failed")
        return jsonify({'error': 'Cannot connect to 1C server'}), 503
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to 1C failed: {str(e)}")
        return jsonify({'error': 'Request to 1C server failed'}), 503
    except Exception as e:
        logger.error(f"Internal error: {str(e)}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

@app.route('/logout')
def logout():
    redirect_type = request.args.get('redirect', 'web')

    # Помечаем сессию как разлогиненную
    session['logged_out'] = True

    # Очищаем остальные данные сессии
    session.clear()

    # Восстанавливаем флаг logged_out, чтобы сохранить информацию о logout
    session['logged_out'] = True

    user_agent = request.headers.get('User-Agent', '').lower()

    is_qt_client = 'qtwebengine' in user_agent
    is_web_client = ('mozilla' in user_agent or 'webkit' in user_agent) and not is_qt_client

    logger.info(f"Logout request: redirect_type={redirect_type}, is_qt_client={is_qt_client}, user_agent={user_agent}")

    if redirect_type == 'card':
        if is_qt_client:
            logger.info("Redirecting Qt client to login_by_card")
            return jsonify({
                'success': True,
                'redirect': 'login_by_card'
            })
        else:
            logger.info("Redirecting web client to login_by_card page")
            return redirect(url_for('login_by_card'))
    else:
        if is_web_client:
            logger.info("Redirecting web client to login page")
            return redirect(url_for('login'))
        else:
            logger.info("Redirecting Qt client to login")
            return jsonify({
                'success': True,
                'redirect': 'login'
            })

# endpoint для обновления активности
@app.route('/api/update_activity', methods=['POST'])
def update_activity():
    if 'sid' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    session['last_activity'] = datetime.now().isoformat()
    return jsonify({'success': True})

@app.route('/api/ping')
def ping():
    if 'sid' in session and session['sid'] == request.args.get('sid'):
        session['last_activity'] = datetime.now().isoformat()
        return jsonify({'success': True})
    return jsonify({'error': 'Invalid session'}), 401

# Новый эндпоинт для проверки статуса авторизации
@app.route('/api/auth_status')
def auth_status():
    return jsonify({
        'authenticated': 'user_id' in session
    })

@app.route('/api/purchase_data')
def api_purchase_data():
    """API для получения данных о приобретениях (заглушка)"""
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    # Заглушка - в реальности здесь будет запрос к другому серверу
    return jsonify([
        {"id": 1, "product": "Товар 1", "date": "2023-10-15", "amount": 2, "price": 1500},
        {"id": 2, "product": "Товар 2", "date": "2023-10-20", "amount": 1, "price": 3500}
    ])

# Новый маршрут purchase_auth_required
@app.route('/purchase_auth_required.html')
def purchase_auth_required():
    """Страница, требующая авторизации для покупок"""
    return render_template('purchase_auth_required.html')

# Эндпоинт для приобретения продукции
@app.route('/purchase')
def purchase():
    """Основная страница приобретения продукции"""
    # Проверяем специальный параметр сессии
    if 'sid' not in session:
        return redirect(url_for('purchase_auth_required'))
    
    # Проверяем таймаут сессии
    if 'last_activity' in session:
        last_activity = datetime.fromisoformat(session['last_activity'])
        if (datetime.now() - last_activity).total_seconds() > app.config['PERMANENT_SESSION_LIFETIME'].total_seconds():
            session.clear()
            return redirect(url_for('force_logout'))
    
    # Обновляем время активности
    session['last_activity'] = datetime.now().isoformat()
    
    return render_template('purchase.html')

# Новый эндпоинт для принудительного разлогина
@app.route('/force_logout')
def force_logout():
    """Страница, информирующая пользователя о необходимости повторной авторизации"""
    return render_template('force_logout.html')

@app.route('/api/check_channel')
def check_channel():
    card_id = request.args.get('cardId')
    channel = request.args.get('channel', 'telegram')
    
    # Проверка доступности канала
    available = check_channel_availability(card_id, channel)
    
    return jsonify({
        'available': available,
        'channel': channel
    })

@app.route('/api/send_code', methods=['POST'])
def api_send_code():
    data = request.get_json()
    card_code = data.get('card_code')
    channel = data.get('channel', 'telegram')
    
    success = False
    if channel == 'telegram':
        success = send_telegram_code(card_code)
    else:  # VK
        success = send_vk_code(card_code)
    
    if success:
        return jsonify({'success': True})
    else:
        return jsonify({
            'success': False, 
            'error': f"Ошибка отправки кода через {channel}"
        }), 500

def check_channel_availability(card_code, channel):
    """Проверяет доступность выбранного канала для карты"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            if channel == 'telegram':
                cursor.execute("""
                    SELECT telegram_id 
                    FROM telegram_bindings 
                    WHERE card_code = %s AND telegram_id IS NOT NULL
                """, (card_code,))
            else:  # VK
                cursor.execute("""
                    SELECT vk_id 
                    FROM telegram_bindings 
                    WHERE card_code = %s AND vk_id IS NOT NULL
                """, (card_code,))
            
            binding = cursor.fetchone()
            return binding is not None
            
    except Exception as e:
        logger.error("Error checking channel availability: %s", e)
        return False
    finally:
        release_db_connection(conn)

@app.route('/dino_game')
def dino_game():
    # Можно добавить проверку сессии, если хотите ограничить доступ
    return render_template('dino_game.html')

if __name__ == '__main__':
    init_tables()
    # Запускаем VK бот при старте сервера, если настроены токен и group_id
    if app.config.get('VK_BOT_TOKEN') and app.config.get('VK_GROUP_ID'):
        start_vk_bot()
    app.run(host='192.168.202.201', port=5050, debug=True)