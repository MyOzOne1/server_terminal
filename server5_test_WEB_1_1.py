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
from requests.adapters import HTTPAdapter
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib3.util.retry import Retry
from flask import Response, app 
import re  
import pdfplumber
import pandas as pd
import openpyxl
from io import BytesIO
import base64
from base64 import b64encode
import json
from urllib.parse import urlparse
import uuid 
import os
import sys
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from functools import wraps
import random
import string
import threading
sys.path.append(r'C:\Users\Azyabin\AppData\Local\Programs\Python\Python310\Lib\site-packages')
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_session import Session
from flask import send_from_directory

app = Flask(__name__)
CORS(app, supports_credentials=True, resources={r"/*": {"origins": "*"}})

# Trust headers from local reverse proxy (Nginx) for correct scheme/host/IP.
app.wsgi_app = ProxyFix(
    app.wsgi_app,
    x_for=1,
    x_proto=1,
    x_host=1,
    x_port=1,
    x_prefix=1
)

# Настройка сессии Flask
app.config.update(
    SECRET_KEY='your-secret-key-here-change-this-in-production',
    SESSION_COOKIE_NAME='darina_session',
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=False,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=2),
    SESSION_REFRESH_EACH_REQUEST=True,
    SESSION_TYPE='filesystem',
    SESSION_FILE_DIR='./flask_sessions',
    SESSION_PERMANENT=True
)
session_store = Session()
session_store.init_app(app)

# Настройка TG бота
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

# Настройка 1С
app.config['ONE_C_USERNAME'] = 'базуеввв'
app.config['ONE_C_PASSWORD'] = 'gjkrjdybr'
app.config['ONE_C_PAYSLIP_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/PaySlip'
app.config['ONE_C_VACATIONS_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/Vacations'
app.config['ONE_C_PERSONAL_DATA_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/PersonalData'
app.config['ONE_C_TIMESHEET_URL'] = 'http://192.168.202.6/copy2/hs/LK_GBS/EmployeeData/Timesheet'

# Заголовок авторизации
auth_str = f"{app.config['ONE_C_USERNAME']}:{app.config['ONE_C_PASSWORD']}"
app.config['ONE_C_AUTH_HEADER'] = 'Basic ' + b64encode(auth_str.encode('utf-8')).decode('ascii')

# Глобальные переменные для VK бота
vk_bot_thread = None
vk_session = None
vk = None
longpoll = None

#________________________________________________________________________________________________
class IgnoreAuthStatusFilter(logging.Filter):
    def filter(self, record):
        return '/api/auth_status' not in record.getMessage()

log = logging.getLogger('werkzeug')
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

#________________________________________________________________________________________________
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

#________________________________________________________________________________________________
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
            
            # Добавление столбца user_id и access_history в telegram_bindings (если его нет)
            cursor.execute("""
                ALTER TABLE telegram_bindings 
                ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id),
                ADD COLUMN IF NOT EXISTS access_history TEXT DEFAULT ''
            """)
            
            # Индексы для оптимизации
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_telegram_bindings_vk_id ON telegram_bindings(vk_id);
                CREATE INDEX IF NOT EXISTS idx_telegram_bindings_telegram_id ON telegram_bindings(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_telegram_bindings_card_code ON telegram_bindings(card_code);
                CREATE INDEX IF NOT EXISTS idx_telegram_bindings_user_id ON telegram_bindings(user_id);
            """)
            
            conn.commit()
            logger.info("Tables initialized successfully")
    except Exception as e:
        logger.error("Error initializing tables: %s", e)
        conn.rollback()
    finally:
        release_db_connection(conn)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def generate_verification_code(length=6):
    """Генерация проверочного кода"""
    return ''.join(random.choices(string.digits, k=length))

def send_telegram_code(card_code):
    """Отправить код подтверждения через Telegram"""
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
        logger.info("Preparing to send Telegram message - chat_id: %s", telegram_id)
        
        url = f"https://api.telegram.org/bot{app.config['TELEGRAM_BOT_TOKEN']}/sendMessage"
        
        response = requests.post(
            url,
            json={
                'chat_id': telegram_id,
                'text': message,
                'parse_mode': 'Markdown'
            },
            timeout=10,
            verify=False
        )
        
        logger.info("Telegram response received - status: %s", response.status_code)
        
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

def send_vk_code(card_code):
    """Отправить код подтверждения через VK"""
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
    """Отправить код подтверждения через указанный канал"""
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

#________________________________________________________________________________________________
@app.before_request
def check_session():
    # Разрешить публичные эндпоинты
    public_endpoints = ['login', 'verify_code', 'resend_code', 'static', 'api_profile_content', 
                    'api_sync_session', 'website_tab', 'auth_tab', 'login_by_card']
    if request.endpoint in public_endpoints:
        return

    # Обновляем активность для авторизованных пользователей
    if 'user_id' in session:
        update_session_activity()

    # Для API-эндпоинтов проверяем авторизацию
    api_endpoints = ['get_payslip', 'get_vacations', 'get_timesheet',
                    'get_personal_data', 'get_vacations_info', 'api_profile',
                    'api_new_products', 'api_discount_products', 'api_confirm_order',
                    'api_access_history', 'profile']

    if request.endpoint in api_endpoints:
        if 'user_id' not in session:
            logger.warning(f"Unauthorized access attempt to {request.endpoint}")
            return jsonify({'error': 'Unauthorized'}), 401

def update_session_activity():
    """Обновление времени активности сессии"""
    if 'user_id' in session:
        session['last_activity'] = datetime.now().isoformat()
        if 'logged_out' in session:
            session.pop('logged_out', None)

# Эндпоинт для проверки активности сессии
@app.route('/api/check_session', methods=['GET'])
def check_session():
    """Проверка активности сессии"""
    if 'user_id' not in session:
        return jsonify({'active': False}), 401
    
    # Обновляем время последней активности
    session['last_activity'] = datetime.now().isoformat()
    return jsonify({'active': True})

# Обновляем before_request для проверки таймаута сессии
@app.before_request
def check_session_timeout():
    """Проверка таймаута сессии"""
    if 'user_id' in session and 'last_activity' in session:
        try:
            last_activity = datetime.fromisoformat(session['last_activity'])
            timeout_duration = timedelta(minutes=10)  # 10 минут таймаут
            
            if datetime.now() - last_activity > timeout_duration:
                # Сессия истекла
                session.clear()
                logger.info(f"Session expired for user")
                
        except (ValueError, KeyError) as e:
            logger.error(f"Error checking session timeout: {str(e)}")
    
    # Всегда обновляем время активности для авторизованных пользователей
    if 'user_id' in session:
        session['last_activity'] = datetime.now().isoformat()

#________________________________________________________________________________________________
# Основные маршруты для нового интерфейса
@app.route('/')
def index():
    """Главная страница с новым интерфейсом"""
    return render_template('profile_2.html')

@app.route('/website_tab')
def website_tab():
    """Вкладка сайта компании"""
    return render_template('website_tab.html')

@app.route('/static/img/<path:filename>')
def serve_img(filename):
    """Обслуживание изображений из папки img/"""
    try:
        logger.debug(f"Serving image: {filename}")
        return send_from_directory(os.path.join(os.getcwd(), 'img'), filename)
    except Exception as e:
        logger.error(f"Error serving image {filename}: {e}")
        return "File not found", 404

@app.route('/auth_tab')
def auth_tab():
    """Вкладка авторизации"""
    # Проверяем, авторизован ли пользователь
    if 'user_id' in session and 'sid' in session:
        # Проверяем таймаут сессии
        if 'last_activity' in session:
            last_activity = datetime.fromisoformat(session['last_activity'])
            if (datetime.now() - last_activity).total_seconds() <= app.config['PERMANENT_SESSION_LIFETIME'].total_seconds():
                # Сессия активна - загружаем профиль
                return redirect(url_for('load_profile_content'))
    
    # Если сессии нет или она истекла - показываем форму авторизации
    return render_template('auth_tab.html')

@app.route('/api/load_profile_content')
def load_profile_content():
    """Загрузка контента профиля после авторизации"""
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session['user_id']
    card_code = session.get('card_code')
    
    if not card_code:
        # Если card_code нет в сессии, но user_id есть, ищем card_code
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT p.code 
                        FROM pass p 
                        WHERE p.user_id = %s
                    """, (user_id,))
                    pass_record = cursor.fetchone()
                    if pass_record:
                        session['card_code'] = pass_record['code']
                        card_code = pass_record['code']
            except Exception as e:
                logger.error("Error fetching card code: %s", e)
            finally:
                release_db_connection(conn)
    
    if card_code:
        return profile_content(card_code)
    else:
        return jsonify({'error': 'Card code not found'}), 404

def profile_content(card_code):
    """Генерация контента профиля"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
        
    try:
        today = date.today()
        formatted_code = f"{card_code[:3]},{card_code[3:]}"
        
        with conn.cursor() as cursor:
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
                WHERE (p.code = %s OR p.code = %s)
                GROUP BY u.id, p.id, topo.name, pos.name, fp.image_path
                ORDER BY p.is_active DESC, p.created_at DESC
                LIMIT 1
            """, (today, today, card_code, formatted_code))
            user = cursor.fetchone()
            
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        in_time = user['first_in_time']
        out_time = user['last_out_time']
        
        if in_time and in_time.date() == today and (not out_time or out_time.date() != today):
            out_time = None
            
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
            'card_code': db_card_code,
            'is_active': user['is_active'],
            'in_time': in_time,
            'out_time': out_time,
            'photo_path': user['photo_path'],
            'full_name': f"{user['last_name']} {user['first_name']} {user['middle_name']}",
            'today': today.strftime('%d.%m.%Y')
        }
        
        # Получаем персональные данные из 1С
        try:
            personal_data_response = get_personal_data()
            if personal_data_response and hasattr(personal_data_response, 'json'):
                personal_data = personal_data_response.json
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
            vacations_response = get_vacations_info()
            if vacations_response and hasattr(vacations_response, 'json'):
                vacations_info = vacations_response.json
                update_user_vacation_data(user_data, vacations_info)
        except Exception as e:
            logger.error(f"Error fetching vacations info: {str(e)}")

        return jsonify({
            'success': True,
            'profile_content': render_template('profile_content.html', user=user_data),
            'user_data': user_data
        })
        
    except Exception as e:
        logger.error("Error in profile_content: %s", str(e))
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

def update_user_vacation_data(user_data, vacations_info):
    """Обновление данных об отпусках пользователя"""
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
            vacation_date = data.get('vacation')
            vacation_days = data.get('NumberOfDays', '')
                
            if vacation_date:
                user_data['vacation_dates'] = vacation_date
                    
                if vacation_days:
                    try:
                        days = int(vacation_days)
                        user_data['vacation_days'] = format_days(days)
                    except:
                        user_data['vacation_days'] = f"{vacation_days} дней"
                else:
                    user_data['vacation_days'] = "Не указано"
                
                # Парсим дату для вычисления дней до отпуска
                try:
                    start_date = datetime.strptime(vacation_date, '%d.%m.%Y').date()
                    today = datetime.now().date()
                    if start_date > today:
                        days = (start_date - today).days
                        user_data['days_left'] = format_days(days)
                except:
                    user_data['days_left'] = "Не удалось вычислить"
        except:
            pass
    
    # Обрабатываем строковый формат
    if not user_data.get('vacation_dates'):
        if content:
            if '-' in content and len(content.split('-')) == 2:
                start_str, end_str = content.split('-')
                user_data['vacation_dates'] = f"{start_str} - {end_str}"
                
                try:
                    start_date = datetime.strptime(start_str, '%d.%m.%Y').date()
                    today = datetime.now().date()
                    if start_date > today:
                        days = (start_date - today).days
                        user_data['days_left'] = format_days(days)
                except:
                    user_data['days_left'] = "Не удалось вычислить"
            
            else:
                user_data['vacation_dates'] = content
                try:
                    start_date = datetime.strptime(content, '%d.%m.%Y').date()
                    today = datetime.now().date()
                    if start_date > today:
                        days = (start_date - today).days
                        user_data['days_left'] = format_days(days)
                except:
                    user_data['days_left'] = "Не указано"

def format_days(days):
    """Форматирование дней в правильную форму"""
    if days % 10 == 1 and days % 100 != 11:
        return f"{days} день"
    elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20):
        return f"{days} дня"
    else:
        return f"{days} дней"

#________________________________________________________________________________________________
# Маршруты авторизации
@app.route('/login', methods=['POST'])
def login():
    """Обработка авторизации по карте"""
    card_code = request.form['card_number'].strip()
    channel = request.form.get('channel', 'telegram')
    
    # Проверяем существование карты
    conn = get_db_connection()
    if not conn:
        return jsonify({
            'success': False, 
            'error': 'Ошибка подключения к базе данных'
        })
        
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
            return jsonify({
                'success': False, 
                'error': 'Карта не зарегистрирована в системе'
            })
                
    except Exception as e:
        logger.error("Database error in login: %s", str(e))
        return jsonify({
            'success': False, 
            'error': 'Ошибка проверки карты'
        })
    finally:
        if conn:
            release_db_connection(conn)
    
    # Отправляем код подтверждения
    result = send_code_via_channel(card_code, channel)
    
    if result.get('success'):
        return jsonify({
            'success': True, 
            'message': result.get('message', 'Код отправлен'),
            'card_code': card_code,
            'channel': channel
        })
    
    return jsonify({
        'success': False, 
        'error': result.get('error', 'Ошибка отправки кода')
    })

@app.route('/verify_code', methods=['POST'])
def verify_code():
    """Проверка кода подтверждения"""
    try:
        card_code = request.form.get('card_code', '').strip()
        user_code = request.form.get('code', '').strip()
        channel = request.form.get('channel', 'telegram').strip()
        
        logger.debug("Received card_code: '%s', code: '%s', channel: '%s'", 
                    card_code, user_code, channel)
                    
        # Проверяем обязательные поля
        if not card_code or not card_code.isdigit():
            return jsonify({
                'success': False,
                'error': 'Неверный формат кода карты. Должны быть только цифры.'
            }), 400
            
        if not user_code:
            logger.error("Missing verification code")
            return jsonify({
                'success': False,
                'error': 'Не указан код подтверждения'
            }), 400
            
        # Проверяем код в зависимости от канала
        code_valid = False
        
        if channel == 'telegram':
            stored = codes.get(card_code)
            if not stored:
                logger.error("Code not found for card: %s", card_code)
                return jsonify({
                    'success': False,
                    'error': 'Код устарел или не существует. Запросите новый код.'
                }), 400
                
            stored_code, timestamp = stored
            logger.debug("Stored Telegram code: %s (generated at %s)", stored_code, timestamp)
            
            # Проверяем срок действия кода (5 минут)
            if time.time() - timestamp > 300:
                logger.error("Code expired")
                del codes[card_code]
                return jsonify({
                    'success': False,
                    'error': 'Время действия кода истекло. Запросите новый код.'
                }), 400
                
            # Проверяем совпадение кодов
            if stored_code == user_code:
                code_valid = True
                if card_code in codes:
                    del codes[card_code]
                    logger.info("Telegram code verified and removed for card: %s", card_code)
            else:
                logger.error("Code mismatch. Expected: %s, Received: %s", stored_code, user_code)
                return jsonify({
                    'success': False,
                    'error': 'Неверный код подтверждения'
                }), 400
                
        elif channel == 'vk':
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
                    
                    if stored_code == user_code:
                        code_valid = True
                        cursor.execute("""
                            UPDATE telegram_bindings 
                            SET vk_verification_code = NULL,
                                vk_code_expires_at = NULL
                            WHERE card_code = %s
                        """, (card_code,))
                        conn.commit()
                        logger.info("VK code verified and cleared for card: %s", card_code)
                    else:
                        logger.error("VK code mismatch. Expected: %s, Received: %s", stored_code, user_code)
                        return jsonify({
                            'success': False,
                            'error': 'Неверный код подтверждения'
                        }), 400
                    
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
            
        # Если код валиден, создаем сессию
        if code_valid:
            conn = get_db_connection()
            if not conn:
                return jsonify({
                    'success': False,
                    'error': 'Ошибка подключения к базе данных'
                }), 500
                
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
                session['channel'] = channel
                
                if 'logged_out' in session:
                    session.pop('logged_out', None)
                    logger.info("Removed 'logged_out' flag from session after successful authentication")
                
                logger.info(f"User authenticated: user_id={user['id']}, card_code={card_code}, SID: {session_id}")
                session.modified = True

                return jsonify({
                    'success': True,
                    'sid': session_id,
                    'card_code': card_code,
                    'message': 'Авторизация успешна',
                    'user_id': user['id'],
                    'redirect': url_for('auth_tab')
                })
                    
            except Exception as e:
                logger.error("Database error in session creation: %s", str(e))
                return jsonify({
                    'success': False,
                    'error': 'Ошибка создания сессии'
                }), 500
            finally:
                if conn:
                    release_db_connection(conn)
        else:
            logger.error("Code validation failed for card: %s", card_code)
            return jsonify({
                'success': False,
                'error': 'Ошибка проверки кода'
            }), 400
            
    except Exception as e:
        logger.error("Critical error in verify_code: %s", str(e), exc_info=True)
        return jsonify({
            'success': False,
            'error': 'Внутренняя ошибка сервера'
        }), 500

@app.route('/resend_code', methods=['POST'])
def resend_code():
    """Повторная отправка кода подтверждения - поддерживает FormData"""
    try:
        # Получаем данные из FormData
        card_code = request.form.get('card_code') or request.form.get('card_number')
        channel = request.form.get('channel', 'telegram')

        if not card_code:
            return jsonify({'success': False, 'error': 'Missing card_code'}), 400

        logger.info(f"Resending code for card: {card_code}, channel: {channel}")

        result = send_code_via_channel(card_code, channel)
        
        if result.get('success'):
            return jsonify({'success': True, 'message': result.get('message')})
        
        return jsonify({'success': False, 'error': result.get('error', 'Ошибка повторной отправки кода')})
        
    except Exception as e:
        logger.error(f"Resend error: {str(e)}")
        return jsonify({'success': False, 'error': 'Внутренняя ошибка сервера'}), 500

@app.route('/logout')
def logout():
    """Выход из системы"""
    logger.info(f"Logout called, session before clear: {dict(session)}")
    
    # Полностью очищаем сессию
    session.clear()
    
    return jsonify({
        'success': True,
        'message': 'Вы успешно вышли из системы',
        'redirect': url_for('auth_tab')
    })

#________________________________________________________________________________________________
# Авторизация по карте через клиентское приложение теперь не запросом а вставкой в поле ввода 
# потому что так хотя бы работает корректно


#________________________________________________________________________________________________
# API эндпоинты
@app.route('/api/session_status')
def api_session_status():
    """Проверка статуса сессии"""
    if 'user_id' in session and 'sid' in session:
        if 'last_activity' in session:
            last_activity = datetime.fromisoformat(session['last_activity'])
            if (datetime.now() - last_activity).total_seconds() <= app.config['PERMANENT_SESSION_LIFETIME'].total_seconds():
                session['last_activity'] = datetime.now().isoformat()
                return jsonify({
                    'authenticated': True,
                    'user_id': session['user_id'],
                    'card_code': session.get('card_code')
                })
    
    session.clear()
    return jsonify({'authenticated': False})

@app.route('/api/profile')
def api_profile():
    """Получение данных профиля"""
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    user_id = session['user_id']
    card_code = session.get('card_code')
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database error'}), 500
    
    try:
        today = date.today()
        with conn.cursor() as cursor:
            # Если есть card_code в сессии, используем его для точного поиска
            if card_code:
                formatted_code = f"{card_code[:3]},{card_code[3:]}"
                cursor.execute("""
                    SELECT 
                        u.id, 
                        u.first_name, 
                        u.last_name, 
                        u.middle_name, 
                        u.email, 
                        u.phone, 
                        u.birthday, 
                        u.is_blocked,
                        p.is_active,
                        topo.name AS work_place,
                        pos.name AS position,
                        p.code,
                        fp.image_path AS photo_path,
                        MIN(wt.in_time) AS first_in_time,
                        MAX(wt.out_time) AS last_out_time
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
                    WHERE u.id = %s AND (p.code = %s OR p.code = %s)
                    GROUP BY u.id, p.id, topo.name, pos.name, fp.image_path
                    ORDER BY p.is_active DESC, p.created_at DESC
                    LIMIT 1
                """, (today, today, user_id, card_code, formatted_code))
            else:
                # Если card_code нет, берем самую последнюю активную карту
                cursor.execute("""
                    SELECT 
                        u.id, 
                        u.first_name, 
                        u.last_name, 
                        u.middle_name, 
                        u.email, 
                        u.phone, 
                        u.birthday, 
                        u.is_blocked,
                        p.is_active,
                        topo.name AS work_place,
                        pos.name AS position,
                        p.code,
                        fp.image_path AS photo_path,
                        MIN(wt.in_time) AS first_in_time,
                        MAX(wt.out_time) AS last_out_time
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
                    WHERE u.id = %s
                    GROUP BY u.id, p.id, topo.name, pos.name, fp.image_path
                    ORDER BY p.is_active DESC, p.created_at DESC
                    LIMIT 1
                """, (today, today, user_id))
            
            user = cursor.fetchone()
        
        if not user:
            return jsonify({'error': 'User not found'}), 404
        
        # Обработка времени прихода/ухода
        in_time = user['first_in_time']
        out_time = user['last_out_time']
        
        # Если приход сегодня, а уход не сегодня (или нет ухода), то уход не показываем
        if in_time and in_time.date() == today and (not out_time or out_time.date() != today):
            out_time = None
        
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
            'birthday': user['birthday'].isoformat() if user['birthday'] else None,
            'is_blocked': user['is_blocked'],
            'is_active': user['is_active'], 
            'work_place': user['work_place'] or '',
            'position': user['position'] or '',
            'card_code': user['code'] or '',
            'photo_path': user['photo_path'] or '',
            'full_name': full_name,
            'in_time': in_time.isoformat() if in_time else None,
            'out_time': out_time.isoformat() if out_time else None
        }
        
        logger.debug(f"Returning user data for user_id: {user_id}, full_name: {full_name}, is_active: {user_data.get('is_active')}, card_code: {card_code}")
        return jsonify(user_data)
        
    except Exception as e:
        logger.error(f"Error in api_profile: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        if conn:
            release_db_connection(conn)

@app.route('/api/get_personal_data', methods=['GET'])
def get_personal_data():
    """Получение персональных данных из 1С"""
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

@app.route('/api/get_payslip', methods=['POST'])
def get_payslip():
    """Получение расчетного листа"""
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
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            url = f"{app.config['ONE_C_PAYSLIP_URL']}?id_1c={one_c_id}&Date={date_param}"
            logger.info(f"Requesting payslip for 1C_ID: {one_c_id}, Date: {date_param}")
            
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            logger.info(f"1C response status: {response.status_code}")
            
            if response.status_code == 200:
                if response.content[:4] == b'%PDF':
                    return Response(
                        response.content,
                        status=200,
                        mimetype='application/pdf'
                    )
                
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
                
                if response.text.startswith('%PDF'):
                    return Response(
                        response.text.encode(),
                        status=200,
                        mimetype='application/pdf'
                    )
                
                error_text = response.text[:500] if response.text else response.content[:500].decode('utf-8', 'ignore')
                logger.error(f"1C returned unrecognized content: {error_text}")
                return jsonify({
                    'error': '1C service returned unrecognized content',
                    'message': error_text
                }), 502
            else:
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

@app.route('/api/get_vacations', methods=['POST'])
def get_vacations():
    """Получение данных об отпусках в PDF"""
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
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            url = f"{app.config['ONE_C_VACATIONS_URL']}?id_1c={one_c_id}&Date={year_int}0101&view=report"
            logger.info(f"Requesting vacations for 1C_ID: {one_c_id}, Year: {year_int}")
            
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            logger.info(f"1C vacations response status: {response.status_code}")
            
            if response.status_code == 200:
                if response.headers.get('Content-Type') == 'application/pdf' or response.content[:4] == b'%PDF':
                    return Response(
                        response.content,
                        status=200,
                        mimetype='application/pdf'
                    )
                
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
                
                error_text = response.text[:500] if response.text else response.content[:500].decode('utf-8', 'ignore')
                logger.error(f"1C returned unrecognized content: {error_text}")
                return jsonify({
                    'error': '1C service returned unrecognized content',
                    'message': error_text
                }), 502
            else:
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

@app.route('/api/get_vacations_info', methods=['GET'])
def get_vacations_info():
    """Получение информации об отпусках в JSON"""
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
                    'is_json': False,
                    'structured_data': None
                }
                
                try:
                    decoded_content = base64.b64decode(content).decode('utf-8')
                    result['content'] = decoded_content
                    result['is_base64'] = True
                except:
                    result['content'] = content
                
                try:
                    vacation_data = parse_1c_vacation_data(result['content'])
                    result['structured_data'] = vacation_data
                    result['has_structured_data'] = True
                except Exception as e:
                    logger.error("Error parsing vacation data: %s", str(e))
                    result['has_structured_data'] = False
                
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

def parse_1c_vacation_data(content):
    """Парсит данные об отпусках из 1С системы"""
    vacation_data = {
        'days_left': None,
        'vacation_dates': None,
        'vacation_days': None
    }
    
    try:
        if content.strip().startswith('{') or content.strip().startswith('['):
            data = json.loads(content)
            vacation_data['days_left'] = data.get('days_until_vacation')
            vacation_data['vacation_dates'] = data.get('next_vacation_date')
            vacation_data['vacation_days'] = data.get('vacation_duration')
        
        else:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
                    row_text = ' '.join(cells).lower()
                    
                    if 'осталось' in row_text and 'дней' in row_text:
                        numbers = re.findall(r'\d+', row_text)
                        if numbers:
                            vacation_data['days_left'] = numbers[0]
                    
                    elif 'дата' in row_text and 'отпуск' in row_text:
                        dates = re.findall(r'\d{2}\.\d{2}\.\d{4}', row_text)
                        if dates:
                            vacation_data['vacation_dates'] = dates[0]
                    
                    elif 'продолжительность' in row_text and 'дней' in row_text:
                        numbers = re.findall(r'\d+', row_text)
                        if numbers:
                            vacation_data['vacation_days'] = numbers[0]
    
    except Exception as e:
        logger.error("Error in parse_1c_vacation_data: %s", str(e))
    
    return vacation_data

@app.route('/api/get_timesheet', methods=['POST'])
def get_timesheet():
    """Получение графика работы"""
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
            cursor.execute("""
                SELECT "1c_id" 
                FROM user_integration_relations 
                WHERE user_id = %s
            """, (user_id,))
            relation = cursor.fetchone()
            
            if not relation or not relation['1c_id']:
                return jsonify({'error': '1C integration not found'}), 404
            
            one_c_id = relation['1c_id']
            
            url = f"{app.config['ONE_C_TIMESHEET_URL']}?id_1c={one_c_id}&Date={date_param}"
            logger.info(f"Requesting timesheet for 1C_ID: {one_c_id}, Date: {date_param}")
            
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)
            
            logger.info(f"1C timesheet response status: {response.status_code}")
            logger.info(f"1C response content-type: {response.headers.get('content-type', 'unknown')}")
            logger.info(f"1C response length: {len(response.content)} bytes")
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                is_pdf = (
                    response.content[:4] == b'%PDF' or
                    response.text.startswith('%PDF') or
                    content_type.find('pdf') != -1 or
                    response.text.startswith(('JVBER', 'UEsDB', 'iVBOR'))
                )
                
                if is_pdf:
                    if response.content[:4] == b'%PDF':
                        logger.info("Returning binary PDF")
                        return Response(
                            response.content,
                            status=200,
                            mimetype='application/pdf',
                            headers={'Content-Disposition': f'attachment; filename="timesheet_{year}_{month}.pdf"'}
                        )
                    
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
                            return Response(
                                response.text,
                                status=200,
                                mimetype='text/plain',
                                headers={'Content-Disposition': f'attachment; filename="timesheet_error_{year}_{month}.txt"'}
                            )
                    
                    if response.text.startswith('%PDF'):
                        logger.info("Returning text PDF")
                        return Response(
                            response.text.encode(),
                            status=200,
                            mimetype='application/pdf',
                            headers={'Content-Disposition': f'attachment; filename="timesheet_{year}_{month}.pdf"'}
                        )
                
                logger.info("1C returned text content instead of PDF")
                
                try:
                    text_content = response.content.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        text_content = response.content.decode('windows-1251')
                    except UnicodeDecodeError:
                        text_content = response.content.decode('utf-8', errors='replace')
                
                logger.info(f"Text content preview: {text_content[:500]}")
                
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

#________________________________________________________________________________________________
# API для истории входов
@app.route('/api/access_history', methods=['GET'])
def api_access_history():
    """Получение истории входов"""
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    logger.info(f"API call for user_id: {session.get('user_id')}")
    conn = get_db_connection()
    if not conn:
        logger.error("DB connection failed")
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        with conn.cursor() as cursor:
            logger.info("Executing SELECT access_history...")
            cursor.execute("""
                SELECT access_history FROM telegram_bindings 
                WHERE user_id = %s AND card_code = %s
            """, (session['user_id'], session.get('card_code')))
            result = cursor.fetchone()
            
            if not result:
                logger.info("No access history found for user")
                return jsonify([])
            
            history_raw = result['access_history'] if result and result['access_history'] else ''
            
            # Обрабатываем разные форматы хранения истории
            if isinstance(history_raw, str):
                try:
                    history = json.loads(history_raw)
                except Exception as e:
                    logger.error(f"Cannot parse access_history JSON: {str(e)}")
                    history = []
            elif isinstance(history_raw, list):
                history = history_raw
            else:
                history = []
            
            # Сортировка от новых к старым
            if history:
                def sort_key(entry):
                    try:
                        return datetime.fromisoformat(entry['date'])
                    except (ValueError, KeyError):
                        return datetime.min
                
                history = sorted(history, key=sort_key, reverse=True)
            
            logger.info(f"History fetched and sorted: {len(history)} entries")
            return jsonify(history)
            
    except Exception as e:
        logger.error(f"Error fetching access history: {str(e)}")
        return jsonify({'error': f'Ошибка сервера: {str(e)}'}), 500
    finally:
        release_db_connection(conn)

@app.before_request
def log_access():
    """Логирование доступа пользователей - исправленная версия"""
    # Логируем только после успешной авторизации
    if 'user_id' not in session or 'card_code' not in session:
        return

    # Получаем базовую информацию для логирования
    user_id = session['user_id']
    card_code = session['card_code']
    ip = request.remote_addr or 'Unknown'
    user_agent = request.headers.get('User-Agent', 'Unknown')[:500]
    endpoint = request.endpoint or 'unknown'

    # Исключаем только действительно ненужные эндпоинты
    excluded_endpoints = [
        'static', 'api_session_status', 'api_update_activity', 
        'api_sync_session', 'api_auth_status'
    ]
    
    if endpoint in excluded_endpoints:
        return

    # Фильтр ботов по User-Agent (более мягкий)
    user_agent_lower = user_agent.lower()
    bot_indicators = ['bot', 'spider', 'crawler', 'monitoring']
    if any(indicator in user_agent_lower for indicator in bot_indicators):
        return

    conn = get_db_connection()
    if not conn:
        logger.error("Cannot get DB connection for log_access")
        return

    try:
        with conn.cursor() as cursor:
            # Получаем текущую историю доступа
            cursor.execute("""
                SELECT access_history FROM telegram_bindings 
                WHERE card_code = %s
            """, (card_code,))
            result = cursor.fetchone()

            # Получаем историю доступа или создаем пустой список
            history = []
            if result and result['access_history']:
                history_raw = result['access_history']
                if isinstance(history_raw, str):
                    try:
                        history = json.loads(history_raw)
                    except Exception:
                        history = []
                elif isinstance(history_raw, list):
                    history = history_raw

            # Создаем новую запись
            now = datetime.now()
            new_entry = {
                'date': now.isoformat(),
                'ip': ip,
                'user_agent': user_agent,
                'endpoint': endpoint
            }

            # Упрощенная проверка на дубликаты - только по времени (за последнюю минуту)
            recent_time_threshold = now - timedelta(minutes=1)
            recent_duplicate = False
            
            for entry in history[-5:]:  # Проверяем только последние 5 записей
                try:
                    entry_time = datetime.fromisoformat(entry['date'])
                    if entry_time >= recent_time_threshold:
                        recent_duplicate = True
                        break
                except (ValueError, KeyError):
                    continue

            if recent_duplicate:
                logger.debug(f"Skipping duplicate log entry for user {user_id}")
                return

            # Добавляем новую запись
            history.append(new_entry)

            # Храним только последние 20 записей (увеличил лимит)
            history = history[-10:]

            # Обновляем базу данных
            if result:
                cursor.execute("""
                    UPDATE telegram_bindings
                    SET access_history = %s, user_id = %s
                    WHERE card_code = %s
                """, (json.dumps(history), user_id, card_code))
            else:
                # Сначала получаем pass_id для данной карты
                formatted_code = f"{card_code[:3]},{card_code[3:]}"
                cursor.execute("""
                    SELECT id FROM pass WHERE code = %s OR code = %s
                """, (card_code, formatted_code))
                pass_row = cursor.fetchone()
                
                if pass_row:
                    cursor.execute("""
                        INSERT INTO telegram_bindings (pass_id, card_code, user_id, access_history)
                        VALUES (%s, %s, %s, %s)
                    """, (pass_row[0], card_code, user_id, json.dumps([new_entry])))
            
            conn.commit()
            logger.info(f"Access logged for user {user_id} from {ip} to {endpoint}")

    except Exception as e:
        logger.error(f"Error logging access: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)


#________________________________________________________________________________________________
# Функции для работы с Excel файлами
def load_excel_data(filepath):
    """Загрузка данных из Excel файла"""
    try:
        if not os.path.exists(filepath):
            logger.error(f"Excel file not found: {filepath}")
            return []
        
        # Читаем Excel файл с явным указанием engine для .xlsx файлов
        try:
            df = pd.read_excel(filepath, dtype=str, engine='openpyxl')  # Все данные как строки
        except Exception as e:
            logger.warning(f"Failed with openpyxl engine: {str(e)}, trying default engine")
            df = pd.read_excel(filepath, dtype=str)
        
        # Логируем названия столбцов
        logger.debug(f"Columns in {filepath}: {list(df.columns)}")
        
        # Логируем первую строку ДО fillna
        if len(df) > 0:
            first_row_before = df.iloc[0].to_dict()
            logger.debug(f"First row BEFORE fillna: {first_row_before}")
        
        # Заменяем NaN на пустые строки
        df = df.fillna('')
        
        # Логируем первую строку ПОСЛЕ fillna
        if len(df) > 0:
            first_row_after = df.iloc[0].to_dict()
            logger.debug(f"First row AFTER fillna: {first_row_after}")
        
        # Преобразуем в список словарей
        data = df.to_dict('records')
        
        # Логируем первый элемент результата
        if data:
            logger.debug(f"First record in result: {data[0]}")
        
        logger.info(f"Loaded {len(data)} records from {filepath}")
        return data
        
    except Exception as e:
        logger.error(f"Error loading Excel file {filepath}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def get_image_path(cell_value, base_path='img'):
    """Извлечение пути к изображению из ячейки Excel"""
    if not cell_value:
        return None
    
    # Если это строка с путем
    if isinstance(cell_value, str):
        # Очищаем путь
        path = cell_value.strip()
        logger.debug(f"get_image_path input: {repr(path)}")
        
        # Если путь уже полный, возвращаем как есть
        if path.startswith('http'):
            return path
        
        # Заменяем обратные слеши на прямые
        path = path.replace('\\', '/')
        logger.debug(f"get_image_path after replace: {repr(path)}")
        
        # Проверяем, есть ли уже префикс img/ и не добавляем его дважды
        if path.startswith('img/'):
            final_path = path
        elif path.startswith('img'):
            final_path = path
        else:
            final_path = f"{base_path}/{path}"
        
        logger.debug(f"get_image_path final_path: {repr(final_path)}")
        
        # Проверяем существование файла
        if os.path.exists(final_path):
            result = f"/static/{final_path}"
            logger.debug(f"get_image_path file EXISTS, returning: {repr(result)}")
            return result
        
        # Если файл не найден, возвращаем путь как есть
        result = f"/static/{final_path}"
        logger.debug(f"get_image_path file NOT FOUND, returning: {repr(result)}")
        return result
    
    # Если это объект с гиперссылкой (в зависимости от парсера Excel)
    elif isinstance(cell_value, dict):
        # Пытаемся извлечь ссылку из объекта
        link = cell_value.get('hyperlink') or cell_value.get('link') or cell_value.get('url')
        if link:
            return get_image_path(link, base_path)
    
    return None

#________________________________________________________________________________________________
#________________________________________________________________________________________________
# API для приобретения продукции
@app.route('/api/new_products', methods=['GET'])
def api_new_products():
    """API для получения новой продукции из new.xlsx"""
    logger.info(f"API /api/new_products called, session keys: {list(session.keys())}")
    
    if 'user_id' not in session:
        logger.warning("Unauthorized access attempt to /api/new_products")
        return jsonify({'error': 'Unauthorized'}), 401
    
    logger.info(f"User {session.get('user_id')} requesting new products")
    
    # Загружаем данные из Excel файла
    excel_data = load_excel_data('new.xlsx')
    
    if not excel_data:
        logger.warning("No data loaded from new.xlsx, returning empty list")
        # Возвращаем пустой список, если файл не найден
        return jsonify([])
    
    logger.info(f"Processing {len(excel_data)} rows from new.xlsx")
    
    new_products = []
    for idx, row in enumerate(excel_data):
        # Логируем каждую строку для отладки
        tovar_value = row.get('Модель', '')  # Столбец называется 'Модель', а не 'Товар'!
        logger.debug(f"Row {idx}: Товар={repr(tovar_value)}, type={type(tovar_value)}, stripped={repr(tovar_value.strip() if isinstance(tovar_value, str) else tovar_value)}")
        
        # Пропускаем пустые строки
        if not row.get('Модель', '').strip():  # Используем 'Модель'
            logger.debug(f"Skipping empty row {idx}")
            continue
        
        # Обрабатываем название столбца 'Фото' (может быть с пробелом)
        photo_col = 'Фото' if 'Фото' in row else 'Фото '
        # Обрабатываем название столбца 'Дата' (может быть с пробелом)
        date_col = 'Дата ' if 'Дата ' in row else 'Дата'
        
        product_item = {
            'id': idx + 1,
            'product': row.get('Модель', '').strip(),  # Используем 'Модель'
            'description': row.get('Описание', '').strip(),
            'date': row.get(date_col, '').strip() if row.get(date_col) else '',
            'amount': row.get('Колличество', ''),  # Используем 'Колличество' (с 2 Л)
            'price': row.get('Цена', ''),
            'photo': get_image_path(row.get(photo_col, '').strip() if row.get(photo_col) else None)
        }
        
        # Преобразуем количество в число, если это строка
        try:
            if product_item['amount']:
                product_item['amount'] = int(float(str(product_item['amount']).strip()))
            else:
                product_item['amount'] = 0
        except (ValueError, TypeError):
            product_item['amount'] = 0
        
        # Преобразуем цену в число, если это строка
        try:
            if product_item['price']:
                product_item['price'] = float(str(product_item['price']).strip())
            else:
                product_item['price'] = 0
        except (ValueError, TypeError):
            product_item['price'] = 0
        
        # Форматируем дату в формат DD.MM.YYYY
        try:
            if product_item['date']:
                date_str = str(product_item['date']).strip()
                # Если это дата в формате YYYY-MM-DD HH:MM:SS, преобразуем в DD.MM.YYYY
                if ' ' in date_str:
                    date_str = date_str.split(' ')[0]
                if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
                    # YYYY-MM-DD -> DD.MM.YYYY
                    parts = date_str.split('-')
                    product_item['date'] = f"{parts[2]}.{parts[1]}.{parts[0]}"
        except Exception as e:
            logger.error(f"Error formatting date: {e}")
        
        new_products.append(product_item)
        logger.debug(f"Added product: {product_item['product']}")
    
    logger.info(f"Returning {len(new_products)} products from /api/new_products")
    return jsonify(new_products)

@app.route('/api/discount_products', methods=['GET'])
def api_discount_products():
    """API для получения уцененной продукции из remont.xlsx"""
    logger.info(f"API /api/discount_products called, session keys: {list(session.keys())}")
    
    if 'user_id' not in session:
        logger.warning("Unauthorized access attempt to /api/discount_products")
        return jsonify({'error': 'Unauthorized'}), 401
    
    logger.info(f"User {session.get('user_id')} requesting discount products")
    
    # Загружаем данные из Excel файла
    excel_data = load_excel_data('remont.xlsx')
    
    if not excel_data:
        logger.warning("No data loaded from remont.xlsx, returning empty list")
        # Возвращаем пустой список, если файл не найден
        return jsonify([])
    
    logger.info(f"Processing {len(excel_data)} rows from remont.xlsx")
    
    discount_products = []
    for idx, row in enumerate(excel_data):
        # Логируем каждую строку для отладки
        tovar_value = row.get('Название', '')  # Столбец называется 'Название', а не 'Товар'!
        logger.debug(f"Row {idx}: Товар={repr(tovar_value)}, type={type(tovar_value)}, stripped={repr(tovar_value.strip() if isinstance(tovar_value, str) else tovar_value)}")
        
        # Пропускаем пустые строки
        if not row.get('Название', '').strip():  # Используем 'Название'
            logger.debug(f"Skipping empty row {idx}")
            continue
        
        product_item = {
            'id': 100 + idx + 1,
            'product': row.get('Название', '').strip(),  # Используем 'Название' для remont.xlsx
            'description': row.get('Описание', '').strip(),
            'defect': row.get('Неисправность/устранение', '').strip(),
            'date': row.get('Дата', '').strip() if row.get('Дата') else '',
            'amount': row.get('Колличество', 1),  # Количество по умолчанию 1 так как в remont.xlsx нет этого столбца
            'price': row.get('Цена', ''),
            'photo': get_image_path(row.get('Фото', '').strip() if row.get('Фото') else None)
        }
        
        # Преобразуем количество в число, если это строка
        try:
            if product_item['amount']:
                product_item['amount'] = int(float(str(product_item['amount']).strip()))
            else:
                product_item['amount'] = 0
        except (ValueError, TypeError):
            product_item['amount'] = 0
        
        # Преобразуем цену в число, если это строка
        try:
            if product_item['price']:
                product_item['price'] = float(str(product_item['price']).strip())
            else:
                product_item['price'] = 0
        except (ValueError, TypeError):
            product_item['price'] = 0
        
        # Форматируем дату в формат DD.MM.YYYY
        try:
            if product_item['date']:
                date_str = str(product_item['date']).strip()
                # Если это дата в формате YYYY-MM-DD HH:MM:SS, преобразуем в DD.MM.YYYY
                if ' ' in date_str:
                    date_str = date_str.split(' ')[0]
                if len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
                    # YYYY-MM-DD -> DD.MM.YYYY
                    parts = date_str.split('-')
                    product_item['date'] = f"{parts[2]}.{parts[1]}.{parts[0]}"
        except Exception as e:
            logger.error(f"Error formatting date: {e}")
        
        discount_products.append(product_item)
        logger.debug(f"Added discount product: {product_item['product']}")
    
    logger.info(f"Returning {len(discount_products)} products from /api/discount_products")
    return jsonify(discount_products)

@app.route('/api/confirm_order', methods=['POST'])
def api_confirm_order():
    """API для подтверждения заказа"""
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.json
    order_items = data.get('items', [])
    payment_method = data.get('payment_method', 'cash')
    
    if not order_items:
        return jsonify({'success': False, 'error': 'Нет товаров в заказе'}), 400
    
    total_amount = sum(item.get('price', 0) * item.get('quantity', 1) for item in order_items)
    order_number = f"ORD-{datetime.now().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    
    logger.info(f"Order confirmed: {order_number}, Total: {total_amount}, Payment: {payment_method}")
    
    return jsonify({
        'success': True,
        'order_number': order_number,
        'total_amount': total_amount,
        'payment_method': payment_method,
        'message': 'Заказ успешно оформлен'
    })

#________________________________________________________________________________________________
# Вспомогательные эндпоинты
@app.route('/api/update_activity', methods=['POST'])
def update_activity():
    """Обновление активности сессии"""
    if 'sid' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    session['last_activity'] = datetime.now().isoformat()
    return jsonify({'success': True})

@app.route('/api/sync_session', methods=['GET'])
def api_sync_session():
    """API для принудительной синхронизации сессии"""
    if 'user_id' in session:
        session['last_activity'] = datetime.now().isoformat()
        session.modified = True
        
        return jsonify({
            'success': True,
            'user_id': session['user_id'],
            'card_code': session.get('card_code'),
            'message': 'Session synchronized'
        })
    return jsonify({'success': False, 'error': 'No active session'})

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Origin, Accept'
    
    if 'sid' in session:
        response.set_cookie(
            'darina_session',
            value=session['sid'],
            httponly=True,
            secure=False,
            samesite='Lax',
            max_age=86400,
            path='/'
        )
    return response

#________________________________________________________________________________________________
# Дополнительный фильтр для Flask/Werkzeug
class FlaskAccessFilter(logging.Filter):
    """Фильтр для доступа к Flask чтобы скрыть успешные запросы"""
    
    def filter(self, record):
        message = record.getMessage()
        
        # Показываем только ошибки (4xx, 5xx)
        if record.levelno >= logging.ERROR:
            return True
            
        # Скрываем успешные запросы (200, 301, 302 и т.д.)
        if '"GET' in message or '"POST' in message or '"PUT' in message or '"DELETE' in message:
            # Показываем только запросы с ошибками
            if any(code in message for code in ['" 404', '" 500', '" 403', '" 401', '" 400']):
                return True
            return False
            
        return True

# Применяем фильтр доступа к Werkzeug
werkzeug_logger = logging.getLogger('werkzeug')
for handler in werkzeug_logger.handlers:
    handler.addFilter(FlaskAccessFilter())


#________________________________________________________________________________________________
if __name__ == '__main__':
    init_tables()
    if app.config.get('VK_BOT_TOKEN') and app.config.get('VK_GROUP_ID'):
        start_vk_bot()
    app.run(host='192.168.210.201', port=5050, debug=True)