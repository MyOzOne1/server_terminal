from flask import Flask, render_template, session, redirect, url_for, request, jsonify, Response, g
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
import asyncio
from bs4 import BeautifulSoup
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
import mimetypes
import shutil
import subprocess
import tempfile
import sys
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from functools import wraps
import random
import string
import threading
from collections import defaultdict, deque
sys.path.append(r'C:\Users\Azyabin\AppData\Local\Programs\Python\Python310\Lib\site-packages')
from flask_cors import CORS
from flask_session import Session
from flask import send_from_directory

try:
    from telegram import Update
    from telegram.ext import Application, CommandHandler, ContextTypes
    from telegram.error import Conflict
    TELEGRAM_BOT_AVAILABLE = True
except Exception:
    TELEGRAM_BOT_AVAILABLE = False

try:
    from docx import Document as DocxDocument
    DOCX_AVAILABLE = True
except Exception:
    DOCX_AVAILABLE = False

try:
    import textract
    TEXTRACT_AVAILABLE = True
except Exception:
    TEXTRACT_AVAILABLE = False

app = Flask(__name__)
CORS(app, supports_credentials=True, resources={r"/*": {"origins": "*"}})
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Настройка сессии Flask
app.config.update(
    SECRET_KEY='your-secret-key-here-change-this-in-production',
    ADMIN_LOGIN='admin',
    ADMIN_PASSWORD='admin',
    RUN_HOST='192.168.210.201',
    RUN_PORT=5050,
    RUN_DEBUG=True,
    TELEGRAM_BOT_ENABLED=True,
    VK_BOT_ENABLED=True,
    FAMILIARIZATION_DOCS_SOURCE='path',
    FAMILIARIZATION_DOCS_PATH='./documents/familiarization',
    FAMILIARIZATION_DOCS_URL='',
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

CONFIG_OVERRIDE_PATH = os.path.join(BASE_DIR, 'config_overrides.json')
ADMIN_CONFIG_KEYS = [
    'ADMIN_LOGIN',
    'ADMIN_PASSWORD',
    'RUN_HOST',
    'RUN_PORT',
    'RUN_DEBUG',
    'TELEGRAM_BOT_ENABLED',
    'VK_BOT_ENABLED',
    'FAMILIARIZATION_DOCS_SOURCE',
    'FAMILIARIZATION_DOCS_PATH',
    'FAMILIARIZATION_DOCS_URL',
    'ONE_C_USERNAME',
    'ONE_C_PASSWORD',
    'ONE_C_PAYSLIP_URL',
    'ONE_C_VACATIONS_URL',
    'ONE_C_PERSONAL_DATA_URL',
    'ONE_C_TIMESHEET_URL',
    'POSTGRES_HOST',
    'POSTGRES_PORT',
    'POSTGRES_DB',
    'POSTGRES_USER',
    'POSTGRES_PASSWORD',
    'TELEGRAM_BOT_TOKEN',
    'VK_BOT_TOKEN',
    'VK_GROUP_ID',
    'SESSION_COOKIE_NAME',
    'SESSION_COOKIE_HTTPONLY',
    'SESSION_COOKIE_SECURE',
    'SESSION_COOKIE_SAMESITE',
    'PERMANENT_SESSION_LIFETIME',
    'SESSION_REFRESH_EACH_REQUEST',
    'SESSION_TYPE',
    'SESSION_FILE_DIR',
    'SESSION_PERMANENT',
    'SECRET_KEY'
]
ADMIN_BOOL_KEYS = {
    'RUN_DEBUG',
    'TELEGRAM_BOT_ENABLED',
    'VK_BOT_ENABLED',
    'SESSION_COOKIE_HTTPONLY',
    'SESSION_COOKIE_SECURE',
    'SESSION_REFRESH_EACH_REQUEST',
    'SESSION_PERMANENT'
}
ADMIN_INT_KEYS = {
    'POSTGRES_PORT',
    'PERMANENT_SESSION_LIFETIME',
    'RUN_PORT'
}

def parse_admin_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}

def normalize_card_code(value):
    digits = ''.join(ch for ch in str(value or '').strip() if ch.isdigit())
    return digits

def format_card_code(value):
    normalized = normalize_card_code(value)
    if len(normalized) > 3:
        return f"{normalized[:3]},{normalized[3:]}"
    return normalized

def card_code_variants(value):
    normalized = normalize_card_code(value)
    if not normalized:
        return []
    formatted = format_card_code(normalized)
    if formatted == normalized:
        return [normalized]
    return [normalized, formatted]

def serialize_config_value(key, value):
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    return value

def deserialize_config_value(key, value):
    if key == 'PERMANENT_SESSION_LIFETIME':
        try:
            return timedelta(seconds=int(value))
        except (TypeError, ValueError):
            return app.config.get('PERMANENT_SESSION_LIFETIME', timedelta(hours=2))
    if key in ADMIN_BOOL_KEYS:
        return parse_admin_bool(value)
    if key in ADMIN_INT_KEYS:
        try:
            return int(value)
        except (TypeError, ValueError):
            return app.config.get(key)
    return value

def update_one_c_auth_header():
    auth_str = f"{app.config.get('ONE_C_USERNAME', '')}:{app.config.get('ONE_C_PASSWORD', '')}"
    app.config['ONE_C_AUTH_HEADER'] = 'Basic ' + b64encode(auth_str.encode('utf-8')).decode('ascii')

def load_config_overrides():
    if not os.path.exists(CONFIG_OVERRIDE_PATH):
        return
    try:
        with open(CONFIG_OVERRIDE_PATH, 'r', encoding='utf-8') as file_handle:
            overrides = json.load(file_handle)
        for key in ADMIN_CONFIG_KEYS:
            if key in overrides:
                app.config[key] = deserialize_config_value(key, overrides[key])
        update_one_c_auth_header()
    except Exception as e:
        logging.getLogger(__name__).warning("Failed to load config overrides: %s", e)

def get_admin_config():
    return {key: serialize_config_value(key, app.config.get(key)) for key in ADMIN_CONFIG_KEYS}

def save_config_overrides(overrides):
    stored = {}
    if os.path.exists(CONFIG_OVERRIDE_PATH):
        try:
            with open(CONFIG_OVERRIDE_PATH, 'r', encoding='utf-8') as file_handle:
                stored = json.load(file_handle)
        except Exception:
            stored = {}
    stored.update(overrides)
    with open(CONFIG_OVERRIDE_PATH, 'w', encoding='utf-8') as file_handle:
        json.dump(stored, file_handle, ensure_ascii=True, indent=2)


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
load_config_overrides()
update_one_c_auth_header()
session_store = Session()
session_store.init_app(app)

DEFAULT_FAMILIARIZATION_DOCS_DIR = os.path.join(BASE_DIR, 'documents', 'familiarization')
FAMILIARIZATION_LOG_DIR = os.path.join(BASE_DIR, 'documents', 'familiarization_logs')
ALLOWED_FAMILIARIZATION_EXTS = {'.pdf', '.docx', '.doc'}
os.makedirs(FAMILIARIZATION_LOG_DIR, exist_ok=True)

# Глобальные переменные для VK бота
vk_bot_thread = None
vk_session = None
vk = None
longpoll = None
vk_bot_stop_event = threading.Event()
vk_bot_error = None

telegram_bot_thread = None
telegram_bot_loop = None
telegram_bot_app = None
telegram_bot_error = None
telegram_bot_stop_event = threading.Event()

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
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('telegram.ext').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

activity_logger = logging.getLogger('activity')
activity_logger.setLevel(logging.INFO)
activity_handler = logging.FileHandler("activity.log")
activity_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
activity_logger.addHandler(activity_handler)
activity_logger.propagate = False

def log_activity_event(event, **fields):
    parts = [f"event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        safe_value = str(value).replace(' ', '_')
        parts.append(f"{key}={safe_value}")
    activity_logger.info("METRIC " + " ".join(parts))

def get_client_ip():
    forwarded_for = request.headers.get('X-Forwarded-For', '')
    if forwarded_for:
        return forwarded_for.split(',')[0].strip()
    return request.remote_addr or 'unknown'

def schedule_server_restart(delay_seconds=1.0):
    def _shutdown():
        time.sleep(delay_seconds)
        os._exit(0)

    threading.Thread(target=_shutdown, daemon=True).start()

#________________________________________________________________________________________________
# Пул соединений PostgreSQL
postgres_pool = None

def create_postgres_pool():
    return psycopg2.pool.SimpleConnectionPool(
        5, 30,
        host=app.config['POSTGRES_HOST'],
        port=app.config['POSTGRES_PORT'],
        database=app.config['POSTGRES_DB'],
        user=app.config['POSTGRES_USER'],
        password=app.config['POSTGRES_PASSWORD']
    )

try:
    postgres_pool = create_postgres_pool()
    logger.info("PostgreSQL connection pool created")
except Exception as e:
    logger.error("Error creating PostgreSQL connection pool: %s", e)

def refresh_postgres_pool():
    global postgres_pool
    try:
        if postgres_pool:
            postgres_pool.closeall()
    except Exception as e:
        logger.error("Error closing PostgreSQL connection pool: %s", e)
    try:
        postgres_pool = create_postgres_pool()
        logger.info("PostgreSQL connection pool refreshed")
    except Exception as e:
        logger.error("Error refreshing PostgreSQL connection pool: %s", e)

codes = {}  # {card_code: (code, timestamp)}
sid_auth_cache = {}  # {sid: {user_id, card_code, channel, expires_at}}
recent_client_auth_cache = {}  # {client_key: {sid, user_id, card_code, channel, expires_at}}

def _client_auth_key():
    ip = get_client_ip() if request else 'unknown'
    ua = (request.headers.get('User-Agent', '') if request else '')[:200]
    return f"{ip}|{ua}"

def _auth_ttl_seconds():
    lifetime = app.config.get('PERMANENT_SESSION_LIFETIME', timedelta(hours=2))
    if isinstance(lifetime, timedelta):
        return max(60, int(lifetime.total_seconds()))
    try:
        return max(60, int(lifetime))
    except Exception:
        return 7200

def cache_auth_session(sid, user_id, card_code, channel):
    if not sid:
        return
    entry = {
        'user_id': user_id,
        'card_code': card_code,
        'channel': channel,
        'expires_at': time.time() + _auth_ttl_seconds()
    }
    sid_auth_cache[sid] = entry

    # Короткоживущий fallback для внешних WebView, где cookie/sid может не доехать.
    client_entry = dict(entry)
    client_entry['sid'] = sid
    client_entry['expires_at'] = time.time() + min(300, _auth_ttl_seconds())
    recent_client_auth_cache[_client_auth_key()] = client_entry

def drop_cached_auth_session(sid):
    if not sid:
        return
    sid_auth_cache.pop(sid, None)
    stale_keys = [key for key, value in recent_client_auth_cache.items() if value.get('sid') == sid]
    for key in stale_keys:
        recent_client_auth_cache.pop(key, None)

def restore_session_from_sid():
    if 'user_id' in session:
        return False

    sid = (request.headers.get('X-Session-ID') or request.args.get('sid') or '').strip()
    if not sid and request.method in {'POST', 'PUT', 'PATCH'}:
        sid = (request.form.get('sid') or '').strip()

    cached = sid_auth_cache.get(sid) if sid else None

    if not cached:
        client_key = _client_auth_key()
        client_cached = recent_client_auth_cache.get(client_key)
        if client_cached and client_cached.get('expires_at', 0) >= time.time():
            sid = client_cached.get('sid')
            cached = {
                'user_id': client_cached.get('user_id'),
                'card_code': client_cached.get('card_code'),
                'channel': client_cached.get('channel'),
                'expires_at': client_cached.get('expires_at')
            }
            if sid:
                sid_auth_cache[sid] = dict(cached)
        elif client_cached:
            recent_client_auth_cache.pop(client_key, None)

    if not sid or not cached:
        return False

    if cached.get('expires_at', 0) < time.time():
        sid_auth_cache.pop(sid, None)
        return False

    session.permanent = True
    session['user_id'] = cached.get('user_id')
    session['sid'] = sid
    session['card_code'] = cached.get('card_code')
    session['channel'] = cached.get('channel')
    session['last_activity'] = datetime.now().isoformat()
    session.modified = True
    return True

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

def get_user_full_name(user_id):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT first_name, last_name, middle_name
                FROM users
                WHERE id = %s
                """,
                (user_id,)
            )
            row = cursor.fetchone()
        if not row:
            return None
        parts = [row.get('last_name'), row.get('first_name'), row.get('middle_name')]
        return ' '.join(part for part in parts if part)
    except Exception as e:
        logger.error("Error fetching user name: %s", e)
        return None
    finally:
        release_db_connection(conn)

def get_familiarization_source():
    return str(app.config.get('FAMILIARIZATION_DOCS_SOURCE', 'path')).strip().lower()

def get_familiarization_docs_dir():
    configured = app.config.get('FAMILIARIZATION_DOCS_PATH')
    path = configured or DEFAULT_FAMILIARIZATION_DOCS_DIR
    abs_path = os.path.abspath(path)
    os.makedirs(abs_path, exist_ok=True)
    return abs_path

def parse_document_timestamp(value):
    if not value:
        return 0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
    except Exception:
        return 0

def normalize_remote_documents(payload):
    if isinstance(payload, dict):
        items = payload.get('documents', [])
    elif isinstance(payload, list):
        items = payload
    else:
        return []

    documents = []
    for item in items:
        if not isinstance(item, dict):
            continue
        file_url = item.get('url') or item.get('download_url')
        if not file_url:
            continue
        path_name = os.path.basename(urlparse(file_url).path)
        ext = (item.get('ext') or os.path.splitext(path_name)[1] or '').lower()
        if ext and ext not in ALLOWED_FAMILIARIZATION_EXTS:
            continue
        doc_id = item.get('id') or path_name or item.get('name')
        name = item.get('name') or os.path.splitext(path_name)[0] or doc_id
        date_value = item.get('date') or item.get('updated_at') or item.get('created_at')
        timestamp = parse_document_timestamp(date_value)
        documents.append({
            'id': str(doc_id),
            'name': name,
            'filename': path_name or str(doc_id),
            'ext': ext,
            'date': date_value,
            'timestamp': timestamp,
            'url': file_url,
            'text_url': item.get('text_url')
        })
    documents.sort(key=lambda item: item['timestamp'], reverse=True)
    return documents

def list_familiarization_documents():
    documents = []
    source = get_familiarization_source()

    if source == 'url':
        url = str(app.config.get('FAMILIARIZATION_DOCS_URL', '')).strip()
        if not url:
            return []
        try:
            response = requests.get(url, timeout=8)
            response.raise_for_status()
            payload = response.json()
            return normalize_remote_documents(payload)
        except Exception as e:
            logger.error("Failed to load remote familiarization list: %s", e)
            return []

    docs_dir = get_familiarization_docs_dir()
    if not os.path.exists(docs_dir):
        return documents

    for entry in os.scandir(docs_dir):
        if not entry.is_file():
            continue
        _, ext = os.path.splitext(entry.name)
        ext = ext.lower()
        if ext not in ALLOWED_FAMILIARIZATION_EXTS:
            continue
        mtime = os.path.getmtime(entry.path)
        documents.append({
            'id': entry.name,
            'name': os.path.splitext(entry.name)[0],
            'filename': entry.name,
            'ext': ext,
            'date': datetime.fromtimestamp(mtime).isoformat(),
            'timestamp': mtime
        })

    documents.sort(key=lambda item: item['timestamp'], reverse=True)
    return documents

def resolve_familiarization_document(doc_id):
    documents = list_familiarization_documents()
    for item in documents:
        if item['id'] == doc_id:
            return item
    return None

def get_document_bytes(document):
    source = get_familiarization_source()
    if source == 'url' and document.get('url'):
        response = requests.get(document['url'], timeout=10)
        response.raise_for_status()
        return response.content

    docs_dir = get_familiarization_docs_dir()
    file_path = os.path.join(docs_dir, document['filename'])
    with open(file_path, 'rb') as file_handle:
        return file_handle.read()

def build_content_disposition(filename):
    ascii_filename = re.sub(r'[^A-Za-z0-9._-]+', '_', filename) or 'document'
    encoded = requests.utils.quote(filename)
    return f'inline; filename="{ascii_filename}"; filename*=UTF-8\'\'{encoded}'

def get_office_converter_path():
    configured = str(app.config.get('LIBREOFFICE_PATH', '') or '').strip()
    if configured and os.path.exists(configured):
        return configured
    for candidate in ('soffice', 'libreoffice', 'soffice.exe'):
        path = shutil.which(candidate)
        if path:
            return path
    return None

def convert_document_to_pdf(document):
    converter = get_office_converter_path()
    if not converter:
        return None, 'Office converter not available'

    filename = document.get('filename') or 'document'
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in {'.doc', '.docx'}:
        return None, 'Unsupported format'

    try:
        content_bytes = get_document_bytes(document)
    except Exception as e:
        return None, f'Failed to read document: {e}'

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, filename)
        with open(input_path, 'wb') as file_handle:
            file_handle.write(content_bytes)

        try:
            result = subprocess.run(
                [converter, '--headless', '--convert-to', 'pdf', '--outdir', temp_dir, input_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False
            )
        except Exception as e:
            return None, f'Conversion failed: {e}'

        if result.returncode != 0:
            return None, 'Conversion failed'

        base_name = os.path.splitext(os.path.basename(input_path))[0]
        output_path = os.path.join(temp_dir, f"{base_name}.pdf")
        if not os.path.exists(output_path):
            pdf_files = [name for name in os.listdir(temp_dir) if name.lower().endswith('.pdf')]
            if not pdf_files:
                return None, 'Converted PDF not found'
            output_path = os.path.join(temp_dir, pdf_files[0])

        with open(output_path, 'rb') as file_handle:
            return file_handle.read(), None

def extract_text_from_pdf(content_bytes):
    try:
        with pdfplumber.open(BytesIO(content_bytes)) as pdf:
            return '\n'.join(page.extract_text() or '' for page in pdf.pages)
    except Exception as e:
        logger.error("Error extracting PDF text: %s", e)
        return ''

def get_familiarization_excel_path(doc_filename):
    base_name = os.path.splitext(doc_filename)[0]
    safe_base = re.sub(r'[\\/]+', '_', base_name)
    return os.path.join(FAMILIARIZATION_LOG_DIR, f"{safe_base}.xlsx")

def normalize_excel_timestamp(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {'nan', 'nat', 'none'}:
        return None
    return text

def load_familiarization_status(doc_filename, user_id, full_name):
    user_id_str = str(user_id) if user_id is not None else ''
    if not full_name and not user_id_str:
        logger.warning(f"load_familiarization_status: no user_id or full_name for {doc_filename}")
        return {'acknowledged': False, 'viewed': False, 'acknowledged_at': None, 'viewed_at': None}
    
    excel_path = get_familiarization_excel_path(doc_filename)
    logger.info(f"load_familiarization_status: doc={doc_filename}, user_id={user_id_str}, full_name={full_name}, excel_path={excel_path}")
    
    if not os.path.exists(excel_path):
        logger.info(f"Excel file does not exist: {excel_path}")
        return {'acknowledged': False, 'viewed': False, 'acknowledged_at': None, 'viewed_at': None}

    try:
        df = pd.read_excel(excel_path, dtype=str, engine='openpyxl')
        logger.info(f"Loaded Excel file with {len(df)} rows, columns: {df.columns.tolist()}")
    except Exception as e:
        logger.error(f"Error reading familiarization log from {excel_path}: {e}")
        return {'acknowledged': False, 'viewed': False, 'acknowledged_at': None, 'viewed_at': None}

    if 'Ф.И.О.' not in df.columns and 'user_id' not in df.columns:
        logger.warning(f"Excel file missing required columns")
        return {'acknowledged': False, 'viewed': False, 'acknowledged_at': None, 'viewed_at': None}

    row = pd.DataFrame()
    if user_id_str and 'user_id' in df.columns:
        row = df[df['user_id'].astype(str) == user_id_str]
        if not row.empty:
            logger.info(f"Found row by user_id: {user_id_str}")

    if row.empty and full_name and 'Ф.И.О.' in df.columns:
        row = df[df['Ф.И.О.'] == full_name]
        if not row.empty:
            logger.info(f"Found row by full_name: {full_name}")

    if row.empty:
        logger.info(f"No matching row found for user_id={user_id_str} or full_name={full_name}")
        return {'acknowledged': False, 'viewed': False, 'acknowledged_at': None, 'viewed_at': None}

    acknowledged_at = normalize_excel_timestamp(row.iloc[0].get('Ознакомлен', ''))
    viewed_at = normalize_excel_timestamp(row.iloc[0].get('Просмотрено', ''))
    acknowledged = bool(acknowledged_at)
    viewed = bool(viewed_at)
    logger.info(f"Status loaded: acknowledged={acknowledged}, viewed={viewed}, acknowledged_at={acknowledged_at}, viewed_at={viewed_at}")
    return {
        'acknowledged': acknowledged,
        'viewed': viewed,
        'acknowledged_at': acknowledged_at,
        'viewed_at': viewed_at
    }

def update_familiarization_status(doc_filename, user_id, full_name, acknowledged=None, viewed=None):
    user_id_str = str(user_id) if user_id is not None else ''
    if not full_name and not user_id_str:
        logger.warning(f"update_familiarization_status: no user_id or full_name provided for {doc_filename}")
        return False

    excel_path = get_familiarization_excel_path(doc_filename)
    logger.info(f"update_familiarization_status: doc={doc_filename}, user_id={user_id_str}, full_name={full_name}, excel_path={excel_path}, acknowledged={acknowledged}, viewed={viewed}")
    
    if os.path.exists(excel_path):
        try:
            df = pd.read_excel(excel_path, dtype=str, engine='openpyxl')
            logger.info(f"Loaded existing Excel file with {len(df)} rows")
        except Exception as e:
            logger.warning(f"Failed to read existing Excel file: {e}, creating new")
            df = pd.DataFrame(columns=['user_id', 'Ф.И.О.', 'Ознакомлен', 'Просмотрено'])
    else:
        logger.info(f"Excel file does not exist, creating new: {excel_path}")
        df = pd.DataFrame(columns=['user_id', 'Ф.И.О.', 'Ознакомлен', 'Просмотрено'])

    for column in ['user_id', 'Ф.И.О.', 'Ознакомлен', 'Просмотрено']:
        if column not in df.columns:
            df[column] = ''

    idx = None
    if user_id_str:
        matches = df.index[df['user_id'].astype(str) == user_id_str].tolist()
        if matches:
            idx = matches[0]
            logger.info(f"Found existing row by user_id at index {idx}")

    if idx is None and full_name:
        matches = df.index[df['Ф.И.О.'] == full_name].tolist()
        if matches:
            idx = matches[0]
            logger.info(f"Found existing row by full_name at index {idx}")

    if idx is None:
        idx = len(df)
        df.loc[idx, 'user_id'] = user_id_str
        df.loc[idx, 'Ф.И.О.'] = full_name
        df.loc[idx, 'Ознакомлен'] = ''
        df.loc[idx, 'Просмотрено'] = ''
        logger.info(f"Creating new row at index {idx}")
    else:
        if user_id_str and not str(df.loc[idx, 'user_id'] or '').strip():
            df.loc[idx, 'user_id'] = user_id_str
        if full_name and not str(df.loc[idx, 'Ф.И.О.'] or '').strip():
            df.loc[idx, 'Ф.И.О.'] = full_name

    timestamp = datetime.now().isoformat(timespec='seconds')

    if acknowledged is not None:
        df.loc[idx, 'Ознакомлен'] = timestamp if acknowledged else ''
        logger.info(f"Set Ознакомлен to {timestamp if acknowledged else 'empty'}")
    if viewed is not None:
        # Нормализуем текущее значение - обрабатываем NaN и пустые строки
        try:
            current_val = df.loc[idx, 'Просмотрено']
            if pd.isna(current_val):
                current_viewed = ''
            else:
                current_viewed = str(current_val).strip()
        except Exception:
            current_viewed = ''
        
        if viewed and not current_viewed:
            df.loc[idx, 'Просмотрено'] = timestamp
            logger.info(f"Set Просмотрено to {timestamp}")
        elif not viewed:
            df.loc[idx, 'Просмотрено'] = ''
            logger.info(f"Cleared Просмотрено")

    try:
        os.makedirs(os.path.dirname(excel_path), exist_ok=True)
        df.to_excel(excel_path, index=False, engine='openpyxl')
        logger.info(f"Successfully saved Excel file to {excel_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving familiarization log to {excel_path}: {e}")
        return False

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_authenticated'):
            return redirect(url_for('admin_login', next=request.path))
        return f(*args, **kwargs)
    return decorated_function

def generate_verification_code(length=6):
    """Генерация проверочного кода"""
    return ''.join(random.choices(string.digits, k=length))

def send_telegram_code(card_code):
    """Отправить код подтверждения через Telegram"""
    conn = None
    try:
        card_code = normalize_card_code(card_code)
        if not card_code:
            return {
                'success': False,
                'error': 'Неверный формат кода карты'
            }
        variants = card_code_variants(card_code)

        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get database connection")
            return {
                'success': False,
                'error': 'Ошибка подключения к базе данных'
            }
        
        logger.info("Starting send_telegram_code for card: %s", card_code)
        
        with conn.cursor() as cursor:
            logger.debug("Querying Telegram binding for code variants: %s", variants)
            if len(variants) > 1:
                cursor.execute("""
                    SELECT tb.telegram_id, tb.card_code
                    FROM telegram_bindings tb
                    WHERE (tb.card_code = %s OR tb.card_code = %s) AND tb.telegram_id IS NOT NULL
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT tb.telegram_id, tb.card_code
                    FROM telegram_bindings tb
                    WHERE tb.card_code = %s AND tb.telegram_id IS NOT NULL
                    LIMIT 1
                """, (variants[0],))
            binding = cursor.fetchone()

            if binding and binding['card_code'] != card_code:
                cursor.execute("""
                    UPDATE telegram_bindings
                    SET card_code = %s
                    WHERE card_code = %s
                """, (card_code, binding['card_code']))
                conn.commit()
        
        if not binding:
            logger.error("No Telegram binding found for card: %s", card_code)
            return {
                'success': False,
                'error': 'Карта не привязана к Telegram аккаунту'
            }

        telegram_id = binding['telegram_id']
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
        card_code = normalize_card_code(card_code)
        if not card_code:
            return {
                'success': False,
                'error': 'Неверный формат кода карты'
            }
        variants = card_code_variants(card_code)

        conn = get_db_connection()
        if not conn:
            return {
                'success': False,
                'error': 'Ошибка подключения к базе данных'
            }
        
        with conn.cursor() as cursor:
            # Ищем привязку VK для карты
            if len(variants) > 1:
                cursor.execute("""
                    SELECT vk_id
                    FROM telegram_bindings
                    WHERE (card_code = %s OR card_code = %s) AND vk_id IS NOT NULL
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT vk_id
                    FROM telegram_bindings
                    WHERE card_code = %s AND vk_id IS NOT NULL
                    LIMIT 1
                """, (variants[0],))
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
    global vk_session, vk, longpoll, vk_bot_thread, vk_bot_error
    
    try:
        vk_bot_error = None
        if not app.config.get('VK_BOT_ENABLED', True):
            logger.info("VK bot disabled by configuration")
            return
        if vk_bot_thread and vk_bot_thread.is_alive():
            logger.info("VK bot already running")
            return

        vk_bot_stop_event.clear()
        vk_session = vk_api.VkApi(token=app.config['VK_BOT_TOKEN'])
        vk = vk_session.get_api()
        longpoll = VkBotLongPoll(vk_session, app.config['VK_GROUP_ID'])
        
        logger.info("VK Bot started")
        
        def vk_bot_loop():
            while not vk_bot_stop_event.is_set():
                try:
                    for event in longpoll.listen():
                        if vk_bot_stop_event.is_set():
                            break
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
        vk_bot_thread = threading.Thread(target=vk_bot_loop, daemon=True)
        vk_bot_thread.start()
        
    except Exception as e:
        vk_bot_error = str(e)
        logger.error("Error starting VK bot: %s", vk_bot_error)

def stop_vk_bot():
    global vk_bot_thread
    if not vk_bot_thread or not vk_bot_thread.is_alive():
        return False, "VK bot not running"
    vk_bot_stop_event.set()
    try:
        if longpoll and hasattr(longpoll, 'session'):
            longpoll.session.close()
    except Exception:
        pass
    vk_bot_thread.join(timeout=5)
    return True, "VK bot stopped"

def telegram_bot_running():
    return telegram_bot_thread is not None and telegram_bot_thread.is_alive()

async def tg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    photo_path = os.path.join(BASE_DIR, 'static', 'CARDS.jpg')
    if os.path.exists(photo_path):
        with open(photo_path, 'rb') as photo:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=photo,
                caption=(
                    "👋 Привет! Для привязки аккаунта используйте команду:\n"
                    "/link <ваш_код_карты>\n\n"
                    "Пример: /link 000,00000"
                )
            )
    else:
        await update.message.reply_text(
            "👋 Привет! Для привязки аккаунта используйте команду:\n"
            "/link <ваш_код_карты>\n\n"
            "Пример: /link 000,00000"
        )

async def tg_link_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Привязать Telegram аккаунт к карте"""
    user_id = update.effective_user.id
    args = context.args
    conn = None

    if not args:
        await update.message.reply_text("❗ Укажите код карты после команды")
        return

    card_code_raw = ' '.join(args).strip()
    card_code = normalize_card_code(card_code_raw)
    if not card_code:
        await update.message.reply_text("❗ Неверный формат кода карты")
        return
    variants = card_code_variants(card_code)

    try:
        conn = get_db_connection()
        if not conn:
            await update.message.reply_text("⚠️ Ошибка подключения к базе данных")
            return

        logger.info("Linking attempt: %s -> %s", user_id, card_code)

        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT card_code
                FROM telegram_bindings
                WHERE telegram_id = %s
            """, (user_id,))
            existing_binding = cursor.fetchone()

            if existing_binding:
                if normalize_card_code(existing_binding[0]) == card_code:
                    await update.message.reply_text("ℹ️ Эта карта уже привязана к вашему аккаунту")
                else:
                    await update.message.reply_text(
                        f"❌ Ваш аккаунт уже привязан к карте: {existing_binding[0]}\n"
                        "Для смены обратитесь к администратору"
                    )
                return

            if len(variants) > 1:
                cursor.execute("""
                    SELECT telegram_id
                    FROM telegram_bindings
                    WHERE (card_code = %s OR card_code = %s) AND telegram_id IS NOT NULL
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT telegram_id
                    FROM telegram_bindings
                    WHERE card_code = %s AND telegram_id IS NOT NULL
                    LIMIT 1
                """, (variants[0],))
            card_owner = cursor.fetchone()

            if card_owner:
                await update.message.reply_text(
                    "❌ Эта карта уже привязана к другому аккаунту\n"
                    "Для перепривязки обратитесь к администратору"
                )
                return

            if len(variants) > 1:
                cursor.execute("""
                    SELECT id, user_id
                    FROM pass
                    WHERE code = %s OR code = %s
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT id, user_id
                    FROM pass
                    WHERE code = %s
                    LIMIT 1
                """, (variants[0],))
            pass_row = cursor.fetchone()

            if not pass_row:
                await update.message.reply_text("❌ Карта не найдена в системе")
                return

            pass_id = pass_row['id']
            pass_user_id = pass_row['user_id']

            if len(variants) > 1:
                cursor.execute("""
                    SELECT id, telegram_id, user_id
                    FROM telegram_bindings
                    WHERE card_code = %s OR card_code = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT id, telegram_id, user_id
                    FROM telegram_bindings
                    WHERE card_code = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (variants[0],))

            existing_card_binding = cursor.fetchone()

            if existing_card_binding:
                existing_tg_id = existing_card_binding.get('telegram_id')
                if existing_tg_id and int(existing_tg_id) != int(user_id):
                    await update.message.reply_text(
                        "❌ Эта карта уже привязана к другому аккаунту\n"
                        "Для перепривязки обратитесь к администратору"
                    )
                    return

                cursor.execute("""
                    UPDATE telegram_bindings
                    SET pass_id = %s,
                        card_code = %s,
                        telegram_id = %s,
                        user_id = COALESCE(user_id, %s)
                    WHERE id = %s
                """, (pass_id, card_code, user_id, pass_user_id, existing_card_binding['id']))
            else:
                cursor.execute("""
                    INSERT INTO telegram_bindings (pass_id, card_code, telegram_id, user_id)
                    VALUES (%s, %s, %s, %s)
                """, (pass_id, card_code, user_id, pass_user_id))

            if cursor.rowcount > 0:
                logger.info("Successfully linked: %s -> %s", user_id, card_code)
                if pass_user_id:
                    log_activity_event('registration', user_id=pass_user_id, card_code=card_code, channel='telegram')
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

def _telegram_bot_worker():
    global telegram_bot_loop, telegram_bot_app, telegram_bot_error
    telegram_bot_error = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        app_instance = Application.builder().token(app.config.get('TELEGRAM_BOT_TOKEN', '')).build()
        app_instance.add_handler(CommandHandler("start", tg_start))
        app_instance.add_handler(CommandHandler("link", tg_link_account))
        polling_conflict_handled = False

        async def tg_error_handler(update, context):
            error = getattr(context, 'error', None)
            if TELEGRAM_BOT_AVAILABLE and isinstance(error, Conflict):
                global telegram_bot_error
                telegram_bot_error = "Conflict: another getUpdates is running"
                logger.error("Telegram bot conflict: another getUpdates instance is running")
                await context.application.stop()
                await context.application.shutdown()
                loop.stop()

        app_instance.add_error_handler(tg_error_handler)

        telegram_bot_loop = loop
        telegram_bot_app = app_instance

        async def runner():
            await app_instance.initialize()
            try:
                await app_instance.bot.delete_webhook(drop_pending_updates=True)
            except Exception:
                pass
            try:
                await app_instance.bot.get_updates(offset=-1, timeout=1)
            except Conflict:
                telegram_bot_error = "Conflict: another getUpdates is running"
                logger.error("Telegram bot conflict detected during preflight")
                await app_instance.shutdown()
                loop.stop()
                return
            await app_instance.start()
            if app_instance.updater:
                async def handle_polling_error(error):
                    nonlocal polling_conflict_handled
                    global telegram_bot_error
                    if polling_conflict_handled:
                        return
                    if isinstance(error, Conflict):
                        polling_conflict_handled = True
                        telegram_bot_error = "Conflict: another getUpdates is running"
                        logger.error("Telegram bot conflict detected during polling")

                        async def shutdown_on_conflict():
                            if app_instance.updater:
                                await app_instance.updater.stop()
                            await app_instance.stop()
                            await app_instance.shutdown()
                            loop.stop()

                        asyncio.create_task(shutdown_on_conflict())

                try:
                    await app_instance.updater.start_polling(error_callback=handle_polling_error)
                except TypeError:
                    await app_instance.updater.start_polling()

        loop.run_until_complete(runner())
        loop.run_forever()

        async def shutdown():
            if app_instance.updater:
                await app_instance.updater.stop()
            await app_instance.stop()
            await app_instance.shutdown()

        loop.run_until_complete(shutdown())
    except Exception as e:
        telegram_bot_error = str(e)
        logger.error("Error in Telegram bot thread: %s", telegram_bot_error)
    finally:
        try:
            if telegram_bot_loop and telegram_bot_loop.is_running():
                telegram_bot_loop.stop()
        except Exception:
            pass
        telegram_bot_loop = None
        telegram_bot_app = None

def start_telegram_bot():
    global telegram_bot_thread
    if not TELEGRAM_BOT_AVAILABLE:
        return False, "Telegram library not available"
    if not app.config.get('TELEGRAM_BOT_ENABLED', True):
        return False, "Telegram bot disabled by configuration"
    if not app.config.get('TELEGRAM_BOT_TOKEN'):
        return False, "Telegram bot token missing"
    if telegram_bot_running():
        return True, "Telegram bot already running"

    telegram_bot_stop_event.clear()
    telegram_bot_thread = threading.Thread(target=_telegram_bot_worker, daemon=True)
    telegram_bot_thread.start()
    return True, "Telegram bot started"

def stop_telegram_bot():
    global telegram_bot_thread
    if not telegram_bot_running():
        return False, "Telegram bot not running"

    telegram_bot_stop_event.set()
    loop = telegram_bot_loop
    app_instance = telegram_bot_app

    async def shutdown():
        if app_instance and app_instance.updater:
            await app_instance.updater.stop()
        if app_instance:
            await app_instance.stop()
            await app_instance.shutdown()

    try:
        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(shutdown(), loop)
            future.result(timeout=10)
            loop.call_soon_threadsafe(loop.stop)
    except Exception as e:
        logger.error("Error stopping Telegram bot: %s", str(e))

    telegram_bot_thread.join(timeout=5)
    return True, "Telegram bot stopped"

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
    card_code = normalize_card_code(card_code)
    variants = card_code_variants(card_code)
    
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
                if normalize_card_code(existing_binding[0]) == card_code:
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
            if len(variants) > 1:
                cursor.execute("""
                    SELECT vk_id 
                    FROM telegram_bindings 
                    WHERE (card_code = %s OR card_code = %s) AND vk_id IS NOT NULL
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT vk_id 
                    FROM telegram_bindings 
                    WHERE card_code = %s AND vk_id IS NOT NULL
                    LIMIT 1
                """, (variants[0],))
            card_owner = cursor.fetchone()
            
            if card_owner:
                vk.messages.send(
                    user_id=user_id,
                    message="❌ Эта карта уже привязана к другому аккаунту\nДля перепривязки обратитесь к администратору",
                    random_id=random.randint(1, 1000000)
                )
                return

            # 3. Получить ID карты из таблицы pass
            if len(variants) > 1:
                cursor.execute("""
                    SELECT id, user_id
                    FROM pass 
                    WHERE code = %s OR code = %s
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT id, user_id
                    FROM pass 
                    WHERE code = %s
                    LIMIT 1
                """, (variants[0],))
            pass_row = cursor.fetchone()
            
            if not pass_row:
                vk.messages.send(
                    user_id=user_id,
                    message="❌ Карта не найдена в системе",
                    random_id=random.randint(1, 1000000)
                )
                return
                
            pass_id = pass_row['id']
            pass_user_id = pass_row['user_id']

            # 4. Создать новую привязку
            if len(variants) > 1:
                cursor.execute("""
                    SELECT id, vk_id, user_id
                    FROM telegram_bindings
                    WHERE card_code = %s OR card_code = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (variants[0], variants[1]))
            else:
                cursor.execute("""
                    SELECT id, vk_id, user_id
                    FROM telegram_bindings
                    WHERE card_code = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (variants[0],))

            existing_card_binding = cursor.fetchone()

            if existing_card_binding:
                existing_vk_id = existing_card_binding.get('vk_id')
                if existing_vk_id and int(existing_vk_id) != int(user_id):
                    vk.messages.send(
                        user_id=user_id,
                        message="❌ Эта карта уже привязана к другому аккаунту\nДля перепривязки обратитесь к администратору",
                        random_id=random.randint(1, 1000000)
                    )
                    return

                cursor.execute("""
                    UPDATE telegram_bindings
                    SET pass_id = %s,
                        card_code = %s,
                        vk_id = %s,
                        user_id = COALESCE(user_id, %s)
                    WHERE id = %s
                """, (pass_id, card_code, user_id, pass_user_id, existing_card_binding['id']))
            else:
                cursor.execute("""
                    INSERT INTO telegram_bindings (pass_id, card_code, vk_id, user_id)
                    VALUES (%s, %s, %s, %s)
                """, (pass_id, card_code, user_id, pass_user_id))
            
            if cursor.rowcount > 0:
                logger.info("Successfully linked VK: %s -> %s", user_id, card_code)
                if pass_user_id:
                    log_activity_event('registration', user_id=pass_user_id, card_code=card_code, channel='vk')
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
    if request.method == 'OPTIONS':
        return

    restore_session_from_sid()

    # Разрешить публичные эндпоинты
    public_endpoints = ['login', 'verify_code', 'resend_code', 'static', 'api_profile_content', 
                    'api_sync_session', 'auth_tab', 'login_by_card']
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

@app.before_request
def start_request_timer():
    g.request_start = time.perf_counter()

def parse_activity_log_stats(log_path, days=30, max_lines=200000, terminal_ip=None):
    today = datetime.now().date()
    start_date = today - timedelta(days=days - 1)
    date_keys = [(start_date + timedelta(days=i)).isoformat() for i in range(days)]

    sessions = defaultdict(int)
    auths = defaultdict(int)
    registrations = defaultdict(int)
    errors = defaultdict(int)
    active_users = defaultdict(set)
    response_times = defaultdict(list)
    active_user_details = {}
    terminals = set()
    event_user_counts = {
        'visits': defaultdict(int),
        'auths': defaultdict(int),
        'registrations': defaultdict(int)
    }
    event_lists = {
        'visits': [],
        'auths': [],
        'registrations': [],
        'active_users': []
    }
    seen_active_sessions = set()

    if not os.path.exists(log_path):
        return {
            'dates': date_keys,
            'visits': [0] * days,
            'auths': [0] * days,
            'registrations': [0] * days,
            'errors': [0] * days,
            'active_users': [0] * days,
            'response_time_ms': [0] * days,
            'response_time_available': False,
            'meta': {
                'start_date': start_date.isoformat(),
                'end_date': today.isoformat(),
                'lines_scanned': 0,
                'log_path': log_path,
                'terminals': [],
                'terminal_filter': terminal_ip or 'all'
            }
        }

    timestamp_re = re.compile(r'^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}),\d{3}\s+-\s+METRIC\s+(.*)$')

    lines_scanned = 0
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as file_handle:
        for line in deque(file_handle, maxlen=max_lines):
            lines_scanned += 1
            match = timestamp_re.match(line)
            if not match:
                continue

            date_str, time_str, payload = match.groups()
            if date_str < start_date.isoformat() or date_str > today.isoformat():
                continue

            fields = {}
            for item in payload.split():
                if '=' not in item:
                    continue
                key, value = item.split('=', 1)
                fields[key] = value

            terminal = fields.get('ip')
            if terminal:
                terminals.add(terminal)
            if terminal_ip and terminal_ip != 'all':
                if terminal != terminal_ip:
                    continue

            event = fields.get('event')
            user_id = fields.get('user_id')
            if user_id:
                detail = active_user_details.setdefault(user_id, {'count': 0, 'last_seen': None})
                detail['count'] += 1
                detail['last_seen'] = f"{date_str} {time_str}"

            if event == 'session_start':
                sessions[date_str] += 1
                if user_id:
                    active_users[date_str].add(user_id)
                    event_user_counts['visits'][user_id] += 1
                    event_lists['visits'].append({
                        'user_id': user_id,
                        'timestamp': f"{date_str} {time_str}"
                    })
                    session_id = fields.get('sid')
                    if session_id and session_id not in seen_active_sessions:
                        seen_active_sessions.add(session_id)
                        event_lists['active_users'].append({
                            'user_id': user_id,
                            'timestamp': f"{date_str} {time_str}",
                            'sid': session_id
                        })
            elif event == 'auth':
                auths[date_str] += 1
                if user_id:
                    active_users[date_str].add(user_id)
                    event_user_counts['auths'][user_id] += 1
                    event_lists['auths'].append({
                        'user_id': user_id,
                        'timestamp': f"{date_str} {time_str}"
                    })
            elif event == 'registration':
                registrations[date_str] += 1
                if user_id:
                    active_users[date_str].add(user_id)
                    event_user_counts['registrations'][user_id] += 1
                    event_lists['registrations'].append({
                        'user_id': user_id,
                        'timestamp': f"{date_str} {time_str}"
                    })
            elif event == 'response':
                if user_id:
                    active_users[date_str].add(user_id)

                status_raw = fields.get('status')
                if status_raw and status_raw.isdigit():
                    status_code = int(status_raw)
                    if status_code >= 400:
                        errors[date_str] += 1

                ms_raw = fields.get('ms')
                if ms_raw:
                    try:
                        response_times[date_str].append(float(ms_raw))
                    except ValueError:
                        pass

    response_time_ms = []
    for date_key in date_keys:
        values = response_times[date_key]
        if values:
            response_time_ms.append(round(sum(values) / len(values), 2))
        else:
            response_time_ms.append(0)

    response_time_available = any(response_times[date_key] for date_key in date_keys)

    for items in event_lists.values():
        items.sort(key=lambda item: item.get('timestamp', ''), reverse=True)

    sorted_users = sorted(
        (
            {
                'user_id': user_id,
                'count': details['count'],
                'last_seen': details['last_seen']
            }
            for user_id, details in active_user_details.items()
        ),
        key=lambda item: (item['count'], item['last_seen'] or ''),
        reverse=True
    )

    user_lists = {
        'visits': [],
        'auths': [],
        'registrations': [],
        'active_users': []
    }

    for event_key, counts in event_user_counts.items():
        rows = []
        for user_id, count in counts.items():
            last_seen = active_user_details.get(user_id, {}).get('last_seen')
            rows.append({'user_id': user_id, 'count': count, 'last_seen': last_seen})
        rows.sort(key=lambda item: (item['count'], item['last_seen'] or ''), reverse=True)
        user_lists[event_key] = rows[:50]

    user_lists['active_users'] = [
        {
            'user_id': item['user_id'],
            'count': item['count'],
            'last_seen': item['last_seen']
        }
        for item in sorted_users[:50]
    ]

    return {
        'dates': date_keys,
        'visits': [sessions[d] for d in date_keys],
        'auths': [auths[d] for d in date_keys],
        'registrations': [registrations[d] for d in date_keys],
        'errors': [errors[d] for d in date_keys],
        'active_users': [len(active_users[d]) for d in date_keys],
        'response_time_ms': response_time_ms,
        'response_time_available': response_time_available,
        'active_user_details': sorted_users[:50],
        'user_lists': user_lists,
        'event_lists': event_lists,
        'meta': {
            'start_date': start_date.isoformat(),
            'end_date': today.isoformat(),
            'lines_scanned': lines_scanned,
            'log_path': log_path,
            'terminals': sorted(terminals),
            'terminal_filter': terminal_ip or 'all'
        }
    }

def fetch_user_names(user_ids):
    if not user_ids:
        return {}

    conn = get_db_connection()
    if not conn:
        return {}

    try:
        placeholders = ','.join(['%s'] * len(user_ids))
        query = f"SELECT id, first_name, last_name, middle_name FROM users WHERE id IN ({placeholders})"
        with conn.cursor() as cursor:
            cursor.execute(query, tuple(user_ids))
            rows = cursor.fetchall()
        names = {}
        for row in rows:
            first_name = (row.get('first_name') or '').strip()
            last_name = (row.get('last_name') or '').strip()
            middle_name = (row.get('middle_name') or '').strip()
            full_name = ' '.join(part for part in [last_name, first_name, middle_name] if part)
            names[str(row['id'])] = full_name or f"user {row['id']}"
        return names
    except Exception as e:
        logger.error("Error fetching user names: %s", e)
        return {}
    finally:
        release_db_connection(conn)

@app.after_request
def log_request_metrics(response):
    if request.path.startswith('/static'):
        return response

    start = getattr(g, 'request_start', None)
    duration_ms = 0.0
    if start is not None:
        duration_ms = (time.perf_counter() - start) * 1000

    log_activity_event(
        'response',
        path=request.path,
        method=request.method,
        status=response.status_code,
        ms=f"{duration_ms:.2f}",
        user_id=session.get('user_id'),
        sid=session.get('sid'),
        ip=get_client_ip()
    )

    return response

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
                drop_cached_auth_session(session.get('sid'))
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

@app.route('/admin/stats')
@admin_login_required
def admin_stats():
    return render_template('admin_stats.html')

@app.route('/admin/settings')
@admin_login_required
def admin_settings():
    return render_template('admin_settings.html')

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'GET':
        next_url = request.args.get('next') or url_for('admin_settings')
        return render_template('admin_login.html', next_url=next_url)

    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()

    if not username or not password:
        return render_template('admin_login.html', next_url=request.form.get('next') or url_for('admin_settings'), error='Введите логин и пароль')

    if username == str(app.config.get('ADMIN_LOGIN')) and password == str(app.config.get('ADMIN_PASSWORD')):
        session['admin_authenticated'] = True
        session['admin_login'] = username
        return redirect(request.form.get('next') or url_for('admin_settings'))

    return render_template('admin_login.html', next_url=request.form.get('next') or url_for('admin_settings'), error='Неверный логин или пароль')

@app.route('/admin/logout')
def admin_logout():
    session.pop('admin_authenticated', None)
    session.pop('admin_login', None)
    return redirect(url_for('admin_login'))

@app.route('/api/admin_settings', methods=['GET', 'POST'])
def api_admin_settings():
    if not session.get('admin_authenticated'):
        return jsonify({'error': 'Unauthorized'}), 401

    if request.method == 'GET':
        return jsonify({
            'settings': get_admin_config(),
            'keys': ADMIN_CONFIG_KEYS,
            'meta': {
                'override_path': CONFIG_OVERRIDE_PATH
            }
        })

    payload = request.get_json(silent=True) or {}
    settings = payload.get('settings', payload)
    if not isinstance(settings, dict):
        return jsonify({'error': 'Invalid payload'}), 400

    updated = {}
    prev_values = {key: app.config.get(key) for key in ADMIN_CONFIG_KEYS}
    for key in ADMIN_CONFIG_KEYS:
        if key not in settings:
            continue
        raw_value = settings[key]
        if key in ADMIN_BOOL_KEYS:
            parsed_value = parse_admin_bool(raw_value)
        elif key in ADMIN_INT_KEYS:
            try:
                parsed_value = int(raw_value)
            except (TypeError, ValueError):
                parsed_value = raw_value
        else:
            parsed_value = raw_value

        app.config[key] = deserialize_config_value(key, parsed_value)
        updated[key] = serialize_config_value(key, app.config.get(key))

    if any(key in {'ONE_C_USERNAME', 'ONE_C_PASSWORD'} for key in updated):
        update_one_c_auth_header()

    postgres_keys = {'POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD'}
    if any(key in updated and updated.get(key) != prev_values.get(key) for key in postgres_keys):
        refresh_postgres_pool()

    if 'TELEGRAM_BOT_ENABLED' in updated:
        if app.config.get('TELEGRAM_BOT_ENABLED'):
            start_telegram_bot()
        else:
            stop_telegram_bot()

    if 'VK_BOT_ENABLED' in updated:
        if app.config.get('VK_BOT_ENABLED'):
            start_vk_bot()
        else:
            stop_vk_bot()

    save_config_overrides(updated)
    return jsonify({'success': True, 'settings': get_admin_config()})

@app.route('/api/admin_reload', methods=['POST'])
def api_admin_reload():
    if not session.get('admin_authenticated'):
        return jsonify({'error': 'Unauthorized'}), 401

    schedule_server_restart()
    return jsonify({'success': True, 'message': 'Server restart scheduled'})

@app.route('/api/admin_status', methods=['GET'])
def api_admin_status():
    if not session.get('admin_authenticated'):
        return jsonify({'error': 'Unauthorized'}), 401

    status = {}

    db_status = {'ok': False}
    conn = get_db_connection()
    if conn:
        db_status['ok'] = True
        release_db_connection(conn)
    status['database'] = db_status

    activity_log = os.path.join(os.getcwd(), 'activity.log')
    status['activity_log'] = {
        'exists': os.path.exists(activity_log),
        'writable': os.access(activity_log, os.W_OK) or os.access(os.getcwd(), os.W_OK)
    }

    session_dir = app.config.get('SESSION_FILE_DIR', './flask_sessions')
    status['session_dir'] = {
        'path': session_dir,
        'exists': os.path.exists(session_dir),
        'writable': os.access(session_dir, os.W_OK) if os.path.exists(session_dir) else False
    }

    tokens = {
        'telegram': bool(app.config.get('TELEGRAM_BOT_TOKEN')),
        'vk': bool(app.config.get('VK_BOT_TOKEN')),
        'vk_group': bool(app.config.get('VK_GROUP_ID'))
    }
    status['tokens'] = tokens

    one_c_endpoints = {
        'payslip': app.config.get('ONE_C_PAYSLIP_URL'),
        'vacations': app.config.get('ONE_C_VACATIONS_URL'),
        'personal_data': app.config.get('ONE_C_PERSONAL_DATA_URL'),
        'timesheet': app.config.get('ONE_C_TIMESHEET_URL')
    }
    one_c_status = {}
    for key, url in one_c_endpoints.items():
        if not url:
            one_c_status[key] = {'ok': False, 'status': 'missing'}
            continue
        try:
            response = requests.get(url, headers={'Authorization': app.config.get('ONE_C_AUTH_HEADER')}, timeout=4, verify=False)
            one_c_status[key] = {'ok': response.status_code < 500, 'status': response.status_code}
        except Exception as e:
            one_c_status[key] = {'ok': False, 'status': str(e)}
    status['one_c'] = one_c_status

    if os.path.exists(CONFIG_OVERRIDE_PATH):
        status['config_overrides'] = {
            'path': CONFIG_OVERRIDE_PATH,
            'modified_at': datetime.fromtimestamp(os.path.getmtime(CONFIG_OVERRIDE_PATH)).isoformat()
        }
    else:
        status['config_overrides'] = {
            'path': CONFIG_OVERRIDE_PATH,
            'modified_at': None
        }

    return jsonify(status)

@app.route('/api/admin_bot_status', methods=['GET'])
def api_admin_bot_status():
    if not session.get('admin_authenticated'):
        return jsonify({'error': 'Unauthorized'}), 401

    return jsonify({
        'telegram': {
            'enabled': bool(app.config.get('TELEGRAM_BOT_ENABLED', True)),
            'available': TELEGRAM_BOT_AVAILABLE,
            'running': telegram_bot_running(),
            'error': telegram_bot_error
        },
        'vk': {
            'enabled': bool(app.config.get('VK_BOT_ENABLED', True)),
            'running': bool(vk_bot_thread and vk_bot_thread.is_alive()),
            'error': vk_bot_error
        }
    })

@app.route('/api/admin_bot_action', methods=['POST'])
def api_admin_bot_action():
    if not session.get('admin_authenticated'):
        return jsonify({'error': 'Unauthorized'}), 401

    payload = request.get_json(silent=True) or {}
    bot = payload.get('bot')
    action = payload.get('action')

    if bot not in {'telegram', 'vk'} or action not in {'start', 'stop', 'restart'}:
        return jsonify({'error': 'Invalid payload'}), 400

    if bot == 'telegram':
        if action == 'start':
            ok, message = start_telegram_bot()
        elif action == 'stop':
            ok, message = stop_telegram_bot()
        else:
            stop_telegram_bot()
            ok, message = start_telegram_bot()
        return jsonify({'success': ok, 'message': message})

    if action == 'start':
        start_vk_bot()
        return jsonify({'success': True, 'message': 'VK bot started'})
    if action == 'stop':
        ok, message = stop_vk_bot()
        return jsonify({'success': ok, 'message': message})
    stop_vk_bot()
    start_vk_bot()
    return jsonify({'success': True, 'message': 'VK bot restarted'})

@app.route('/api/admin_stats', methods=['GET'])
def api_admin_stats():
    if not session.get('admin_authenticated'):
        return jsonify({'error': 'Unauthorized'}), 401
    try:
        days = int(request.args.get('days', 30))
        max_lines = int(request.args.get('max_lines', 200000))
    except ValueError:
        return jsonify({'error': 'Invalid parameters'}), 400

    terminal_ip = request.args.get('terminal')

    days = max(1, min(days, 365))
    max_lines = max(1000, min(max_lines, 500000))

    log_path = os.path.join(os.getcwd(), 'activity.log')
    stats = parse_activity_log_stats(log_path, days=days, max_lines=max_lines, terminal_ip=terminal_ip)

    event_lists = stats.get('event_lists', {})
    user_ids = set()
    for items in event_lists.values():
        for item in items:
            user_id = item.get('user_id')
            if user_id:
                user_ids.add(user_id)

    name_map = fetch_user_names(sorted(user_ids))
    for items in event_lists.values():
        for item in items:
            user_id = item.get('user_id')
            if user_id:
                item['name'] = name_map.get(str(user_id), f"user {user_id}")

    return jsonify(stats)

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
    card_code = normalize_card_code(request.form.get('card_number', '').strip())
    channel = request.form.get('channel', 'telegram')

    if not card_code:
        return jsonify({
            'success': False,
            'error': 'Неверный формат кода карты'
        }), 400
    
    # Проверяем существование карты
    conn = get_db_connection()
    if not conn:
        return jsonify({
            'success': False, 
            'error': 'Ошибка подключения к базе данных'
        })
        
    try:
        formatted_code = format_card_code(card_code)
        
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
        card_code = normalize_card_code(request.form.get('card_code', '').strip())
        user_code = request.form.get('code', '').strip()
        channel = request.form.get('channel', 'telegram').strip()
        
        logger.debug("Received card_code: '%s', code: '%s', channel: '%s'", 
                    card_code, user_code, channel)
                    
        # Проверяем обязательные поля
        if not card_code:
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
                client_ip = get_client_ip()
                log_activity_event('auth', user_id=user['id'], sid=session_id, card_code=card_code, ip=client_ip)
                log_activity_event('session_start', user_id=user['id'], sid=session_id, ip=client_ip)
                cache_auth_session(session_id, user['id'], card_code, channel)
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
        card_code = normalize_card_code(request.form.get('card_code') or request.form.get('card_number'))
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

    drop_cached_auth_session(session.get('sid'))
    
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
                    'card_code': session.get('card_code'),
                    'sid': session.get('sid')
                })
    
    drop_cached_auth_session(session.get('sid'))
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
            
            current_year = datetime.now().year
            url = f"{app.config['ONE_C_VACATIONS_URL']}?id_1c={one_c_id}&Date={current_year}0101&view=table"
            
            headers = {'Authorization': app.config['ONE_C_AUTH_HEADER']}
            response = requests.get(url, headers=headers, timeout=30)

            logger.info(
                "1C vacations info response: status=%s, content-type=%s, length=%s",
                response.status_code,
                response.headers.get('content-type', 'unknown'),
                len(response.content) if response.content is not None else 0
            )
            
            if response.status_code == 200:
                content = response.text
                result = {
                    'is_base64': False,
                    'is_json': False,
                    'structured_data': None
                }

                logger.debug("1C vacations raw preview: %s", (content or '')[:500])
                
                try:
                    decoded_content = base64.b64decode(content).decode('utf-8')
                    result['content'] = decoded_content
                    result['is_base64'] = True
                except:
                    result['content'] = content

                logger.debug(
                    "1C vacations decoded preview: %s",
                    (result.get('content') or '')[:500]
                )
                
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

                logger.info(
                    "Vacations info parse summary: is_base64=%s, is_json=%s, structured_data=%s",
                    result.get('is_base64'),
                    result.get('is_json'),
                    result.get('structured_data')
                )
                
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

            def set_if_empty(field, value):
                if vacation_data.get(field) is None and value not in (None, ''):
                    vacation_data[field] = str(value)

            def walk_json(obj):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        key_lower = str(key).lower()

                        if isinstance(value, (str, int, float)):
                            if key_lower in (
                                'days_left', 'daysleft', 'days_until_vacation',
                                'daysuntilvacation', 'days_to_vacation', 'daystovacation'
                            ):
                                set_if_empty('days_left', value)

                            if key_lower in (
                                'vacation_dates', 'vacation_date', 'vacationdate',
                                'next_vacation_date', 'nextvacationdate', 'vacation',
                                'vacation_start', 'vacationstart', 'start_date', 'startdate'
                            ):
                                set_if_empty('vacation_dates', value)

                            if key_lower in (
                                'vacation_days', 'vacationdays', 'vacation_duration',
                                'vacationduration', 'numberofdays', 'number_of_days',
                                'days_count', 'dayscount'
                            ):
                                set_if_empty('vacation_days', value)

                        if isinstance(value, (dict, list)):
                            walk_json(value)

                elif isinstance(obj, list):
                    for item in obj:
                        walk_json(item)

            walk_json(data)

            if vacation_data.get('vacation_dates'):
                try:
                    start_date = datetime.strptime(vacation_data['vacation_dates'], '%d.%m.%Y').date()
                    today = datetime.now().date()
                    if start_date > today:
                        vacation_data['days_left'] = str((start_date - today).days)
                except Exception:
                    pass
        
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

#________________________________________________________________________________________________
# API для документов ознакомления
@app.route('/api/familiarization_documents', methods=['GET'])
def api_familiarization_documents():
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    user_id = session.get('user_id')
    user_full_name = get_user_full_name(user_id) or 'Unknown user'
    documents = list_familiarization_documents()
    response_items = []

    for item in documents:
        status = load_familiarization_status(item['filename'], user_id, user_full_name)
        response_items.append({
            'id': item['id'],
            'name': item['name'],
            'date': item['date'],
            'ext': item['ext'],
            'acknowledged': status.get('acknowledged', False),
            'viewed': status.get('viewed', False),
            'acknowledged_at': status.get('acknowledged_at'),
            'viewed_at': status.get('viewed_at')
        })

    return jsonify({
        'documents': response_items,
        'docx_available': DOCX_AVAILABLE
    })

@app.route('/api/familiarization_documents/<path:doc_id>/file', methods=['GET'])
def api_familiarization_document_file(doc_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    document = resolve_familiarization_document(doc_id)
    if not document:
        return jsonify({'error': 'Document not found'}), 404

    filename = document['filename']
    disposition = build_content_disposition(filename)
    content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
    as_pdf = request.args.get('format') == 'pdf'
    ext = os.path.splitext(filename)[1].lower()

    if as_pdf and ext in {'.doc', '.docx'}:
        pdf_bytes, error = convert_document_to_pdf(document)
        if pdf_bytes is None:
            return jsonify({'error': error or 'Conversion unavailable'}), 501
        pdf_filename = f"{os.path.splitext(filename)[0]}.pdf"
        pdf_response = Response(pdf_bytes, status=200, mimetype='application/pdf')
        pdf_response.headers['Content-Disposition'] = build_content_disposition(pdf_filename)
        return pdf_response

    source = get_familiarization_source()
    if source == 'url' and document.get('url'):
        try:
            response = requests.get(document['url'], timeout=12)
            response_type = response.headers.get('content-type') or content_type
            file_response = Response(response.content, status=response.status_code, mimetype=response_type)
            file_response.headers['Content-Disposition'] = disposition
            return file_response
        except Exception as e:
            logger.error("Failed to fetch remote document: %s", e)
            return jsonify({'error': 'Failed to fetch document'}), 502

    docs_dir = get_familiarization_docs_dir()
    file_response = send_from_directory(
        docs_dir,
        filename,
        as_attachment=False,
        mimetype=content_type
    )
    file_response.headers['Content-Disposition'] = disposition
    return file_response

@app.route('/api/familiarization_documents/<path:doc_id>/text', methods=['GET'])
def api_familiarization_document_text(doc_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    document = resolve_familiarization_document(doc_id)
    if not document:
        return jsonify({'error': 'Document not found'}), 404

    ext = document.get('ext')
    try:
        content_bytes = get_document_bytes(document)
    except Exception as e:
        logger.error("Failed to read document bytes: %s", e)
        return jsonify({'error': 'Failed to read document'}), 500

    if ext == '.pdf':
        text = extract_text_from_pdf(content_bytes)
        return jsonify({'text': text})

    if ext == '.docx':
        if not DOCX_AVAILABLE:
            return jsonify({'error': 'DOCX support not available'}), 501
        try:
            doc = DocxDocument(BytesIO(content_bytes))
            paragraphs = [p.text for p in doc.paragraphs if p.text]
            return jsonify({'text': '\n'.join(paragraphs)})
        except Exception as e:
            logger.error("Error reading DOCX: %s", e)
            return jsonify({'error': 'Failed to read document'}), 500

    if ext == '.doc':
        if not TEXTRACT_AVAILABLE:
            return jsonify({'error': 'DOC support not available'}), 501
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.doc') as temp_file:
                temp_file.write(content_bytes)
                temp_path = temp_file.name
            try:
                extracted = textract.process(temp_path)
            finally:
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass
            try:
                text = extracted.decode('utf-8')
            except UnicodeDecodeError:
                text = extracted.decode('cp1251', errors='replace')
            return jsonify({'text': text})
        except Exception as e:
            logger.error("Error reading DOC: %s", e)
            return jsonify({'error': 'Failed to read document'}), 500

    return jsonify({'error': 'Unsupported format'}), 400

@app.route('/api/familiarization_documents/<path:doc_id>/viewed', methods=['POST'])
def api_familiarization_document_viewed(doc_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    document = resolve_familiarization_document(doc_id)
    if not document:
        return jsonify({'error': 'Document not found'}), 404

    user_id = session.get('user_id')
    user_full_name = get_user_full_name(user_id) or 'Unknown user'
    ok = update_familiarization_status(document['filename'], user_id, user_full_name, viewed=True)
    status = load_familiarization_status(document['filename'], user_id, user_full_name)
    return jsonify({'success': ok, 'viewed_at': status.get('viewed_at')})

@app.route('/api/familiarization_documents/<path:doc_id>/acknowledge', methods=['POST'])
def api_familiarization_document_acknowledge(doc_id):
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401

    document = resolve_familiarization_document(doc_id)
    if not document:
        return jsonify({'error': 'Document not found'}), 404

    payload = request.get_json(silent=True) or {}
    acknowledged = bool(payload.get('acknowledged'))

    user_id = session.get('user_id')
    user_full_name = get_user_full_name(user_id) or 'Unknown user'
    ok = update_familiarization_status(document['filename'], user_id, user_full_name, acknowledged=acknowledged)
    status = load_familiarization_status(document['filename'], user_id, user_full_name)
    return jsonify({'success': ok, 'acknowledged_at': status.get('acknowledged_at')})

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
                    log_activity_event(
                        'registration',
                        user_id=user_id,
                        card_code=card_code,
                        channel='web',
                        ip=get_client_ip()
                    )
            
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
    origin = request.headers.get('Origin')
    if origin:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Vary'] = 'Origin'
    else:
        response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, Origin, Accept'

    # Flask-Session сам выставляет корректный cookie идентификатора сессии.
    # Ручная перезапись cookie ломает восстановление серверной сессии.
    return response

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
    is_reloader_main = os.environ.get('WERKZEUG_RUN_MAIN') == 'true'
    run_debug = bool(app.config.get('RUN_DEBUG', False))
    # Run DB init and start bots only in the reloader main process when debug is enabled.
    if not run_debug or is_reloader_main:
        init_tables()
        if app.config.get('VK_BOT_TOKEN') and app.config.get('VK_GROUP_ID') and app.config.get('VK_BOT_ENABLED', True):
            start_vk_bot()
        if app.config.get('TELEGRAM_BOT_ENABLED', True):
            start_telegram_bot()
    app.run(
        host=app.config.get('RUN_HOST', '192.168.210.201'),
        port=app.config.get('RUN_PORT', 5050),
        debug=app.config.get('RUN_DEBUG', True)
    )