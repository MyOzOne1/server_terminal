from flask import Flask, render_template, session, redirect, url_for, request, jsonify, make_response
import sqlite3
import secrets
import time
import requests
import logging
import sys
sys.path.append(r'C:\Users\Azyabin\AppData\Local\Programs\Python\Python310\Lib\site-packages')
from flask_cors import CORS

app = Flask(__name__)
CORS(app, supports_credentials=True, resources={r"/": {"origins": "*"}})
app.config.update(
    SESSION_COOKIE_SAMESITE='Lax',
    SESSION_COOKIE_SECURE=False,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_DOMAIN='192.168.202.201'  
)
app.config['DATABASE'] = 'db_users.db'
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['DATABASE_LOGS'] = 'access_logs.db'
app.config['TELEGRAM_BOT_TOKEN'] = '8142838834:AAEA42xudSOnnqaIZX6PjT77-VGcLDDYW04'

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

codes = {}  # {card_code: (code, timestamp)}

def get_db(database='main'):
    if database == 'logs':
        db = sqlite3.connect(app.config['DATABASE_LOGS'])
    else:
        db = sqlite3.connect(app.config['DATABASE'])
    db.row_factory = sqlite3.Row
    return db

def init_databases():
    with app.app_context():
        main_db = get_db()
        main_db.execute('''
            CREATE TABLE IF NOT EXISTS merged_users (
                users_id INTEGER PRIMARY KEY,
                code TEXT UNIQUE,
                last_name TEXT,
                first_name TEXT,
                middle_name TEXT,
                birthday TEXT,
                phone TEXT,
                email TEXT,
                work_place TEXT,
                services TEXT,
                hire_date TEXT,
                is_online INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                department TEXT,
                telegram_id TEXT
            )
        ''')
        main_db.commit()
        main_db.close()

        log_db = get_db('logs')
        log_db.execute('''
            CREATE TABLE IF NOT EXISTS access_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                access_time DATETIME
            )
        ''')
        log_db.commit()
        log_db.close()

def send_telegram_code(card_code):
    try:
        db = get_db()
        user = db.execute('''
            SELECT code, telegram_id FROM merged_users 
            WHERE code = ? AND telegram_id IS NOT NULL
        ''', (card_code,)).fetchone()
        db.close()

        if not user:
            logger.error("User %s not found or not linked", card_code)
            return False

        code = str(secrets.randbelow(10**6)).zfill(6)  # 000000-999999
        codes[card_code] = (code, time.time())
        logger.info("Generated code: %s for %s", code, card_code)

        message = f"🔐 Ваш код доступа: {code}\n⏳ Срок действия: 5 минут"
        response = requests.post(
            f"https://api.telegram.org/bot{app.config['TELEGRAM_BOT_TOKEN']}/sendMessage",
            json={
                'chat_id': user['telegram_id'],
                'text': message,
                'parse_mode': 'Markdown'
            },
            timeout=10
        )

        resp_data = response.json()
        if not resp_data.get('ok'):
            logger.error("Telegram API error: %s", resp_data.get('description'))
            return False

        logger.debug("Message sent to %s", user['telegram_id'])
        return True

    except Exception as e:
        logger.error("Critical error: %s", str(e), exc_info=True)
        return False

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        card_code = request.form['card_number'].strip()
        if send_telegram_code(card_code):
            return render_template('verify_code.html', card_code=card_code)
        return render_template('login.html', error="Ошибка отправки кода")
    return render_template('login.html')

@app.route('/verify_code', methods=['POST'])
def verify_code():
    try:
        logger.debug("Headers: %s", request.headers)
        logger.debug("Form data: %s", request.form)

        card_code = request.form.get('card_code', '').strip()
        user_code = request.form.get('code', '').strip()
        
        logger.debug("Received card_code: '%s', code: '%s'", card_code, user_code)

        # Проверка наличия обязательных полей
        if not card_code or not user_code:
            logger.error("Missing required fields")
            return jsonify({'error': 'Не указаны код карты или код подтверждения'}), 400

        # Проверка существования кода
        stored = codes.get(card_code)
        if not stored:
            logger.error("Code not found for card: %s", card_code)
            return jsonify({'error': 'Код устарел или не существует'}), 400

        code, timestamp = stored
        logger.debug("Stored code: %s (generated at %s)", code, timestamp)

        # Проверка времени действия
        if time.time() - timestamp > 300:
            logger.error("Code expired")
            del codes[card_code]
            return jsonify({'error': 'Время действия кода истекло'}), 400

        # Проверка совпадения кодов
        if code != user_code:
            logger.error("Code mismatch. Expected: %s, Received: %s", code, user_code)
            return jsonify({'error': 'Неверный код подтверждения'}), 400

        # Поиск пользователя в БД
        db = get_db()
        user = db.execute(
            'SELECT * FROM merged_users WHERE REPLACE(code, ",", "") = ?', 
            (card_code.replace(",", ""),)
        ).fetchone()
        db.close()

        if not user:
            logger.error("User not found in database")
            return jsonify({'error': 'Пользователь не существует'}), 404

        # Обновление сессии
        session.permanent = True
        session['user_id'] = user['users_id']
        logger.info("User authenticated: %s", user['code'])

        # Перенаправление на страницу профиля
        return redirect(url_for('profile', card_code=card_code))

    except Exception as e:
        logger.error("Error: %s", str(e), exc_info=True)
        return jsonify({'error': 'Внутренняя ошибка сервера'}), 500
    
@app.route('/api/profile')
def api_profile():
    # Проверка аутентификации пользователя
    if 'user_id' not in session:
        return jsonify({'error': 'Unauthorized'}), 401
    
    # Получение данных пользователя из БД
    user_id = session['user_id']
    db = get_db()
    user = db.execute('SELECT * FROM merged_users WHERE users_id = ?', (user_id,)).fetchone()
    db.close()
    
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    # Формирование данных пользователя
    user_data = {
        'code': user['code'],
        'first_name': user['first_name'],
        'last_name': user['last_name'],
        'middle_name': user['middle_name'],
        'email': user['email'],
        'phone': user['phone'],
        'department': user['department']
        # Добавьте другие необходимые поля
    }
    return jsonify(user_data)

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = 'http://localhost:5050'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response

@app.route('/resend_code/<card_code>', methods=['POST'])
def resend_code(card_code):
    try:
        if send_telegram_code(card_code):
            return render_template('verify_code.html', card_code=card_code, success="Код отправлен повторно")
        return render_template('verify_code.html', card_code=card_code, error="Ошибка повторной отправки")
    except Exception as e:
        logger.error("Resend error: %s", str(e))
        return render_template('verify_code.html', card_code=card_code, error="Внутренняя ошибка сервера")

@app.route('/profile/<card_code>')
def profile(card_code):
    logger.debug("Session data: %s", dict(session))
    if 'user_id' not in session:
        return redirect(url_for('login'))

    db = get_db()
    user = db.execute('SELECT * FROM merged_users WHERE code = ?', (card_code,)).fetchone()
    db.close()

    return render_template('profile.html', user=user)

if __name__ == '__main__':
    init_databases()
    app.run(host='192.168.202.201', port=5050, debug=True)