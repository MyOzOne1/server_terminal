import logging
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import random
import string
import datetime
from typing import Dict, Optional

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Настройки VK
VK_TOKEN = "vk1.a.FPDePh1s0WC-5HZvM-_6o8491Yae_v49v9H1aeUS2V8E4plvIuo8A-08vVqKr9nO-9kCF-oCUJdGRys12NB9SNfRnGNc9VavOSLrdADiBEJKAN3Xvm2CYB822M8Ot-_gQ_G6F6y024PX3nf9FNI_BYCp5vJwpm3JPBt_dtaTU4SC4kZMyRo7Hr7Ne6xSnE7WaZpXdHybIRAaXYVyVYPQog"
GROUP_ID = "my_photo_live4"  # ID вашей группы

# Хранилище для кодов подтверждения (в реальном приложении лучше использовать БД)
verification_codes: Dict[int, Dict[str, str]] = {}

# Инициализация VK API
vk_session = vk_api.VkApi(token=VK_TOKEN)
vk = vk_session.get_api()
longpoll = VkBotLongPoll(vk_session, GROUP_ID)

def generate_verification_code(length: int = 6) -> str:
    """Генерация случайного кода подтверждения"""
    return ''.join(random.choices(string.digits, k=length))

def send_message(user_id: int, message: str, keyboard: Optional[str] = None):
    """Отправка сообщения пользователю"""
    try:
        params = {
            'user_id': user_id,
            'message': message,
            'random_id': 0
        }
        
        if keyboard:
            params['keyboard'] = keyboard
            
        vk.messages.send(**params)
    except Exception as e:
        logger.error("Error sending message: %s", e)

def create_keyboard() -> str:
    """Создание клавиатуры с кнопкой для запроса кода"""
    keyboard = {
        "one_time": True,
        "buttons": [
            [
                {
                    "action": {
                        "type": "text",
                        "label": "Получить код подтверждения"
                    },
                    "color": "primary"
                }
            ]
        ]
    }
    
    return str(keyboard).replace("'", '"')

def handle_start(user_id: int):
    """Обработчик начала взаимодействия"""
    keyboard = create_keyboard()
    message = (
        "👋 Добро пожаловать!\n"
        "Для подтверждения вашей личности нам необходимо отправить вам проверочный код.\n\n"
        "Нажмите кнопку ниже, чтобы получить код подтверждения."
    )
    send_message(user_id, message, keyboard)

def handle_code_request(user_id: int):
    """Обработчик запроса кода подтверждения"""
    # Генерируем код
    code = generate_verification_code()
    
    # Сохраняем код с временной меткой
    verification_codes[user_id] = {
        'code': code,
        'timestamp': datetime.datetime.now().isoformat()
    }
    
    # Отправляем код пользователю
    message = (
        f"✅ Ваш проверочный код: {code}\n\n"
        "Пожалуйста, введите этот код в течение 5 минут для подтверждения вашей личности.\n\n"
        "Просто отправьте мне сообщение с этим кодом."
    )
    send_message(user_id, message)

def handle_code_verification(user_id: int, code: str):
    """Обработчик проверки кода"""
    # Проверяем, есть ли код для этого пользователя
    if user_id not in verification_codes:
        send_message(user_id, "❌ Сначала запросите код подтверждения с помощью кнопки.")
        return
    
    user_data = verification_codes[user_id]
    stored_code = user_data['code']
    timestamp = datetime.datetime.fromisoformat(user_data['timestamp'])
    
    # Проверяем, не истекло ли время действия кода (5 минут)
    if datetime.datetime.now() - timestamp > datetime.timedelta(minutes=5):
        del verification_codes[user_id]  # Удаляем просроченный код
        send_message(user_id, "❌ Время действия кода истекло. Запросите новый код.")
        return
    
    # Проверяем совпадение кода
    if code == stored_code:
        del verification_codes[user_id]  # Удаляем использованный код
        send_message(user_id, "✅ Код подтвержден! Ваша личность успешно проверена.")
        # Здесь можно добавить логику после успешной проверки
    else:
        send_message(user_id, "❌ Неверный код. Пожалуйста, попробуйте еще раз.")

def main():
    """Основная функция запуска бота"""
    logger.info("VK Verification Bot started")
    
    # Основной цикл обработки событий
    for event in longpoll.listen():
        if event.type == VkBotEventType.MESSAGE_NEW:
            msg = event.object.message
            user_id = msg['from_id']
            text = msg['text'].strip().lower()
            
            # Обработка команды "начать" или первого сообщения
            if text in ['начать', 'start', 'привет']:
                handle_start(user_id)
            
            # Обработка запроса кода подтверждения
            elif text == 'получить код подтверждения':
                handle_code_request(user_id)
            
            # Обработка введенного кода (проверяем, что это 6-значное число)
            elif text.isdigit() and len(text) == 6:
                handle_code_verification(user_id, text)
            
            # Обработка неизвестных сообщений
            else:
                send_message(user_id, "Я не понимаю ваше сообщение. Используйте кнопки для взаимодействия.")

if __name__ == '__main__':
    main()