# Импорт необходимых модулей AirFlow для создания DAG и задач
from airflow import DAG
# Импорт оператора для выполнения Python функций
from airflow.operators.python import PythonOperator
# Импорт модулей для работы с датой и временем
from datetime import datetime, timedelta
# Импорт библиотеки для HTTP запросов
import requests
# Импорт модуля для работы с JSON данными   
import json
# Импорт модуля для работы с файловой системой
import os
# Импорт базового хука для работы с соединениями AirFlow
from airflow.hooks.base import BaseHook
# Импорт модуля для файловых блокировок (для Linux систем)
import fcntl

# Константа с путем к файлу для хранения подписчиков
# Файл будет создан в той же директории, где находится DAG файл
SUBSCRIBERS_FILE = '/opt/airflow/dags/subscribers.json'

def load_subscribers():
    """Загружает список подписчиков без блокировок"""
    try:
        if os.path.exists(SUBSCRIBERS_FILE):
            with open(SUBSCRIBERS_FILE, 'r') as f:
                data = json.load(f)
                print(f"Loaded {len(data)} subscribers")
                return data
        print("No subscribers file found")
        return []
    except Exception as e:
        print(f"❌ Ошибка загрузки subscribers: {e}")
        return []

def save_subscribers(subscribers):
    """Сохраняет список подписчиков без блокировок"""
    try:
        os.makedirs(os.path.dirname(SUBSCRIBERS_FILE), exist_ok=True)
        with open(SUBSCRIBERS_FILE, 'w') as f:
            json.dump(subscribers, f)
        print(f"Saved {len(subscribers)} subscribers")
    except Exception as e:
        print(f"❌ Ошибка сохранения subscribers: {e}")

def process_telegram_updates():
    """
    Основная функция для обработки обновлений от Telegram бота.
    """
    try:
        # Получаем соединение с Telegram
        conn = BaseHook.get_connection('telegram_generic')
        token = conn.extra_dejson.get('token')
        
        if not token:
            print("❌ Токен не найден")
            return

        # Получаем обновления
        url = f"https://api.telegram.org/bot{token}/getUpdates"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            updates = data.get('result', [])
            
            # Загружаем текущий список подписчиков
            subscribers = load_subscribers()
            
            for update in updates:
                message = update.get('message', {})
                text = message.get('text', '').strip().lower()
                chat_id = message.get('chat', {}).get('id')
                
                if not chat_id:
                    continue
                    
                # Обрабатываем команду /start
                if text == '/start':
                    if chat_id not in subscribers:
                        subscribers.append(chat_id)
                        save_subscribers(subscribers)
                        send_message(chat_id, "✅ Вы подписались на курсы валют!")
                        
                # Обрабатываем команду /stop
                elif text == '/stop':
                    if chat_id in subscribers:
                        subscribers.remove(chat_id)
                        save_subscribers(subscribers)
                        send_message(chat_id, "❌ Вы отписались от рассылки")
            
            # Помечаем обработанные обновления
            if updates:
                last_update_id = updates[-1]['update_id']
                requests.get(f"https://api.telegram.org/bot{token}/getUpdates?offset={last_update_id + 1}", timeout=5)
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")

        # Удаляем вебхук
        #try:
         #   delete_url = f"https://api.telegram.org/bot{token}/deleteWebhook"
         #   delete_response = requests.post(delete_url, params={'drop_pending_updates': True}, timeout=10)
         #   print(f"Delete webhook status: {delete_response.status_code}")
        #except Exception as e:
        #    print(f"❌ Ошибка при удалении webhook: {e}")
        #    return



def send_message(chat_id, text):
    """
    Отправляет текстовое сообщение в указанный чат Telegram.
    """
    # Получаем соединение с Telegram из AirFlow connections
    conn = BaseHook.get_connection('telegram_generic')
    # Извлекаем токен бота из дополнительных параметров соединения
    token = conn.extra_dejson['token']
    
    # Формируем URL для отправки сообщения через Telegram API
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    # Выполняем POST запрос с данными сообщения в формате JSON
    requests.post(url, json={'chat_id': chat_id, 'text': text})

# Создание DAG (Directed Acyclic Graph) - основного объекта AirFlow
with DAG(
    # Уникальный идентификатор DAG
    dag_id='telegram_subscription_bot',
    # Описание DAG для интерфейса AirFlow
    description='Обработка подписок на рассылку курсов валют',
    # Расписание выполнения: каждые 10 минут
    schedule='*/5 * * * *',
    # Дата начала работы DAG
    start_date=datetime(2025, 8, 26),
    # Отключение дозапуска пропущенных интервалов
    catchup=False,
    # Аргументы по умолчанию для всех задач в DAG
    default_args={
        # Количество повторных попыток при ошибке
        'retries': 1,
        # Задержка между попытками
        'retry_delay': timedelta(minutes=1),
    },
) as dag:

    # Создание задачи PythonOperator для обработки обновлений
    process_updates_task = PythonOperator(
        # Уникальный идентификатор задачи внутри DAG
        task_id='process_telegram_updates',
        # Python функция, которая будет выполнена
        python_callable=process_telegram_updates,
    )