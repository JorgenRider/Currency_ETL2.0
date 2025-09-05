from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def fetch_historical_rates():
    """Загружает исторические данные за последний год"""
    hook = PostgresHook(postgres_conn_id='currency_db_connection')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Определяем диапазон
    end_date = datetime(2025, 5, 28)
    start_date = datetime(2010, 8, 10)
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%d/%m/%Y")
        print(f"Загружаем данные за {date_str}")
        
        try:
            url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={date_str}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                xml_data = BeautifulSoup(response.text, "xml")
                my_code_list = ['USD', 'EUR', 'IDR']
                
                for currency in xml_data.find_all("Valute"):
                    if currency.find("CharCode").text in my_code_list:
                        code = currency.find("CharCode").text
                        nominal = int(currency.find("Nominal").text)
                        value = float(currency.find("Value").text.replace(",", "."))
                        
                        # Проверяем, нет ли уже записи на эту дату
                        check_query = """
                        SELECT COUNT(*) FROM OLTP_Layer.raq_currency 
                        WHERE code = %s AND timestamp::date = %s
                        """
                        cursor.execute(check_query, (code, current_date.date()))
                        exists = cursor.fetchone()[0]
                        
                        if not exists:
                            insert_query = """
                            INSERT INTO OLTP_Layer.raq_currency (code, nominal, rate, timestamp)
                            VALUES (%s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (code, nominal, value, current_date))
                            print(f"Добавлено: {code} на {current_date.date()}")
            
            conn.commit()
            
        except Exception as e:
            print(f"Ошибка для даты {date_str}: {e}")
            conn.rollback()
        
        current_date += timedelta(days=1)
    
    cursor.close()
    conn.close()

# DAG для однократного запуска
with DAG(
    dag_id='load_historical_currency_data',
    description='Загрузка исторических данных о курсах валют',
    schedule=None,  # Без расписания, запуск вручную
    start_date=datetime(2025, 8, 31),
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:

    load_task = PythonOperator(
        task_id='load_historical_rates',
        python_callable=fetch_historical_rates,
    )
