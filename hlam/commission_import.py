import sqlite3  # Работа с SQLite
import requests  # HTTP-запросы к API
import os  # Работа с переменными окружения
from dotenv import load_dotenv  # Загрузка .env файла

# Загружаем переменные окружения из файла api.env
load_dotenv("../backend/api.env")

# Получаем API-ключ Wildberries из переменной окружения
WB_API_KEY = os.getenv("WB_API_KEY")

# Проверка: если ключа нет — останавливаем скрипт
if not WB_API_KEY:
    raise Exception("❌ Не задан WB_API_KEY в .env")

# Устанавливаем заголовки для авторизации
headers = {
    "Authorization": WB_API_KEY
}


# Функция получения комиссий по категориям
def fetch_commissions():
    print("🔄 Получаем комиссии с Wildberries...")

    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"

    try:
        response = requests.get(url, headers=headers)
        print("Ответ сервера:", response)
        response.raise_for_status()  # Проверка на ошибку статуса

        # Попробуем распарсить ответ как JSON
        raw_data = response.json()


        # Ответ API — это словарь с ключом 'data'
        if not isinstance(raw_data, dict):
            raise Exception(f"❌ Ожидался dict, но пришло: {type(raw_data)}")

        # Получаем список комиссий по категориям
        data = raw_data.get("report", [])


        # Если это не список — ошибка
        if not isinstance(data, list):
            raise Exception(f"❌ Ожидался список, но пришло: {type(data)}")

        # Формируем словарь: ключ — subjectName, значение — комиссия
        commissions = {
            item["subjectName"]: item["kgvpSupplier"]
            for item in data if "subjectName" in item and "kgvpSupplier" in item
        }

        print(f"✅ Получено комиссий: {len(commissions)}")
        return commissions

    except Exception as e:
        print(f"❌ Ошибка при получении комиссий: {e}")
        return {}


# Функция обновления комиссии в базе данных
def update_commissions_in_db(commissions_dict):
    print("📝 Обновляем комиссии в базе данных...")

    # Подключаемся к базе
    conn = sqlite3.connect("../backend/wildberries_cards.db")
    cursor = conn.cursor()

    updated = 0

    for subject_name, commission in commissions_dict.items():
        # Обновляем все карточки с нужной категорией
        cursor.execute("""
                       UPDATE cards
                       SET commission_percent = ?
                       WHERE subjectName = ?
                       """, (commission, subject_name))
        updated += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"✅ Обновлено карточек: {updated}")


# Точка входа
if __name__ == "__main__":
    commissions = fetch_commissions()
    if commissions:
        update_commissions_in_db(commissions)
    else:
        print("⚠️ Комиссии не были загружены.")




