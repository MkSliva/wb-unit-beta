import os
import requests
import psycopg2
from dotenv import load_dotenv

# 🔐 Загрузка переменных окружения
load_dotenv("../backend/api.env")
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)
WB_API_KEY = os.getenv("WB_API_KEY")

if not WB_API_KEY:
    raise Exception("❌ Не задан WB_API_KEY в .env")

headers = {
    "Authorization": WB_API_KEY
}

# === 1. Получение комиссий с WB ===
def fetch_commissions():
    print("🔄 Получаем комиссии с Wildberries...")
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"

    try:
        response = requests.get(url, headers=headers)
        print("Ответ сервера:", response.text)
        response.raise_for_status()

        raw_data = response.json()
        if not isinstance(raw_data, dict):
            raise Exception(f"❌ Ожидался dict, но пришло: {type(raw_data)}")

        data = raw_data.get("report", [])
        if not isinstance(data, list):
            raise Exception(f"❌ Ожидался список, но пришло: {type(data)}")

        commissions = {
            item["subjectName"].strip().lower(): item["kgvpSupplier"]
            for item in data if "subjectName" in item and "kgvpSupplier" in item
        }

        print(f"✅ Получено комиссий: {len(commissions)}")
        return commissions

    except Exception as e:
        print(f"❌ Ошибка при получении комиссий: {e}")
        return {}


# === 2. Обновление комиссий в таблице cards ===
def update_commissions_in_db(commissions_dict):
    print("📝 Обновляем комиссии в базе данных...")

    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Проверим и создадим колонку commission_percent, если она отсутствует
    try:
        cursor.execute("ALTER TABLE cards ADD COLUMN commission_percent REAL")
        conn.commit()
        print("🌟 Колонка commission_percent добавлена.")
    except psycopg2.errors.DuplicateColumn:
        conn.rollback()
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Ошибка при добавлении commission_percent: {e}")

    updated = 0
    for subject_name, commission in commissions_dict.items():
        cursor.execute(
            """
            UPDATE cards
            SET commission_percent = %s
            WHERE LOWER(TRIM(subject_name)) = %s
            """,
            (commission, subject_name)
        )
        updated += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"✅ Обновлено карточек: {updated}")


# === 3. Запуск ===
if __name__ == "__main__":
    commissions = fetch_commissions()
    if commissions:
        update_commissions_in_db(commissions)
    else:
        print("⚠️ Комиссии не были загружены.")
