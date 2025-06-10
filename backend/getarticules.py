import os
import httpx
import asyncio
import psycopg2
from dotenv import load_dotenv

# 🔐 Загрузка токена и переменных окружения
load_dotenv("api.env")
WB_API_KEY = os.getenv("WB_API_KEY")
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)


# === 1. Получение карточек с Content API ===
async def fetch_all_cards():
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers = {
        "Authorization": WB_API_KEY,
        "Content-Type": "application/json"
    }

    limit = 100
    cursor = {}
    all_cards = {}

    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            payload = {
                "settings": {
                    "cursor": {"limit": limit},
                    "filter": {"withPhoto": -1}
                }
            }

            if cursor:
                payload["settings"]["cursor"].update(cursor)

            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            raw_data = response.json()

            cards = raw_data.get("cards", [])
            cursor_data = raw_data.get("cursor", {})
            total = cursor_data.get("total", 0)

            for card in cards:
                nmID = card.get("nmID")
                if nmID:
                    all_cards[nmID] = {
                        "imtID": card.get("imtID"),
                        "vendorCode": card.get("vendorCode"),
                        "brand": card.get("brand"),
                        "subjectName": card.get("subjectName")
                    }

            if total < limit:
                break

            cursor = {
                "updatedAt": cursor_data.get("updatedAt"),
                "nmID": cursor_data.get("nmID")
            }

    print(f"✅ Получено карточек: {len(all_cards)}")
    return all_cards

# === 2. Сохранение карточек в PostgreSQL ===
def save_cards_to_db(cards: dict):
    conn = psycopg2.connect(
    dsn=DB_URL
    )
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS cards (
            nm_id INTEGER PRIMARY KEY,
            imt_id INTEGER,
            vendor_code TEXT,
            brand TEXT,
            subject_name TEXT
        )
    ''')

    for nm_id, data in cards.items():
        cur.execute('''
            INSERT INTO cards (nm_id, imt_id, vendor_code, brand, subject_name)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (nm_id) DO UPDATE SET
                imt_id = EXCLUDED.imt_id,
                vendor_code = EXCLUDED.vendor_code,
                brand = EXCLUDED.brand,
                subject_name = EXCLUDED.subject_name
        ''', (nm_id, data["imtID"], data["vendorCode"], data["brand"], data["subjectName"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Данные успешно сохранены в базу.")

# === 3. Основной запуск ===
if __name__ == "__main__":
    cards = asyncio.run(fetch_all_cards())
    save_cards_to_db(cards)
