import os
import httpx
import asyncio
import requests
import psycopg2

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)
from datetime import datetime, timedelta
from dotenv import load_dotenv
from collections import defaultdict

# 🔐 Загрузка токена
load_dotenv("../backend/api.env")
WB_API_KEY = os.getenv("WB_API_KEY")

# 🕛 Даты для выборки
yesterday = (datetime.utcnow() - timedelta(days=2)).date().isoformat()
today = yesterday


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
                nm_id = card.get("nmID")
                if nm_id:
                    all_cards[nm_id] = {
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


# === 2. Получение метрик продаж ===
def get_sales_data(nm_ids: list, token: str):
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    headers = {"Authorization": token}

    payload = {
        "nmIDs": nm_ids,
        "period": {
            "begin": yesterday,
            "end": today
        },
        "timezone": "Europe/Moscow",
        "aggregationLevel": "day"
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])
    except Exception as err:
        print(f"❌ Ошибка запроса: {err}")
        return []


# === 3. Сохранение в БД с агрегацией ===
def save_sales_to_db(sales_data: list, cards_info: dict):
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Создание таблицы
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        date TEXT,
        nm_id INTEGER,
        vendor_code TEXT,
        imt_id INTEGER,
        realsales INTEGER DEFAULT 0,
        quantity INTEGER DEFAULT 0,
        revenue REAL DEFAULT 0,
        returns INTEGER DEFAULT 0,
        opens INTEGER DEFAULT 0,
        atc INTEGER DEFAULT 0,
        buyouts INTEGER DEFAULT 0,
        buyout_percent REAL DEFAULT 0,
        add_to_cart_conv REAL DEFAULT 0,
        cart_to_order_conv REAL DEFAULT 0,
        PRIMARY KEY (date, nm_id)
    )
    """)

    # Агрегация
    aggregated = defaultdict(lambda: {
        "realsales": 0,
        "quantity": 0,
        "revenue": 0,
        "returns": 0,
        "opens": 0,
        "atc": 0,
        "buyouts": 0,
        "buyout_percent": 0.0,
        "add_to_cart_conv": 0.0,
        "cart_to_order_conv": 0.0,
        "vendor_code": "",
        "imt_id": 0,
        "count": 0
    })

    for entry in sales_data:
        nm_id = entry["nmID"]
        history = entry.get("history", [])
        card_meta = cards_info.get(nm_id, {})
        vendor_code = card_meta.get("vendorCode", "")
        imt_id = card_meta.get("imtID", 0)

        for day in history:
            date = day["dt"]
            key = (date, nm_id)
            group = aggregated[key]

            group["realsales"] += day.get("buyoutsCount", 0)
            group["quantity"] += day.get("ordersCount", 0)
            group["revenue"] += day.get("ordersSumRub", 0)
            group["returns"] += 0
            group["opens"] += day.get("openCardCount", 0)
            group["atc"] += day.get("addToCartCount", 0)
            group["buyouts"] += day.get("buyoutsCount", 0)
            group["buyout_percent"] += day.get("buyoutPercent", 0.0)
            group["add_to_cart_conv"] += day.get("addToCartConversion", 0.0)
            group["cart_to_order_conv"] += day.get("cartToOrderConversion", 0.0)
            group["vendor_code"] = vendor_code
            group["imt_id"] = imt_id
            group["count"] += 1

    # Запись агрегированных данных
    for (date, nm_id), d in aggregated.items():
        count = d["count"] if d["count"] > 0 else 1
        cursor.execute("""
            INSERT INTO sales (
                date, nm_id, vendor_code, imt_id,
                realsales, quantity, revenue, returns,
                opens, atc, buyouts,
                buyout_percent, add_to_cart_conv, cart_to_order_conv
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(date, nm_id) DO UPDATE SET
                vendor_code=excluded.vendor_code,
                imt_id=excluded.imt_id,
                realsales=excluded.realsales,
                quantity=excluded.quantity,
                revenue=excluded.revenue,
                returns=excluded.returns,
                opens=excluded.opens,
                atc=excluded.atc,
                buyouts=excluded.buyouts,
                buyout_percent=excluded.buyout_percent,
                add_to_cart_conv=excluded.add_to_cart_conv,
                cart_to_order_conv=excluded.cart_to_order_conv
        """, (
            date, nm_id, d["vendor_code"], d["imt_id"],
            d["realsales"], d["quantity"], d["revenue"], d["returns"],
            d["opens"], d["atc"], d["buyouts"],
            round(d["buyout_percent"] / count, 2),
            round(d["add_to_cart_conv"] / count, 2),
            round(d["cart_to_order_conv"] / count, 2)
        ))

        print(f"✅ Обновлено: nmID {nm_id} на {date}")

    conn.commit()
    conn.close()


# === 4. Основной скрипт ===
async def main():
    token = WB_API_KEY
    cards_info = await fetch_all_cards()
    nm_ids = list(cards_info.keys())

    batch_size = 20
    all_sales = []

    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i + batch_size]
        print(f"⏳ Запрос {i // batch_size + 1} из {len(nm_ids) // batch_size + 1}")
        sales_data = get_sales_data(batch, token)
        all_sales.extend(sales_data)
        await asyncio.sleep(21)

    save_sales_to_db(all_sales, cards_info)
    print("🎉 Завершено")


if __name__ == "__main__":
    asyncio.run(main())