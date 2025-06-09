import os
import httpx
import asyncio
import requests
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 🔐 Загружаем ключ
load_dotenv("../backend/api.env")
WB_API_KEY = os.getenv("WB_API_KEY")

# 📅 Определяем даты
yesterday = (datetime.utcnow() - timedelta(days=2)).date().isoformat()
today = (datetime.utcnow() - timedelta(days=1)).date().isoformat()


# 1️⃣ Получаем все карточки
async def fetch_all_cards():
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers = {
        "Authorization": WB_API_KEY,
        "Content-Type": "application/json"
    }

    limit = 100
    cursor = {}
    all_cards = []

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

            all_cards.extend(cards)

            if total < limit:
                break

            cursor = {
                "updatedAt": cursor_data.get("updatedAt"),
                "nmID": cursor_data.get("nmID")
            }

    nm_ids = [card.get("nmID") for card in all_cards if card.get("nmID")]
    print(f"✅ Получено карточек: {len(nm_ids)}")
    return nm_ids


# 2️⃣ Получаем продажи по артикулам
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

    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP ошибка: {http_err}")
    except Exception as err:
        print(f"❌ Ошибка: {err}")

    return []


# 3️⃣ Сохраняем в БД
def save_sales_to_db(sales_data: list):
    conn = sqlite3.connect("../backend/wildberries_cards.db")
    cursor = conn.cursor()

    # 🔍 Проверка и добавление новых колонок
    cursor.execute("PRAGMA table_info(sales)")
    existing_columns = [row[1] for row in cursor.fetchall()]
    required_columns = {
        "realsales": "INTEGER DEFAULT 0",
        "quantity": "INTEGER DEFAULT 0",
        "revenue": "REAL DEFAULT 0",
        "returns": "INTEGER DEFAULT 0",
        "opens": "INTEGER DEFAULT 0",
        "atc": "INTEGER DEFAULT 0",
        "buyouts": "INTEGER DEFAULT 0",
        "buyout_percent": "REAL DEFAULT 0",
        "add_to_cart_conv": "REAL DEFAULT 0",
        "cart_to_order_conv": "REAL DEFAULT 0",
        "vendor_code": "TEXT",
        "imt_id": "INTEGER"
    }

    for column, col_type in required_columns.items():
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE sales ADD COLUMN {column} {col_type}")

    # 💾 Запись данных
    for entry in sales_data:
        nm_id = entry["nmID"]
        vendor_code = entry.get("vendorCode", "")
        imt_id = entry.get("imtID", 0)
        history = entry.get("history", [])

        for day in history:
            date = day["dt"]
            opens = day.get("openCardCount", 0)
            atc = day.get("addToCartCount", 0)
            orders = day.get("ordersCount", 0)
            revenue = day.get("ordersSumRub", 0)
            buyouts = day.get("buyoutsCount", 0)
            buyout_sum = day.get("buyoutsSumRub", 0)
            buyout_percent = day.get("buyoutPercent", 0)
            add_to_cart_conv = day.get("addToCartConversion", 0)
            cart_to_order_conv = day.get("cartToOrderConversion", 0)

            cursor.execute("""
                INSERT INTO sales (
                    date, nm_id, vendor_code, imt_id,
                    realsales, quantity, revenue, returns,
                    opens, atc, buyouts,
                    buyout_percent, add_to_cart_conv, cart_to_order_conv
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                date, nm_id, vendor_code, imt_id,
                buyouts, orders, revenue, 0,
                opens, atc, buyouts,
                buyout_percent, add_to_cart_conv, cart_to_order_conv
            ))

            print(f"✅ Обновлено: nmID {nm_id} на {date}")

    conn.commit()
    conn.close()



# 4️⃣ Основной запуск
async def main():
    token = WB_API_KEY
    nm_ids = await fetch_all_cards()

    # ⚠️ API принимает до 20 артикулов за раз — разобьём на батчи
    batch_size = 20
    all_sales = []

    print("📊 Запрашиваем продажи по партиям...")

    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i + batch_size]
        print(f"⏳ Запрос {i // batch_size + 1} из {len(nm_ids) // batch_size + 1}")

        sales_data = get_sales_data(batch, token)
        if sales_data:
            all_sales.extend(sales_data)

        await asyncio.sleep(23)  # 💤 защита от 429 ошибок

    print("💾 Сохраняем всё в БД...")
    save_sales_to_db(all_sales)
    print("🎉 Готово.")


# 🚀 Запуск
if __name__ == "__main__":
    asyncio.run(main())
