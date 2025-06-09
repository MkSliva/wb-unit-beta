import os
from statistics import quantiles

import httpx
import asyncio
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from collections import defaultdict
import json
import pandas as pd

# 🔐 Загрузка токена
load_dotenv("api.env")
WB_API_KEY = os.getenv("WB_API_KEY")
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

# 🕛 Даты для выборки
yesterday = (datetime.utcnow() - timedelta(days=0)).date().isoformat()





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


# === 2. Получение метрик продаж ===
def get_sales_data(nmIDs: list, token: str):
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    headers = {"Authorization": token}

    payload = {
        "nmIDs": nmIDs,
        "period": {
            "begin": yesterday,
            "end": yesterday
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


# === 3. Получение рекламных метрик ===
def get_ad_metrics():
    API_KEY = WB_API_KEY
    url_campaigns = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    headers = {"Authorization": API_KEY, "Content-Type": "application/json"}

    r = requests.get(url_campaigns, headers=headers)
    if r.status_code != 200:
        print("❌ Не удалось получить кампании")
        return {}

    campaign_ids = []
    for group in r.json().get("adverts", []):
        for advert in group.get("advert_list", []):
            campaign_ids.append(advert.get("advertId"))

    body = [{"id": cid, "dates": [yesterday]} for cid in campaign_ids]

    url_stats = "https://advert-api.wildberries.ru/adv/v2/fullstats"
    response = requests.post(url_stats, headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        print("❌ Ошибка запроса метрик рекламы")
        return {}

    data = response.json()
    aggregated = defaultdict(lambda: {
        "ad_views": 0, "ad_clicks": 0, "ad_ctr": 0, "ad_cpc": 0, "ad_spend": 0,
        "ad_atbs": 0, "ad_orders": 0, "ad_cr": 0, "ad_shks": 0, "ad_sum_price": 0
    })

    for campaign in data:

        for day in campaign.get("days", []):

            for app in day.get("apps", []):

                for item in app.get("nm", []):

                    nmID = item.get("nmId")
                    group = aggregated[nmID]
                    group["ad_views"] += safe_int(item.get("views"))
                    group["ad_clicks"] += safe_int(item.get("clicks"))
                    group["ad_spend"] += safe_int(item.get("sum"))
                    group["ad_atbs"] += safe_int(item.get("atbs"))
                    group["ad_orders"] += safe_int(item.get("orders"))
                    group["ad_shks"] += safe_int(item.get("shks"))
                    group["ad_sum_price"] += safe_int(item.get("sum_price"))

    for data in aggregated.values():
        views = data["ad_views"]
        clicks = data["ad_clicks"]
        orders = data["ad_orders"]
        data["ad_ctr"] = round((clicks / views) * 100, 2) if views else 0
        data["ad_cpc"] = round(data["ad_spend"] / clicks, 2) if clicks else 0
        data["ad_cr"] = round((orders / clicks) * 100, 2) if clicks else 0

    return aggregated


# === 4. Сохранение в БД ===
def save_sales_to_db(sales_data: list, cards_info: dict, ad_data: dict):
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()
    # Создание таблицы, если не существует
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            nm_ID INTEGER,
            date TEXT,
            imtName TEXT,
            total_profit REAL,
            ordersCount INTEGER,
            brand TEXT,
            subjectName TEXT,
            salePrice REAL,
            purchase_price REAL,
            delivery_to_warehouse REAL,
            wb_commission_rub REAL,
            wb_logistics REAL,
            tax_rub REAL,
            packaging REAL,
            fuel REAL,
            gift REAL,
            defect_percent REAL,
            cost_price REAL,
            profit_per_item REAL,
            commission_percent REAL,
            ad_views INTEGER,
            ad_clicks INTEGER,
            ad_ctr REAL,
            ad_cpc REAL,
            ad_spend REAL,
            ad_atbs INTEGER,
            ad_orders INTEGER,
            ad_cr REAL,
            ad_shks INTEGER,
            ad_sum_price REAL,
            quantity INTEGER,
            vendorCode TEXT,
            imtID INTEGER,
            openCardCount INTEGER,
            addToCartCount INTEGER,
            ordersSumRub INTEGER,
            buyoutsCount INTEGER
        )
    """)


    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # 📦 Получение справочной информации из таблицы cards
    cursor.execute("SELECT nmID, brand, subjectName, salePrice, purchase_price, delivery_to_warehouse, wb_commission_rub, wb_logistics, tax_rub, packaging, fuel, gift, defect_percent, cost_price, profit_per_item, commission_percent FROM cards")
    card_details_raw = cursor.fetchall()
    card_details = {
        row[0]: {
            "brand": row[1] or "",
            "subjectName": row[2] or "",
            "salePrice": row[3] or 0,
            "purchase_price": row[4] or 0,
            "delivery_to_warehouse": row[5] or 0,
            "wb_commission_rub": row[6] or 0,
            "wb_logistics": row[7] or 0,
            "tax_rub": row[8] or 0,
            "packaging": row[9] or 0,
            "fuel": row[10] or 0,
            "gift": row[11] or 0,
            "defect_percent": row[12] or 0,
            "cost_price": row[13] or 0,
            "profit_per_item": row[14] or 0,
            "commission_percent": row[15] or 0
        } for row in card_details_raw
    }

    for entry in sales_data:
        vendorCode = entry.get("vendorCode", "")
        imtID = entry.get("imtID", 0)

        nmID = entry["nmID"]
        imtName = entry["imtName"]
        for record in entry["history"]:

            date = record["dt"]
            record["date"] = record.pop("dt")  # заменяем dt на date
            date = record["date"]

            quantity = record["ordersCount"]

            ad_spend = ad_data.get(nmID, {}).get("ad_spend", 0)
            profit_per_item = card_details.get(nmID, {}).get("profit_per_item", 0)
            total_profit = round((profit_per_item * quantity) - ad_spend, 2)
            print(total_profit)




            cursor.execute(
                "SELECT COUNT(*) FROM sales WHERE nm_ID = %s AND date = %s",
                (nmID, date),
            )
            exists = cursor.fetchone()[0] > 0

            merged = {
                **record,
                **ad_data.get(nmID, {}),
                **card_details.get(nmID, {}),
                "total_profit": total_profit,
                "vendorCode": vendorCode,
                "imtID": cards_info.get(nmID, {}).get("imtID", None),
                "imtName": imtName,

            }
            ensure_columns_exist(conn, "sales", merged)
            if exists:
                placeholders = ", ".join([f"{k} = %s" for k in merged])
                values = list(merged.values()) + [nmID, date]
                cursor.execute(
                    f"UPDATE sales SET {placeholders} WHERE nm_ID = %s AND date = %s",
                    values,
                )
            else:
                columns = ["nm_ID", "date"] + list(merged.keys())
                values = [nmID, date] + list(merged.values())
                placeholders = ", ".join(["%s"] * len(columns))
                cursor.execute(
                    f"INSERT INTO sales ({', '.join(columns)}) VALUES ({placeholders})",
                    values,
                )

    conn.commit()
    conn.close()


# === 5. Основной скрипт ===
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
        await asyncio.sleep(20)

    ad_metrics = get_ad_metrics()
    save_sales_to_db(all_sales, cards_info, ad_metrics)
    print("🎉 Завершено")


def calculate_total_profit_for_day():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Получаем все строки за вчерашний день
    yesterday = (datetime.utcnow() - timedelta(days=0)).date().isoformat()
    cursor.execute(
        "SELECT total_profit FROM sales WHERE date = %s",
        (yesterday,),
    )
    rows = cursor.fetchall()

    # Суммируем profit
    total = sum(row[0] for row in rows if row[0] is not None)

    print(f"💰 Общая прибыль за {yesterday}: {total:.2f} ₽")
    conn.close()



def export_sales_to_excel():
    # Подключение к БД
    conn = psycopg2.connect(DB_URL)

    # Получаем дату вчерашнего дня
    yesterday = (datetime.utcnow() - timedelta(days=0)).date().isoformat()

    # Запрос всех строк за вчера
    query = "SELECT * FROM sales WHERE date = %s"
    df = pd.read_sql_query(query, conn, params=(yesterday,))

    # Сохраняем в Excel
    filename = f"sales_export_{yesterday}.xlsx"
    df.to_excel(filename, index=False)
    print(f"✅ Данные успешно экспортированы в {filename}")

    conn.close()

def ensure_columns_exist(conn, table_name, data_dict):
    cursor = conn.cursor()

    # Получаем список уже существующих столбцов
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
        (table_name,),
    )
    existing_columns = [row[0] for row in cursor.fetchall()]

    # Проверяем, каких не хватает
    for key in data_dict.keys():
        if key not in existing_columns:
            print(f"🛠 Добавляем столбец: {key}")
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {key} REAL")
            except Exception as e:
                print(f"❌ Ошибка при добавлении столбца {key}: {e}")

    conn.commit()

def safe_int(val):
    return int(val) if isinstance(val, (int, float)) else 0



def calculate_bundle_profits():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Получаем все нужные данные
    cursor.execute("""
        SELECT imtID, imtName, total_profit
        FROM sales
        WHERE date = (SELECT MAX(date) FROM sales)
    """)
    rows = cursor.fetchall()
    conn.close()

    # Группировка по связкам (по imtID)
    bundles = defaultdict(list)

    for imtID, imtName, profit in rows:
        profit = profit or 0  # если вдруг NULL
        bundles[imtID].append({"imtName": imtName, "profit": profit})

    # Считаем прибыль по каждой связке и выбираем имя
    results = []
    for imtID, items in bundles.items():
        total = sum(item["profit"] for item in items)
        best_item = max(items, key=lambda x: x["profit"])
        results.append({
            "imtID": imtID,
            "title": best_item["imtName"],
            "total_profit": round(total, 2)
        })

    # Вывод в консоль
    print("📦 Прибыль по связкам:")
    for bundle in sorted(results, key=lambda x: -x["total_profit"]):
        print(f"{bundle['title']} → {bundle['total_profit']} ₽")

    return results  # можно будет использовать в FastAPI

def calculate_profit_by_bundles():
    # Вчерашняя дата
    yesterday = (datetime.utcnow() - timedelta(days=0)).date().isoformat()

    # Подключение к базе
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Получаем все данные по вчерашнему дню
    cursor.execute(
        """
        SELECT imtID, imtName, vendorCode, cost_price, total_profit, ordersCount
        FROM sales
        WHERE imtID IS NOT NULL
          AND total_profit IS NOT NULL
          AND date = %s
        """,
        (yesterday,),
    )
    rows = cursor.fetchall()

    # Группировка по связкам
    bundles = {}
    for imtID, imtName, vendorCode, cost_price, profit, orders in rows:
        if imtID not in bundles:
            bundles[imtID] = []
        bundles[imtID].append({
            "imtName": imtName,
            "vendorCode": vendorCode,
            "cost_price": cost_price,
            "profit": profit,
            "ordersCount": orders
        })

    # Обработка результатов
    results = []
    for imtID, items in bundles.items():
        total_profit = sum(item["profit"] for item in items)
        best_item = max(items, key=lambda x: x["profit"])
        results.append({
            "imtID": imtID,
            "title": best_item["imtName"],
            "total_profit": round(total_profit, 2),
            "items": items
        })

    # Сортировка
    results = sorted(results, key=lambda x: -x["total_profit"])

    # Вывод
    print(f"\n📊 Прибыль по связкам за {yesterday}:")
    for bundle in results:
        print(f"\n🔹 {bundle['title']} → {bundle['total_profit']} ₽")
        print("   📦 Товары в связке:")
        for item in bundle["items"]:
            print(f"     • {item['vendorCode']} | Себестоимость: {item['cost_price']} ₽ | Заказов: {item['ordersCount']} шт")

#asyncio.run(main())
#calculate_total_profit_for_day()

if __name__ == "__main__":
    asyncio.run(main())
    calculate_total_profit_for_day()
    calculate_profit_by_bundles()

