from datetime import datetime, timedelta
import sqlite3
import asyncio
import requests
import httpx
import json
from dotenv import load_dotenv
import os
import pandas as pd
from collections import defaultdict

load_dotenv("api.env")
WB_API_KEY = os.getenv("WB_API_KEY")

# === Safe int ===
def safe_int(val):
    return int(val) if isinstance(val, (int, float)) else 0

# === Добавление недостающих столбцов ===
def ensure_columns_exist(conn, table_name, data_dict):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = [row[1] for row in cursor.fetchall()]
    for key in data_dict.keys():
        if key not in existing_columns:
            print(f"🛠 Добавляем столбец: {key}")
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {key} REAL")
            except Exception as e:
                print(f"❌ Ошибка при добавлении столбца {key}: {e}")
    conn.commit()

# === Получение карточек ===
async def fetch_all_cards():
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers = {"Authorization": WB_API_KEY, "Content-Type": "application/json"}
    limit, cursor, all_cards = 100, {}, {}
    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            payload = {"settings": {"cursor": {"limit": limit}, "filter": {"withPhoto": -1}}}
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

# === Получение продаж ===
def get_sales_data(nmIDs: list, token: str, date: str):
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    headers = {"Authorization": token}
    payload = {
        "nmIDs": nmIDs,
        "period": {"begin": date, "end": date},
        "timezone": "Europe/Moscow",
        "aggregationLevel": "day"
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as err:
        print(f"❌ Ошибка запроса: {err}")
        return []

# === Получение рекламы ===
def get_ad_metrics(date: str):
    API_KEY = WB_API_KEY
    headers = {"Authorization": API_KEY, "Content-Type": "application/json"}
    r = requests.get("https://advert-api.wildberries.ru/adv/v1/promotion/count", headers=headers)
    if r.status_code != 200:
        print("❌ Не удалось получить кампании")
        return {}
    campaign_ids = [advert.get("advertId") for group in r.json().get("adverts", []) for advert in group.get("advert_list", [])]
    body = [{"id": cid, "dates": [date]} for cid in campaign_ids]
    r2 = requests.post("https://advert-api.wildberries.ru/adv/v2/fullstats", headers=headers, data=json.dumps(body))
    if r2.status_code != 200:
        print("❌ Ошибка запроса метрик рекламы")
        return {}
    data = r2.json()
    aggregated = defaultdict(lambda: {k: 0 for k in ["ad_views", "ad_clicks", "ad_spend", "ad_atbs", "ad_orders", "ad_shks", "ad_sum_price"]})
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
        views, clicks, orders = data["ad_views"], data["ad_clicks"], data["ad_orders"]
        data["ad_ctr"] = round((clicks / views) * 100, 2) if views else 0
        data["ad_cpc"] = round(data["ad_spend"] / clicks, 2) if clicks else 0
        data["ad_cr"] = round((orders / clicks) * 100, 2) if clicks else 0
    return aggregated

# === Сохранение в БД ===
def save_sales_to_db_for_date(sales_data, cards_info, ad_data, date):
    conn = sqlite3.connect("wildberries_cards.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            nm_ID INTEGER, date TEXT, imtName TEXT, total_profit REAL, ordersCount INTEGER,
            brand TEXT, subjectName TEXT, salePrice REAL, purchase_price REAL,
            delivery_to_warehouse REAL, wb_commission_rub REAL, wb_logistics REAL, tax_rub REAL,
            packaging REAL, fuel REAL, gift REAL, defect_percent REAL, cost_price REAL,
            profit_per_item REAL, commission_percent REAL, ad_views INTEGER, ad_clicks INTEGER,
            ad_ctr REAL, ad_cpc REAL, ad_spend REAL, ad_atbs INTEGER, ad_orders INTEGER,
            ad_cr REAL, ad_shks INTEGER, ad_sum_price REAL, quantity INTEGER,
            vendorCode TEXT, imtID INTEGER, openCardCount INTEGER, addToCartCount INTEGER,
            ordersSumRub INTEGER, buyoutsCount INTEGER
        )""")
    cursor.execute("SELECT nmID, brand, subjectName, salePrice, purchase_price, delivery_to_warehouse, wb_commission_rub, wb_logistics, tax_rub, packaging, fuel, gift, defect_percent, cost_price, profit_per_item, commission_percent FROM cards")
    card_details = {row[0]: dict(zip(["brand", "subjectName", "salePrice", "purchase_price", "delivery_to_warehouse", "wb_commission_rub", "wb_logistics", "tax_rub", "packaging", "fuel", "gift", "defect_percent", "cost_price", "profit_per_item", "commission_percent"], row[1:])) for row in cursor.fetchall()}
    for entry in sales_data:
        nmID = entry["nmID"]
        vendorCode = entry.get("vendorCode", "")
        imtID = entry.get("imtID", 0)
        imtName = entry.get("imtName", "")
        for record in entry["history"]:
            record_date = record["dt"]
            record["date"] = record_date
            quantity = record["ordersCount"]
            ad_spend = ad_data.get(nmID, {}).get("ad_spend", 0)
            profit_per_item = card_details.get(nmID, {}).get("profit_per_item", 0)
            total_profit = round((profit_per_item * quantity) - ad_spend, 2)
            merged = {
                **record, **ad_data.get(nmID, {}), **card_details.get(nmID, {}),
                "total_profit": total_profit, "vendorCode": vendorCode,
                "imtID": cards_info.get(nmID, {}).get("imtID", None),
                "imtName": imtName
            }
            ensure_columns_exist(conn, "sales", merged)
            cursor.execute("SELECT COUNT(*) FROM sales WHERE nm_ID = ? AND date = ?", (nmID, record_date))
            exists = cursor.fetchone()[0] > 0
            if exists:
                placeholders = ", ".join([f"{k} = ?" for k in merged])
                values = list(merged.values()) + [nmID, record_date]
                cursor.execute(f"UPDATE sales SET {placeholders} WHERE nm_ID = ? AND date = ?", values)
            else:
                columns = ["nm_ID", "date"] + list(merged.keys())
                values = [nmID, record_date] + list(merged.values())
                placeholders = ", ".join(["?"] * len(columns))
                cursor.execute(f"INSERT INTO sales ({', '.join(columns)}) VALUES ({placeholders})", values)
    conn.commit()
    conn.close()

# === Основная функция ===
async def run_data_collection_for_date(date: str):
    token = WB_API_KEY
    cards_info = await fetch_all_cards()
    nm_ids = list(cards_info.keys())
    batch_size = 20
    all_sales = []
    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i + batch_size]
        print(f"⏳ Запрос {i // batch_size + 1} из {len(nm_ids) // batch_size + 1} за {date}")
        sales_data = get_sales_data(batch, token, date)
        all_sales.extend(sales_data)
        await asyncio.sleep(20)
    ad_metrics = get_ad_metrics(date)
    save_sales_to_db_for_date(all_sales, cards_info, ad_metrics, date)
    print(f"✅ Обработка завершена за {date}")

# === Цикл по датам ===
async def run_full_history():
    start_date = datetime(2025, 1, 1)
    today = datetime.utcnow().date()
    while start_date.date() < today:
        date_str = start_date.strftime("%Y-%m-%d")
        await run_data_collection_for_date(date_str)
        start_date += timedelta(days=1)

# Запуск
if __name__ == "__main__":
    asyncio.run(run_full_history())
