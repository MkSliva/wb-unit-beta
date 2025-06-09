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
import json

# 🔐 Загрузка токена
load_dotenv("../backend/api.env")
WB_API_KEY = os.getenv("WB_API_KEY")

# 🕛 Даты для выборки
yesterday = (datetime.utcnow() - timedelta(days=1)).date().isoformat()


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
def get_sales_data(nm_ids: list, token: str):
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    headers = {"Authorization": token}

    payload = {
        "nmIDs": nm_ids,
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
                    nm_id = item.get("nmId")
                    group = aggregated[nm_id]
                    group["ad_views"] += item.get("views", 0)
                    group["ad_clicks"] += item.get("clicks", 0)
                    group["ad_spend"] += item.get("sum", 0)
                    group["ad_atbs"] += item.get("atbs", 0)
                    group["ad_orders"] += item.get("orders", 0)
                    group["ad_shks"] += item.get("shks", 0)
                    group["ad_sum_price"] += item.get("sum_price", 0)

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

    # 🧱 Убедимся, что все нужные столбцы существуют
    all_columns = [
        "openCardCount", "addToCartCount", "ordersCount", "ordersSumRub", "buyoutsCount", "buyoutsSumRub",
        "buyoutPercent", "addToCartConversion", "cartToOrderConversion",
        "ad_views", "ad_clicks", "ad_ctr", "ad_cpc", "ad_spend",
        "ad_atbs", "ad_orders", "ad_cr", "ad_shks", "ad_sum_price",
        "brand", "subjectName", "salePrice", "purchase_price", "delivery_to_warehouse", "wb_commission_rub",
        "wb_logistics", "tax_rub", "packaging", "fuel", "gift", "defect_percent", "cost_price",
        "profit_per_item", "commission_percent"
    ]

    for column in all_columns:
        try:
            cursor.execute(f"ALTER TABLE sales ADD COLUMN {column} REAL")
        except Exception:
            # Столбец уже существует
            pass


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
        nm_id = entry["nmID"]
        for record in entry["history"]:
            date = record["dt"]
            cursor.execute(
                "SELECT COUNT(*) FROM sales WHERE nm_id = %s AND date = %s",
                (nm_id, date),
            )
            exists = cursor.fetchone()[0] > 0

            record["date"] = record.pop("dt")

            merged = {**record, **ad_data.get(nm_id, {}), **card_details.get(nm_id, {})}

            if exists:
                placeholders = ", ".join([f"{k} = %s" for k in merged])
                values = list(merged.values()) + [nm_id, date]
                cursor.execute(
                    f"UPDATE sales SET {placeholders} WHERE nm_ID = %s AND date = %s",
                    values,
                )
            else:
                columns = ["nm_id", "date"] + list(merged.keys())
                values = [nm_id, date] + list(merged.values())
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


if __name__ == "__main__":
    asyncio.run(main())


