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
        ad_views INTEGER DEFAULT 0,
        ad_clicks INTEGER DEFAULT 0,
        ad_ctr REAL DEFAULT 0,
        ad_cpc REAL DEFAULT 0,
        ad_spend REAL DEFAULT 0,
        ad_atbs INTEGER DEFAULT 0,
        ad_orders INTEGER DEFAULT 0,
        ad_cr REAL DEFAULT 0,
        ad_shks INTEGER DEFAULT 0,
        ad_sum_price REAL DEFAULT 0,
        PRIMARY KEY (date, nm_id)
    )
    """)

    aggregated = defaultdict(lambda: {
        "realsales": 0, "quantity": 0, "revenue": 0, "returns": 0,
        "opens": 0, "atc": 0, "buyouts": 0,
        "buyout_percent": 0.0, "add_to_cart_conv": 0.0, "cart_to_order_conv": 0.0,
        "vendor_code": "", "imt_id": 0, "count": 0
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
            group["opens"] += day.get("openCardCount", 0)
            group["atc"] += day.get("addToCartCount", 0)
            group["buyouts"] += day.get("buyoutsCount", 0)
            group["buyout_percent"] += day.get("buyoutPercent", 0.0)
            group["add_to_cart_conv"] += day.get("addToCartConversion", 0.0)
            group["cart_to_order_conv"] += day.get("cartToOrderConversion", 0.0)
            group["vendor_code"] = vendor_code
            group["imt_id"] = imt_id
            group["count"] += 1

    for (date, nm_id), d in aggregated.items():
        ad = ad_data.get(nm_id, {})
        count = d["count"] if d["count"] else 1

        cursor.execute("""
            INSERT INTO sales (
                date, nm_id, vendor_code, imt_id,
                realsales, quantity, revenue, returns,
                opens, atc, buyouts,
                buyout_percent, add_to_cart_conv, cart_to_order_conv,
                ad_views, ad_clicks, ad_ctr, ad_cpc, ad_spend,
                ad_atbs, ad_orders, ad_cr, ad_shks, ad_sum_price
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                cart_to_order_conv=excluded.cart_to_order_conv,
                ad_views=excluded.ad_views,
                ad_clicks=excluded.ad_clicks,
                ad_ctr=excluded.ad_ctr,
                ad_cpc=excluded.ad_cpc,
                ad_spend=excluded.ad_spend,
                ad_atbs=excluded.ad_atbs,
                ad_orders=excluded.ad_orders,
                ad_cr=excluded.ad_cr,
                ad_shks=excluded.ad_shks,
                ad_sum_price=excluded.ad_sum_price
        """, (
            date, nm_id, d["vendor_code"], d["imt_id"],
            d["realsales"], d["quantity"], d["revenue"], d["returns"],
            d["opens"], d["atc"], d["buyouts"],
            round(d["buyout_percent"] / count, 2),
            round(d["add_to_cart_conv"] / count, 2),
            round(d["cart_to_order_conv"] / count, 2),
            ad.get("ad_views", 0), ad.get("ad_clicks", 0), ad.get("ad_ctr", 0),
            ad.get("ad_cpc", 0), ad.get("ad_spend", 0),
            ad.get("ad_atbs", 0), ad.get("ad_orders", 0), ad.get("ad_cr", 0),
            ad.get("ad_shks", 0), ad.get("ad_sum_price", 0)
        ))

        print(f"✅ Обновлено: nmID {nm_id} на {date}")

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
        await asyncio.sleep(21)

    ad_metrics = get_ad_metrics()
    save_sales_to_db(all_sales, cards_info, ad_metrics)
    print("🎉 Завершено")


if __name__ == "__main__":
    asyncio.run(main())
