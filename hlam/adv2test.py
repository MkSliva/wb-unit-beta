import requests
from datetime import datetime, timedelta
import json
from collections import defaultdict
import sqlite3

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDkyOTQ5MSwiaWQiOiIwMTk3NDIyNi0zY2VmLTdlMDctODgzNS1iMDlmNzdjMmJmZWMiLCJpaWQiOjIwMDEwODE3LCJvaWQiOjEzODY0NjcsInMiOjEwNzM3NTc5NTAsInNpZCI6IjE5MzhhN2NmLWE5YjMtNDhmOC1hYjUyLWVmMGMzMjU2OWJhYiIsInQiOmZhbHNlLCJ1aWQiOjIwMDEwODE3fQ._hZYYZ6vb3jDb_oAUMSs3nfy7CvgsMNUXy62eai2VSiwj1Q0zcdGoEVJK_nbNMsPAVpu8poWMKcyV8RP0V3x4Q"
  # 🔐 Твой ключ

yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def get_all_campaign_ids(api_key: str):
    url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    headers = {"Authorization": api_key}
    print("📡 Запрашиваем список всех рекламных кампаний...")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Ошибка при получении кампаний: {response.status_code} — {response.text}")
        return []
    try:
        data = response.json()
        campaign_ids = []
        for campaign_group in data.get("adverts", []):
            for advert in campaign_group.get("advert_list", []):
                campaign_id = advert.get("advertId")
                if campaign_id:
                    campaign_ids.append(campaign_id)
        print(f"✅ Найдено {len(campaign_ids)} кампаний.")
        return campaign_ids
    except Exception as e:
        print("❌ Ошибка при разборе ответа:", e)
        return []

campaign_ids = get_all_campaign_ids(API_KEY)
request_body = [{"id": cid, "dates": [yesterday]} for cid in campaign_ids]

headers = {
    "Content-Type": "application/json",
    "Authorization": API_KEY
}
url = "https://advert-api.wildberries.ru/adv/v2/fullstats"

print(f"📤 Отправляем запрос за дату: {yesterday}")
response = requests.post(url, headers=headers, data=json.dumps(request_body))

print(f"📡 Статус ответа: {response.status_code}")
if response.status_code != 200:
    print("❌ Ошибка запроса:", response.text)
    exit()

data = response.json()
if not data or not isinstance(data, list):
    print("⚠️ Нет данных в ответе.")
    exit()

print("🔍 Обработка полученной статистики...\n")

aggregated = defaultdict(lambda: {
    "views": 0, "clicks": 0, "ctr": 0, "cpc": 0, "sum": 0,
    "atbs": 0, "orders": 0, "cr": 0, "shks": 0, "sum_price": 0,
    "name": "", "campaign_id": 0, "date": ""
})

for campaign in data:
    campaign_id = campaign.get("advertId")
    for day in campaign.get("days", []):
        date_str = day.get("date")
        for app in day.get("apps", []):
            for item in app.get("nm", []):
                nm_id = item.get("nmId")
                group = aggregated[nm_id]
                group["views"] += item.get("views", 0)
                group["clicks"] += item.get("clicks", 0)
                group["sum"] += item.get("sum", 0)
                group["atbs"] += item.get("atbs", 0)
                group["orders"] += item.get("orders", 0)
                group["shks"] += item.get("shks", 0)
                group["sum_price"] += item.get("sum_price", 0)
                group["name"] = item.get("name", "")
                group["campaign_id"] = campaign_id
                group["date"] = date_str

# 📥 Подключение к базе данных
conn = sqlite3.connect("../backend/wildberries_cards.db")
cursor = conn.cursor()

# 🛠 Создание таблицы, если её нет
cursor.execute("""
CREATE TABLE IF NOT EXISTS sales (
    date TEXT,
    campaign_id INTEGER,
    nm_id INTEGER,
    name TEXT,
    views INTEGER,
    clicks INTEGER,
    ctr REAL,
    cpc REAL,
    sum REAL,
    atbs INTEGER,
    orders INTEGER,
    cr REAL,
    shks INTEGER,
    sum_price REAL,
    PRIMARY KEY (date, nm_id)
)
""")

# 💾 Обновление или вставка данных
for nm_id, d in sorted(aggregated.items()):
    views = d["views"]
    clicks = d["clicks"]
    orders = d["orders"]

    d["ctr"] = round((clicks / views) * 100, 2) if views else 0
    d["cpc"] = round(d["sum"] / clicks, 2) if clicks else 0
    d["cr"] = round((orders / clicks) * 100, 2) if clicks else 0

    cursor.execute("""
    INSERT INTO sales (date, campaign_id, nm_id, name, views, clicks, ctr, cpc, sum, atbs, orders, cr, shks, sum_price)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date, nm_id) DO UPDATE SET
        campaign_id=excluded.campaign_id,
        name=excluded.name,
        views=excluded.views,
        clicks=excluded.clicks,
        ctr=excluded.ctr,
        cpc=excluded.cpc,
        sum=excluded.sum,
        atbs=excluded.atbs,
        orders=excluded.orders,
        cr=excluded.cr,
        shks=excluded.shks,
        sum_price=excluded.sum_price
    """, (
        d["date"], d["campaign_id"], nm_id, d["name"], d["views"], d["clicks"],
        d["ctr"], d["cpc"], d["sum"], d["atbs"], d["orders"], d["cr"],
        d["shks"], d["sum_price"]
    ))

    print(f"✅ Обновлены данные для nmID {nm_id} на {d['date']}")

conn.commit()
conn.close()
print("📁 Все данные успешно сохранены в БД.")

