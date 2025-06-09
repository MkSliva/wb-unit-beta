import sqlite3
import requests
import json
from datetime import datetime, timedelta

api_key = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDkyOTQ5MSwiaWQiOiIwMTk3NDIyNi0zY2VmLTdlMDctODgzNS1iMDlmNzdjMmJmZWMiLCJpaWQiOjIwMDEwODE3LCJvaWQiOjEzODY0NjcsInMiOjEwNzM3NTc5NTAsInNpZCI6IjE5MzhhN2NmLWE5YjMtNDhmOC1hYjUyLWVmMGMzMjU2OWJhYiIsInQiOmZhbHNlLCJ1aWQiOjIwMDEwODE3fQ._hZYYZ6vb3jDb_oAUMSs3nfy7CvgsMNUXy62eai2VSiwj1Q0zcdGoEVJK_nbNMsPAVpu8poWMKcyV8RP0V3x4Q"


# 📅 Получаем дату вчерашнего дня в формате YYYY-MM-DD
def get_yesterday_date():
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# 📥 Получение ID всех кампаний
def get_campaign_ids(api_key):
    url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDkyOTQ5MSwiaWQiOiIwMTk3NDIyNi0zY2VmLTdlMDctODgzNS1iMDlmNzdjMmJmZWMiLCJpaWQiOjIwMDEwODE3LCJvaWQiOjEzODY0NjcsInMiOjEwNzM3NTc5NTAsInNpZCI6IjE5MzhhN2NmLWE5YjMtNDhmOC1hYjUyLWVmMGMzMjU2OWJhYiIsInQiOmZhbHNlLCJ1aWQiOjIwMDEwODE3fQ._hZYYZ6vb3jDb_oAUMSs3nfy7CvgsMNUXy62eai2VSiwj1Q0zcdGoEVJK_nbNMsPAVpu8poWMKcyV8RP0V3x4Q"

    }

    response = requests.get(url, headers=headers)
    campaign_ids = []

    if response.status_code == 200:
        data = response.json()
        for advert_type in data.get("adverts", []):
            for campaign in advert_type.get("advert_list", []):
                campaign_ids.append(campaign["advertId"])
    else:
        print(f"❌ Ошибка при получении списка кампаний: {response.status_code} — {response.text}")

    return campaign_ids

# 📊 Получение статистики по каждой кампании за указанную дату
def fetch_advertising_data(api_key, campaign_ids, date):
    url = "https://advert-api.wildberries.ru/adv/v2/fullstats"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDkyOTQ5MSwiaWQiOiIwMTk3NDIyNi0zY2VmLTdlMDctODgzNS1iMDlmNzdjMmJmZWMiLCJpaWQiOjIwMDEwODE3LCJvaWQiOjEzODY0NjcsInMiOjEwNzM3NTc5NTAsInNpZCI6IjE5MzhhN2NmLWE5YjMtNDhmOC1hYjUyLWVmMGMzMjU2OWJhYiIsInQiOmZhbHNlLCJ1aWQiOjIwMDEwODE3fQ._hZYYZ6vb3jDb_oAUMSs3nfy7CvgsMNUXy62eai2VSiwj1Q0zcdGoEVJK_nbNMsPAVpu8poWMKcyV8RP0V3x4Q"

    }

    body = [{"id": cid, "dates": [date]} for cid in campaign_ids]
    response = requests.post(url, headers=headers, data=json.dumps(body))

    stats = []

    if response.status_code == 200:
        data = response.json()
        for campaign in data:
            for day in campaign.get("days", []):
                for app in day.get("apps", []):
                    for item in app.get("nm", []):
                        stats.append({
                            "date": day.get("date")[:10],  # отрезаем время
                            "name": item.get("name"),
                            "nm_id": item.get("nmId"),
                            "adv_spent": item.get("sum"),
                            "adv_clicks": item.get("clicks"),
                            "adv_views": item.get("views")
                        })
    else:
        print(f"❌ Ошибка запроса статистики: {response.status_code} — {response.text}")

    print(stats)

    return stats

# 💾 Обновление таблицы sales

db_path = "/backend/wildberries_cards.db"

def update_sales_table(ad_data, db_path):
    db_path = "/backend/wildberries_cards.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print(ad_data)
    for entry in ad_data:
        print(entry)
        print(f"📌 Обновляем nm_id {entry['nm_id']} на {entry['date']}")
        print(f"  📈 Расход: {entry['adv_spent']} ₽, Клики: {entry['adv_clicks']}, Показы: {entry['adv_views']}")


        cursor.execute("""
                       INSERT INTO sales (date,
                                          campaign_name,
                                          nm_id,
                                          vendor_code,
                                          imtID,
                                          quantity,
                                          revenue,
                                          adv_spent,
                                          adv_clicks,
                                          adv_views,
                                          profit,
                                          total_cost)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       """, (
                           entry["date"],  # 📅 Дата
                           entry.get("name"),
                           entry["nm_id"],  # 🆔 Артикул WB
                           entry.get("vendor_code"),  # 🔢 Артикул продавца (если есть)
                           entry.get("imtID"),  # 🧩 imtID (если есть)
                           entry.get("quantity", 0),  # 🧮 Кол-во продаж (по умолчанию 0)
                           entry.get("revenue", 0.0),  # 💸 Выручка (по умолчанию 0)
                           entry["adv_spent"],  # 💰 Расход на рекламу
                           entry["adv_clicks"],  # 🖱 Клики
                           entry["adv_views"],  # 👁 Показы
                           entry.get("profit", 0.0),  # 📈 Чистая прибыль (если есть)
                           entry.get("total_cost", 0.0)  # 💰 Себестоимость (если есть)
                       ))

    conn.commit()
    conn.close()
    print("✅ Обновление sales завершено.")

# 🚀 Основной запуск
if __name__ == "__main__":
    API_KEY = "your_token_here"  # 🔐 Укажи свой токен
    DB_PATH = "/mnt/data/8468ced7-426c-4a72-b5e4-9b8e8a6192fd.db"  # 🔧 Путь к БД
    date = get_yesterday_date()

    print("📥 Получаем ID рекламных кампаний...")
    campaign_ids = get_campaign_ids(API_KEY)

    if campaign_ids:
        print(f"📊 Получаем статистику за {date}...")
        adv_data = fetch_advertising_data(API_KEY, campaign_ids, date)

        print("💾 Обновляем таблицу sales...")
        update_sales_table(adv_data, DB_PATH)
    else:
        print("⚠️ Кампании не найдены.")


