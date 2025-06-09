import requests
from datetime import datetime, timedelta
import json
from collections import defaultdict

# 🔐 Укажи здесь свой API-ключ Wildberries
API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDkyOTQ5MSwiaWQiOiIwMTk3NDIyNi0zY2VmLTdlMDctODgzNS1iMDlmNzdjMmJmZWMiLCJpaWQiOjIwMDEwODE3LCJvaWQiOjEzODY0NjcsInMiOjEwNzM3NTc5NTAsInNpZCI6IjE5MzhhN2NmLWE5YjMtNDhmOC1hYjUyLWVmMGMzMjU2OWJhYiIsInQiOmZhbHNlLCJ1aWQiOjIwMDEwODE3fQ._hZYYZ6vb3jDb_oAUMSs3nfy7CvgsMNUXy62eai2VSiwj1Q0zcdGoEVJK_nbNMsPAVpu8poWMKcyV8RP0V3x4Q"

# 📅 Получаем дату вчерашнего дня в формате YYYY-MM-DD
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def get_all_campaign_ids(api_key: str):
    """Получает список всех рекламных кампаний"""
    url = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
    headers = {
        "Authorization": api_key
    }

    print("📡 Запрашиваем список всех рекламных кампаний...")
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Ошибка при получении кампаний: {response.status_code} — {response.text}")
        return []

    try:
        data = response.json()
        campaign_ids = []

        # Обрабатываем список всех кампаний
        for campaign_group in data.get("adverts", []):
            advert_list = campaign_group.get("advert_list", [])
            for advert in advert_list:
                campaign_id = advert.get("advertId")
                if campaign_id:
                    campaign_ids.append(campaign_id)

        print(f"✅ Найдено {len(campaign_ids)} кампаний.")
        return campaign_ids

    except Exception as e:
        print("❌ Ошибка при разборе ответа:", e)
        return []


# 📦 Тело запроса в формате Wildberries (список кампаний)
# Замените ID кампаний на свои реальные campaignId
campaign_ids = get_all_campaign_ids(API_KEY)

# Строим request_body автоматически
request_body = [
    {
        "id": cid,
        "dates": [yesterday]
    }
    for cid in campaign_ids
]


# 🧾 Заголовки запроса с авторизацией
headers = {
    "Content-Type": "application/json",
    "Authorization": API_KEY
}

# 🌐 URL API Wildberries для получения рекламной статистики
url = "https://advert-api.wildberries.ru/adv/v2/fullstats"

# 📤 Отправляем POST-запрос
print(f"📤 Отправляем запрос за дату: {yesterday}")
response = requests.post(url, headers=headers, data=json.dumps(request_body))

# 🧾 Статус и проверка ответа
print(f"📡 Статус ответа: {response.status_code}")
if response.status_code != 200:
    print("❌ Ошибка запроса:", response.text)
    exit()

# 📦 Преобразуем JSON-ответ в Python-объект
data = response.json()

# 🛑 Проверка наличия данных
if not data or not isinstance(data, list):
    print("⚠️ Нет данных в ответе.")
    exit()

# 🔍 Разбор ответа по кампаниям, дням, приложениям и товарам
print("🔍 Обработка полученной статистики...\n")
print("🔍 Агрегируем данные по nmID...\n")

# 📊 Группируем по nmID
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

                # Объединение метрик
                group["views"] += item.get("views", 0)
                group["clicks"] += item.get("clicks", 0)
                group["sum"] += item.get("sum", 0)
                group["atbs"] += item.get("atbs", 0)
                group["orders"] += item.get("orders", 0)
                group["shks"] += item.get("shks", 0)
                group["sum_price"] += item.get("sum_price", 0)

                # Для однородности
                group["name"] = item.get("name", "")
                group["campaign_id"] = campaign_id
                group["date"] = date_str

# 🎯 Перерасчёт CTR, CPC, CR
for nm_id, data in sorted(aggregated.items()):
    views = data["views"]
    clicks = data["clicks"]
    orders = data["orders"]

    data["ctr"] = round((clicks / views) * 100, 2) if views else 0
    data["cpc"] = round(data["sum"] / clicks, 2) if clicks else 0
    data["cr"] = round((orders / clicks) * 100, 2) if clicks else 0

    # 📤 Вывод
    print(f"📦 Товар:")
    print(f"  📅 Дата: {data['date']}")
    print(f"  📢 Кампания ID: {data['campaign_id']}")
    print(f"  🆔 Артикул (nmID): {nm_id}")
    print(f"  🏷 Название: {data['name']}")
    print(f"  👁 Показы: {views}")
    print(f"  🖱 Клики: {clicks}")
    print(f"  🔁 CTR: {data['ctr']}%")
    print(f"  💰 CPC: {data['cpc']} ₽")
    print(f"  💸 Затраты: {data['sum']} ₽")
    print(f"  🛒 В корзину: {data['atbs']}")
    print(f"  📦 Заказы: {orders}")
    print(f"  🎯 CR: {data['cr']}%")
    print(f"  📤 Отгрузки: {data['shks']}")
    print(f"  📈 Выручка: {data['sum_price']} ₽")
    print("────────────────────────────")





