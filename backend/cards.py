import os
import httpx
from dotenv import load_dotenv
from hlam.db import init_db, save_cards_to_db
import asyncio
from hlam.update_prices import update_prices_get_method
from hlam.commission_import import fetch_commissions, update_commissions_in_db
from backend.importexcel import import_excel_if_missing

# Загружаем токен
load_dotenv("api.env")
WB_API_KEY = os.getenv("WB_API_KEY")

url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
headers = {
    "Authorization": WB_API_KEY,
    "Content-Type": "application/json"
}

async def fetch_and_save_cards():
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

    result = []
    for card in all_cards:
        result.append({
            "nmID": card.get("nmID"),
            "imtID": card.get("imtID"),
            "vendorCode": card.get("vendorCode"),
            "brand": card.get("brand"),
            "subjectName": card.get("subjectName"),
            "price": None  # цену позже обновим отдельно
        })

    save_cards_to_db(result)
    print(f"✅ Сохранено карточек: {len(result)}")

if __name__ == "__main__":
    init_db()
    asyncio.run(fetch_and_save_cards())  # Загружаем карточки
    asyncio.run(update_prices_get_method())         # Сразу обновляем цены
    print("\n🔄 Обновляем комиссии по категориям...")

    try:
        # Получаем словарь вида: {"Смартфоны": 15.0, "Наушники": 20.0, ...}
        commissions = fetch_commissions()

        # Обновляем комиссию в базе данных
        update_commissions_in_db(commissions)

        print(f"✅ Обновлено комиссий: {len(commissions)}")

    except Exception as e:
        print(f"❌ Ошибка при обновлении комиссий: {e}")

    import_excel_if_missing()








