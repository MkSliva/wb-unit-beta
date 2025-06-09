import os
import httpx
import sqlite3
import asyncio
import time
from dotenv import load_dotenv

# 📌 Загружаем API-ключ из файла .env
load_dotenv("../backend/api.env")
WB_API_KEY = os.getenv("WB_API_KEY")

# 🔧 Максимальное количество товаров за один запрос
LIMIT = 1000

# 🔄 Основная асинхронная функция обновления цен
async def update_prices_get_method():
    # Подключение к базе данных
    conn = sqlite3.connect("../backend/wildberries_cards.db")
    cursor = conn.cursor()

    offset = 0
    base_url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
    headers = {"Authorization": WB_API_KEY}

    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            params = {"limit": LIMIT, "offset": offset}
            try:
                print(f"🔄 Получаем товары с offset={offset}...")

                response = await client.get(base_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()


                goods = data.get("data", {}).get("listGoods", [])

                if not goods:
                    print("✅ Все товары обработаны.")
                    break

                # Обновление каждой карточки в базе
                for item in goods:

                    nm_id = item.get("nmID")

                    sizes = item.get("sizes", [])
                    if sizes:
                        price = sizes[0].get("price")
                        sale_price = sizes[0].get("discountedPrice")
                    else:
                        price = None
                        sale_price = None


                    cursor.execute("""
                        UPDATE cards
                        SET price = ?, salePrice = ?
                        WHERE nmID = ?
                    """, (price, sale_price, nm_id))

                conn.commit()
                offset += LIMIT
                time.sleep(0.6)  # ограничение частоты запросов

            except Exception as e:
                print(f"❌ Ошибка при запросе: {e}")
                break

    conn.close()
    print("🎯 Обновление завершено.")

# 🔽 Запуск функции при старте скрипта
if __name__ == "__main__":
    asyncio.run(update_prices_get_method())




