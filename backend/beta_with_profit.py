import os
import math
import httpx
import asyncio
import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, List, Dict  # Добавлены для полной типизации

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv

# 🔐 Загрузка токена
load_dotenv("api.env") # Убедитесь, что api.env находится в том же каталоге или указан правильный путь
WB_API_KEY = os.getenv("WB_API_KEY")
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

tax_percent = 12 # Процент налога, можно сделать его динамическим, если потребуется

# Значение процента брака, используемое для новых записей в sales
peremennaya_real_defect_percent = 2

# 🕛 Даты для выборки
glebas = 1 # Количество дней назад для yesterday
yesterday = (datetime.utcnow() - timedelta(glebas)).date().isoformat()
print(yesterday)

# Заголовки для запросов к API Wildberries
# Используем WB_API_KEY, если у вас есть отдельный "стандартный" ключ,
# можно создать отдельные заголовки, например headers_standard
headers = {
    "Authorization": WB_API_KEY
}

# --- НОВАЯ ФУНКЦИЯ: Получение комиссий с Wildberries API ---
def fetch_commissions():
    print("🔄 Получаем комиссии с Wildberries...")
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"

    try:
        response = requests.get(url, headers=headers)
        # print("Ответ сервера:", response.text) # Можно раскомментировать для отладки, но обычно очень многословно
        response.raise_for_status() # Вызывает исключение для статусов 4xx/5xx

        raw_data = response.json()
        if not isinstance(raw_data, dict):
            raise Exception(f"❌ Ожидался dict, но пришло: {type(raw_data)}")

        data = raw_data.get("report", [])
        if not isinstance(data, list):
            raise Exception(f"❌ Ожидался список, но пришло: {type(data)}")

        commissions = {
            item["subjectName"].strip().lower(): item["kgvpSupplier"] # WB возвращает предмет в subjectName, а комиссию в kgvpSupplier
            for item in data if "subjectName" in item and "kgvpSupplier" in item
        }

        print(f"✅ Получено комиссий: {len(commissions)}")
        return commissions

    except requests.exceptions.RequestException as e: # Более специфичное исключение для ошибок requests
        print(f"❌ Ошибка HTTP при получении комиссий: {e}")
        return {}
    except Exception as e:
        print(f"❌ Непредвиденная ошибка при получении комиссий: {e}")
        return {}

# --- ГЛОБАЛЬНАЯ ПЕРЕМЕННАЯ ДЛЯ ХРАНЕНИЯ КОМИССИЙ ---
COMMISSIONS_DATA: Dict[str, float] = {} # Будет заполнена при запуске

# --- Функция, запускаемая при старте скрипта, для инициализации данных ---
async def startup_tasks():
    global COMMISSIONS_DATA
    COMMISSIONS_DATA = fetch_commissions()
    print(f"Комиссии загружены при старте: {len(COMMISSIONS_DATA)}")

# === 1. Получение карточек с Content API ===
async def fetch_all_cards():
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers = {
        "Authorization": WB_API_KEY,
        "Content-Type": "application/json"
    }

    limit = 100
    cursor = {}
    all_cards = {} # nmID -> card_info

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
                        "subjectName": card.get("subjectName") # subjectName нужен для комиссии
                    }

            if total < limit:
                break

            cursor = {
                "updatedAt": cursor_data.get("updatedAt"),
                "nmID": cursor_data.get("nmID")
            }

    print(f"✅ Получено карточек: {len(all_cards)}")
    return all_cards


def get_all_discounted_prices(api_key: str) -> dict:
    url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
    headers = {
        "Authorization": WB_API_KEY
    }

    limit = 1000
    offset = 0
    result = {} # nmID -> discountedPrice

    while True:
        params = {
            "limit": limit,
            "offset": offset
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            # print(response) # Закомментируйте для уменьшения шума
            response.raise_for_status()
            goods = response.json().get("data", {}).get("listGoods", [])
        except Exception as e:
            print(f"❌ Ошибка при получении товаров: {e}")
            break

        if not goods:
            break

        for item in goods:
            nm_id = item.get("nmID")
            sizes = item.get("sizes", [])
            if sizes and isinstance(sizes, list):
                discounted_price = sizes[0].get("discountedPrice", 0)
                result[nm_id] = discounted_price

        offset += limit

    print(f"✅ Получено цен: {len(result)}")
    return result


# === 2. Получение метрик продаж ===
def get_sales_data(nmIDs: list, token: str):
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"
    headers = {"Authorization": WB_API_KEY}

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
def save_sales_to_db(sales_data: list, cards_info: dict, ad_data: dict, actual_prices: dict):
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Убедимся, что таблица 'sales' существует с базовыми столбцами
    # Добавили основные числовые поля с BIGINT/REAL для надежности
    # ВАЖНО: subjectName здесь, потому что он нужен для расчета комиссии
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS sales
                   (
                       "nm_ID"
                       BIGINT,
                       "date"
                       DATE,
                       "openCardCount"
                       BIGINT,
                       "addToCartCount"
                       BIGINT,
                       "ordersCount"
                       BIGINT,
                       "ordersSumRub"
                       REAL,
                       "buyoutsCount"
                       BIGINT,
                       "buyoutsSumRub"
                       REAL,
                       "buyoutPercent"
                       REAL,
                       "addToCartConversion"
                       REAL,
                       "cartToOrderConversion"
                       REAL,
                       "ad_views"
                       BIGINT,
                       "ad_clicks"
                       BIGINT,
                       "ad_ctr"
                       REAL,
                       "ad_cpc"
                       REAL,
                       "ad_spend"
                       REAL,
                       "ad_atbs"
                       BIGINT,
                       "ad_orders"
                       BIGINT,
                       "ad_cr"
                       REAL,
                       "ad_shks"
                       BIGINT,
                       "ad_sum_price"
                       REAL,
                       "brand"
                       TEXT,
                       "subjectName"
                       TEXT, -- ОБЯЗАТЕЛЬНО УБЕДИТЕСЬ, ЧТО ЭТОТ СТОЛБЕЦ СУЩЕСТВУЕТ
                       "salePrice"
                       REAL,
                       "purchase_price"
                       REAL,
                       "delivery_to_warehouse"
                       REAL,
                       "wb_commission_rub"
                       REAL, -- ТЕПЕРЬ РАССЧИТЫВАЕТСЯ ДИНАМИЧЕСКИ
                       "wb_logistics"
                       REAL,
                       "tax_rub"
                       REAL, -- ТЕПЕРЬ РАССЧИТЫВАЕТСЯ ДИНАМИЧЕСКИ
                       "packaging"
                       REAL,
                       "fuel"
                       REAL,
                       "gift"
                       REAL,
                       "real_defect_percent"
                       REAL,
                       "ad_manager_name"
                       TEXT,
                       "defect_percent"
                       REAL,
                       "cost_price"
                       REAL, -- ТЕПЕРЬ РАССЧИТЫВАЕТСЯ ДИНАМИЧЕСКИ
                       "profit_per_item"
                       REAL, -- ТЕПЕРЬ РАССЧИТЫВАЕТСЯ ДИНАМИЧЕСКИ
                       "commission_percent"
                       REAL, -- ТЕПЕРЬ ХРАНИТ ПРОЦЕНТ WB
                       "total_profit"
                       REAL,
                       "vendorCode"
                       TEXT,
                       "imtID"
                       BIGINT,
                       "imtName"
                       TEXT,
                       "actual_discounted_price"
                       REAL,
                       PRIMARY
                       KEY
                   (
                       "nm_ID",
                       "date"
                   )
                       );
                   """)
    conn.commit()

    # Ensure new columns exist and have default values
    cursor.execute('ALTER TABLE sales ADD COLUMN IF NOT EXISTS "real_defect_percent" REAL')
    cursor.execute('ALTER TABLE sales ADD COLUMN IF NOT EXISTS "ad_manager_name" TEXT')
    cursor.execute('ALTER TABLE sales ADD COLUMN IF NOT EXISTS "card_changes" TEXT')
    conn.commit()
    cursor.execute('UPDATE sales SET "real_defect_percent" = 2 WHERE "real_defect_percent" IS NULL')
    cursor.execute("UPDATE sales SET \"ad_manager_name\" = '0' WHERE \"ad_manager_name\" IS NULL")
    cursor.execute("UPDATE sales SET \"card_changes\" = '0' WHERE \"card_changes\" IS NULL")
    conn.commit()

    # 📦 Получение справочной информации из самой свежей записи таблицы sales
    cursor.execute(
        """
        SELECT DISTINCT ON ("nm_ID") "nm_ID", "imtID", brand, "subjectName", purchase_price,
               delivery_to_warehouse, wb_logistics, packaging, fuel, gift,
               real_defect_percent, ad_manager_name, card_changes
        FROM sales
        ORDER BY "nm_ID", "date" DESC
        """
    )
    card_details_raw = cursor.fetchall()
    card_details = {
        row[0]: {
            "imtID": row[1],
            "brand": row[2] or "",
            "subjectName": row[3] or "",  # subjectName теперь нужен для комиссии
            "purchase_price": row[4] or 0,
            "delivery_to_warehouse": row[5] or 0,
            "wb_logistics": row[6] or 0,
            "packaging": row[7] or 0,
            "fuel": row[8] or 0,
            "gift": row[9] or 0,
            "real_defect_percent": row[10] or peremennaya_real_defect_percent,
            "ad_manager_name": row[11] or '0',
            "card_changes": row[12] or '0',
            # 'cost_price', 'profit_per_item', 'wb_commission_rub', 'tax_rub', 'commission_percent'
            # теперь будут рассчитаны
        } for row in card_details_raw
    }

    cursor.execute(
        """
        SELECT DISTINCT ON ("imtID") "imtID", ad_manager_name, card_changes
        FROM sales
        ORDER BY "imtID", "date" DESC
        """
    )
    imt_details_raw = cursor.fetchall()
    imt_fallback = {
        row[0]: {
            "ad_manager_name": row[1] or '0',
            "card_changes": row[2] or '0'
        }
        for row in imt_details_raw
    }

    for entry in sales_data:
        vendorCode = entry.get("vendorCode", "")
        imtID = entry.get("imtID", 0)

        nmID = entry["nmID"]
        imtName = entry["imtName"]
        for record in entry["history"]:
            date_value = record["dt"] # Получаем значение из 'dt'

            # --- ИСПРАВЛЕНИЕ ОШИБКИ AttributeError: 'str' object has no attribute 'date' ---
            if isinstance(date_value, str):
                try:
                    # Попробуем распарсить дату из строки.
                    # Формат WB API может быть 'YYYY-MM-DDTHH:MM:SSZ' или 'YYYY-MM-DD'
                    if 'T' in date_value: # Если строка содержит время, как '2025-06-09T00:00:00Z'
                        # Заменяем 'Z' на '+00:00' для fromisoformat
                        date_obj = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                    else: # Если просто '2025-06-09'
                        date_obj = datetime.strptime(date_value, '%Y-%m-%d')
                except ValueError:
                    print(f"❌ Ошибка парсинга даты: '{date_value}'. Используем текущую дату UTC.")
                    date_obj = datetime.utcnow() # Fallback на текущую дату UTC
            elif isinstance(date_value, datetime):
                date_obj = date_value # Если уже datetime объект, используем его как есть
            else:
                print(f"❌ Неожиданный тип даты: {type(date_value)}. Значение: '{date_value}'. Используем текущую дату UTC.")
                date_obj = datetime.utcnow() # Fallback для других неожиданных типов
            # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

            record["date"] = date_obj.date().isoformat() # Теперь 'date' это строка 'YYYY-MM-DD'
            # record.pop("dt") # Если вам больше не нужен оригинальный ключ 'dt', можете удалить его (опционально)

            date_for_db = record["date"] # Используем уже преобразованную дату для БД

            quantity = record.get("ordersCount", 0) # Используем .get() для безопасности

            ad_spend = ad_data.get(nmID, {}).get("ad_spend", 0)
            actual_price = actual_prices.get(nmID, 0) # Это actual_discounted_price


            # --- НОВЫЙ РАСЧЕТ: wb_commission_rub и tax_rub ---
            commission_percent_for_item = 0
            calculated_wb_commission_rub = 0
            calculated_tax_rub = 0

            # Получаем subjectName для текущего nmID из cards_info (полученного из Content API)
            # или из card_details (полученного из БД 'cards')
            subject_name_from_info = cards_info.get(nmID, {}).get("subjectName") or card_details.get(nmID, {}).get("subjectName")

            if actual_price > 0 and subject_name_from_info:
                subject_name_lower = subject_name_from_info.strip().lower()
                if subject_name_lower in COMMISSIONS_DATA:
                    commission_percent_for_item = COMMISSIONS_DATA[subject_name_lower]
                    # Формула wb_commission_rub: actual_discounted_price / 100 * comissiom_percent + 1.1
                    calculated_wb_commission_rub = actual_price / 100 * (commission_percent_for_item + 1.1)
                # Формула tax_rub: actual_discounted_price / 100 * tax_percent
                calculated_tax_rub = (actual_price / 100 * tax_percent)
            # --- КОНЕЦ НОВОГО РАСЧЕТА ---


            # === Пересчет cost_price ===
            purchase_price = card_details.get(nmID, {}).get("purchase_price", 0)
            delivery_to_warehouse = card_details.get(nmID, {}).get("delivery_to_warehouse", 0)
            wb_logistics = card_details.get(nmID, {}).get("wb_logistics", 0)
            packaging = card_details.get(nmID, {}).get("packaging", 0)
            fuel = card_details.get(nmID, {}).get("fuel", 0)
            gift = card_details.get(nmID, {}).get("gift", 0)
            real_defect_percent = card_details.get(nmID, {}).get(
                "real_defect_percent", peremennaya_real_defect_percent
            )
            ad_manager_name = card_details.get(nmID, {}).get("ad_manager_name", '0')
            if ad_manager_name in [None, '', '0']:
                fallback = imt_fallback.get(imtID, {}).get("ad_manager_name")
                if fallback not in [None, '', '0']:
                    ad_manager_name = fallback
            card_changes_val = card_details.get(nmID, {}).get("card_changes", '0')
            if card_changes_val in [None, '', '0']:
                cc_fallback = imt_fallback.get(imtID, {}).get("card_changes")
                if cc_fallback not in [None, '', '0']:
                    card_changes_val = cc_fallback
            defect_percent = actual_price / 100 * real_defect_percent

            # Формула себестоимости с учетом новых рассчитанных значений
            calculated_cost_price = (
                purchase_price
                + delivery_to_warehouse
                + calculated_wb_commission_rub
                + wb_logistics
                + calculated_tax_rub
                + packaging
                + fuel
                + gift
                + defect_percent
            )


            # === Пересчет profit_per_item и total_profit ===
            calculated_profit_per_item = actual_price - calculated_cost_price
            calculated_total_profit = round((calculated_profit_per_item * quantity) - ad_spend, 2)


            # Формируем словарь для базы данных
            merged = {
                **record,
                **ad_data.get(nmID, {}),
                "total_profit": calculated_total_profit, # Используем пересчитанную
                "vendorCode": vendorCode,
                "imtID": cards_info.get(nmID, {}).get("imtID", None),
                "imtName": imtName,
                "actual_discounted_price": actual_price,
                # Добавляем рассчитанные значения
                "wb_commission_rub": calculated_wb_commission_rub,
                "tax_rub": calculated_tax_rub,
                "commission_percent": commission_percent_for_item, # Сохраняем процент комиссии WB
                "cost_price": calculated_cost_price, # Сохраняем пересчитанную себестоимость
                "profit_per_item": calculated_profit_per_item, # Сохраняем пересчитанную прибыль за единицу

                # Остальные поля из cards_details, которые нужны и не пересчитываются
                "brand": card_details.get(nmID, {}).get("brand", ""),
                "subjectName": subject_name_from_info, # Сохраняем subjectName, который получили
                "purchase_price": purchase_price,
                "delivery_to_warehouse": delivery_to_warehouse,
                "wb_logistics": wb_logistics,
                "packaging": packaging,
                "fuel": fuel,
                "gift": gift,
                "real_defect_percent": real_defect_percent,
                "defect_percent": defect_percent,
                "ad_manager_name": ad_manager_name,
                "card_changes": card_changes_val,
            }

            merged["nm_ID"] = nmID # nm_ID уже есть в record, но явно добавляем для ясности
            # merged["date"] уже установлено выше

            # --- Дополнительная обработка NaN и других проблемных значений перед ensure_columns_exist и сохранением ---
            for key, value in merged.items():
                if isinstance(value, float) and math.isnan(value):
                    merged[key] = 0 # Преобразуем NaN в 0

            # print(merged) # Закомментировал для уменьшения шума

            # Передаем merged для проверки и создания столбцов
            ensure_columns_exist(conn, "sales", merged)

            cursor.execute(
                "SELECT COUNT(*) FROM sales WHERE \"nm_ID\" = %s AND \"date\" = %s",
                (nmID, date_for_db),
            )
            exists = cursor.fetchone()[0] > 0

            # Формируем списки колонок и значений для вставки/обновления
            data_for_db_values = merged.copy() # Создаем копию, чтобы не менять оригинальный merged

            if "nm_ID" in data_for_db_values:
                del data_for_db_values["nm_ID"]
            if "date" in data_for_db_values:
                del data_for_db_values["date"]

            columns_list = list(data_for_db_values.keys())
            values_list = list(data_for_db_values.values())

            # --- ДОБАВЛЕННЫЕ ОТЛАДОЧНЫЕ ПРИНТЫ (для проверки) ---
            print("\n--- ОТЛАДКА: Данные для SQL-запроса ---")
            print(f"Обработка nm_ID: {nmID}, date: {date_for_db}")
            print(f"Статус: {'Обновление' if exists else 'Вставка новой записи'}")
            print("Список столбцов (без nm_ID и date):")
            print(columns_list)
            print("Список значений (без nm_ID и date):")
            for i, val in enumerate(values_list):
                col_name = columns_list[i]
                print(f"  {col_name}: {val} (Тип: {type(val)})")
                if isinstance(val, int) and (val > 2147483647 or val < -2147483648):
                    print(f"  !!! ПРЕДУПРЕЖДЕНИЕ: Возможное превышение INTEGER для '{col_name}' !!!")
                elif isinstance(val, float) and (val > 1.0e300 or val < -1.0e300):
                    print(f"  !!! ПРЕДУПРЕЖДЕНИЕ: Возможное очень большое/малое REAL для '{col_name}' !!!")
                elif val == 0 and col_name in ["gift", "defect_percent"]: # Специальное уведомление для преобразованного
                    print(f"  ИНФО: Значение '{col_name}' было NaN/None и преобразовано в 0.")

            print("--- КОНЕЦ ОТЛАДКИ ---\n")
            # --- КОНЕЦ ДОБАВЛЕННЫХ ОТЛАДОЧНЫХ ПРИНТОВ ---

            if exists:
                # Если запись за эту дату уже существует — обновляем её
                placeholders = ", ".join([f'"{k}" = %s' for k in columns_list])
                values_to_execute = values_list + [nmID, date_for_db] # Добавляем nmID и date для WHERE
                cursor.execute(
                    f'UPDATE sales SET {placeholders} WHERE "nm_ID" = %s AND "date" = %s',
                    values_to_execute,
                )

            else:
                # Если записи нет — вставляем новую строку
                columns = ["nm_ID", "date"] + columns_list
                values = [nmID, date_for_db] + values_list
                placeholders = ", ".join(["%s"] * len(columns))
                quoted_columns = ', '.join(f'"{col}"' for col in columns)

                cursor.execute(
                    f"INSERT INTO sales ({quoted_columns}) VALUES ({placeholders})",
                    values,
                )

    conn.commit()
    conn.close()


# === 5. Основной скрипт ===
async def main():
    await startup_tasks() # Вызываем для загрузки комиссий при старте
    token = WB_API_KEY
    cards_info = await fetch_all_cards() # Получаем subjectName здесь
    actual_prices = get_all_discounted_prices(WB_API_KEY)
    nm_ids = list(cards_info.keys())

    batch_size = 20
    all_sales = []

    # Уменьшил количество итераций для быстрого тестирования,
    # для продакшена верните len(nm_ids)
    for i in range(0, (len(nm_ids)), batch_size): # Ограничил до первых 20 nm_ids для теста
        batch = nm_ids[i:i + batch_size]
        print(f"⏳ Запрос {i // batch_size + 1} из {len(nm_ids) // batch_size + 1}")
        sales_data = get_sales_data(batch, token)
        all_sales.extend(sales_data)
        await asyncio.sleep(20) # Уменьшил задержку для теста

    ad_metrics = get_ad_metrics()
    save_sales_to_db(all_sales, cards_info, ad_metrics, actual_prices)
    print("🎉 Завершено")


def calculate_total_profit_for_day():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Получаем все строки за вчерашний день
    yesterday_str = (datetime.utcnow() - timedelta(glebas)).date().isoformat()
    cursor.execute(
        "SELECT total_profit FROM sales WHERE \"date\" = %s", # Используем кавычки для "date"
        (yesterday_str,),
    )
    rows = cursor.fetchall()

    # Суммируем profit
    total = sum(row[0] for row in rows if row[0] is not None)

    print(f"💰 Общая прибыль за {yesterday_str}: {total:.2f} ₽")
    conn.close()


def export_sales_to_excel():
    # Подключение к БД
    conn = psycopg2.connect(DB_URL)

    # Получаем дату вчерашнего дня
    yesterday_str = (datetime.utcnow() - timedelta(glebas)).date().isoformat()

    # Запрос всех строк за вчера
    query = "SELECT * FROM sales WHERE \"date\" = %s" # Используем кавычки для "date"
    df = pd.read_sql_query(query, conn, params=(yesterday_str,))

    # Сохраняем в Excel
    filename = f"sales_export_{yesterday_str}.xlsx"
    df.to_excel(filename, index=False)
    print(f"✅ Данные успешно экспортированы в {filename}")

    conn.close()


def ensure_columns_exist(conn, table_name, data_dict):
    cursor = conn.cursor()

    # Получение уже существующих столбцов
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table_name,),
    )
    existing_columns = [row[0] for row in cursor.fetchall()]

    for key, value in data_dict.items():
        # Пропускаем nm_ID и date, так как они должны быть созданы в PRIMARY KEY при создании таблицы
        if key == "nm_ID" or key == "date":
            continue

        if key not in existing_columns:
            print(f"🛠 Добавляем столбец: {key}")

            column_type = "TEXT" # Тип по умолчанию

            # Явно указываем типы для известных числовых полей
            # Расширен список полей, которые должны быть REAL или BIGINT
            if key in [
                "total_profit", "actual_discounted_price", "ad_spend", "ad_ctr", "ad_cpc", "ad_cr",
                "ordersSumRub", "buyoutsSumRub", "buyoutPercent", "addToCartConversion",
                "cartToOrderConversion", "salePrice", "purchase_price", "delivery_to_warehouse",
                "wb_commission_rub", "wb_logistics", "tax_rub", "packaging", "fuel", "gift",
                "defect_percent", "real_defect_percent", "cost_price", "profit_per_item", "commission_percent"
            ]:
                column_type = "REAL"
            elif key in [
                "ad_views", "ad_clicks", "ad_atbs", "ad_orders", "ad_shks", "ordersCount",
                "openCardCount", "addToCartCount", "buyoutsCount", "imtID"
            ]:
                column_type = "BIGINT" # Используем BIGINT для больших счетчиков
            elif isinstance(value, float):
                # Если это float, но не NaN, и не попало в явные списки, то REAL
                column_type = "REAL"
            elif isinstance(value, int):
                # Если число небольшое, используем INTEGER, иначе BIGINT
                if value > 2147483647 or value < -2147483648: # Максимальное/минимальное значение INTEGER
                    column_type = "BIGINT"
                else:
                    column_type = "INTEGER"
            elif isinstance(value, bool):
                column_type = "BOOLEAN"
            elif isinstance(value, (list, dict)):
                column_type = "JSONB"
            elif value is None:
                column_type = "TEXT" # Если значение None (включая NaN преобразованный в None), по умолчанию TEXT. Но мы теперь преобразуем в 0.

            try:
                # Используем двойные кавычки для сохранения регистра имени столбца
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{key}" {column_type}')
                conn.commit()
            except Exception as e:
                print(f"❌ Ошибка при добавлении столбца {key}: {e}")
                conn.rollback()


def safe_int(val):
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


def calculate_bundle_profits():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Получаем все нужные данные
    cursor.execute("""
                   SELECT "imtID", "imtName", "total_profit"
                   FROM sales
                   WHERE "date" = (SELECT MAX("date") FROM sales)
                   """)
    rows = cursor.fetchall()
    conn.close()

    # Группировка по связкам (по imtID)
    bundles = defaultdict(list)

    for imtID, imtName, profit in rows:
        profit = profit or 0 # если вдруг NULL
        bundles[imtID].append({"imtName": imtName, "profit": profit})

    # Считаем прибыль по каждой связке и выбираем имя
    results = []
    for imtID, items in bundles.items():
        total = sum(item["profit"] for item in items)
        # Убедимся, что items не пустой перед вызовом max
        best_item = max(items, key=lambda x: x["profit"]) if items else {"imtName": "Неизвестно"}
        results.append({
            "imtID": imtID,
            "title": best_item["imtName"],
            "total_profit": round(total, 2)
        })

    # Вывод в консоль
    print("📦 Прибыль по связкам:")
    for bundle in sorted(results, key=lambda x: -x["total_profit"]):
        print(f"{bundle['title']} → {bundle['total_profit']} ₽")

    return results # можно будет использовать в FastAPI


def calculate_profit_by_bundles():
    # Вчерашняя дата
    yesterday_str = (datetime.utcnow() - timedelta(glebas)).date().isoformat()

    # Подключение к базе
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    # Получаем все данные по вчерашнему дню
    cursor.execute(
        """
        SELECT "imtID", "imtName", "vendorCode", "cost_price", "total_profit", "ordersCount"
        FROM sales
        WHERE "imtID" IS NOT NULL
          AND "total_profit" IS NOT NULL
          AND "date" = %s
        """,
        (yesterday_str,),
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
            "ordersCount": orders # ИЗМЕНЕНО: теперь ordersCount
        })

    # Обработка результатов
    results = []
    for imtID, items in bundles.items():
        total_profit = sum(item["profit"] for item in items)
        best_item = max(items, key=lambda x: x["profit"]) if items else {"imtName": "Неизвестно"}
        results.append({
            "imtID": imtID,
            "title": best_item["imtName"],
            "total_profit": round(total_profit, 2),
            "items": items
        })

    # Сортировка
    results = sorted(results, key=lambda x: -x["total_profit"])

    # Вывод
    print(f"\n📊 Прибыль по связкам за {yesterday_str}:")
    for bundle in results:
        print(f"\n🔹 {bundle['title']} → {bundle['total_profit']} ₽")
        print("   📦 Товары в связке:")
        for item in bundle["items"]:
            print(
                f"     • {item['vendorCode']} | Себестоимость: {item['cost_price']} ₽ | Заказов: {item['ordersCount']} шт")


if __name__ == "__main__":
    asyncio.run(main())
    calculate_total_profit_for_day()
    calculate_profit_by_bundles()

