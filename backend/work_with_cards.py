import requests
import sqlite3
import time
from dotenv import load_dotenv
import os

# === Конфигурация ===
load_dotenv("api.env")
WB_API_KEY = os.getenv("WB_API_KEY")
TAX_PERCENT = 12  # Процент налога
DEFECT_PERCENT = 2  # Процент брака
SALARY_PER_ITEM = 100  # Зарплата на единицу товара

# === Получение всех карточек товара через Content API ===
def fetch_all_cards():
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers = {
        "Authorization": WB_API_KEY,
        "Content-Type": "application/json"
    }

    limit = 100
    cursor_data = {}
    all_cards = {}

    for _ in range(100):  # максимум 100 запросов
        payload = {
            "settings": {
                "cursor": {"limit": limit},
                "filter": {"withPhoto": -1}
            }
        }

        if cursor_data:
            payload["settings"]["cursor"].update(cursor_data)

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        cards = data.get("cards", [])
        for card in cards:
            nm_id = card.get("nmID")
            if nm_id:
                all_cards[nm_id] = {
                    "vendorCode": card.get("vendorCode"),
                    "nmID": nm_id
                }

        cursor_data = data.get("cursor", {})
        if cursor_data.get("total", 0) < limit:
            break

        time.sleep(0.7)  # ограничение по API — не более 100 запросов в минуту

    print(f"✅ Получено карточек: {len(all_cards)}")
    return all_cards

# === Получение всех цен со скидкой через Discount Prices API ===
def get_all_discounted_prices(api_key: str) -> dict:
    url = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
    headers = {
        "Authorization": WB_API_KEY
    }

    limit = 1000
    offset = 0
    result = {}

    while True:
        params = {
            "limit": limit,
            "offset": offset
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            print(response)
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

# === Обновление таблицы cards в БД с расчётом прибыли ===
def update_cards_with_profit():
    conn = sqlite3.connect("wildberries_cards.db")
    cursor = conn.cursor()

    # Получаем карточки и цены
    cards = fetch_all_cards()
    discounted_prices = get_all_discounted_prices(WB_API_KEY)

    for nm_id, info in cards.items():
        vendor_code = info.get("vendorCode", "")
        sale_price = discounted_prices.get(nm_id)

        if sale_price is None:
            print(f"⚠️ Пропущено (нет цены): {vendor_code}")
            continue

        print(f"{vendor_code} — цена со скидкой: {sale_price}")

        # Получаем себестоимость и расходы из таблицы
        cursor.execute("""
            SELECT purchase_price, delivery_to_warehouse, commission_percent, wb_logistics,
                   packaging, fuel, gift
            FROM cards WHERE vendorCode = ?
        """, (vendor_code,))
        row = cursor.fetchone()
        if not row:
            print(f"⚠️ Пропущено (нет данных в БД): {vendor_code}")
            continue

        (purchase_price, delivery_to_warehouse, commission_percent, wb_logistics,
         packaging, fuel, gift) = [v or 0 for v in row]

        # Расчёты
        tax_rub = sale_price * TAX_PERCENT / 100
        commission_rub = sale_price * (commission_percent or 0) / 100
        wb_commission_rub = sale_price * (commission_percent or 0) / 100

        print("purchase_price:", purchase_price)
        print("delivery_to_warehouse:", delivery_to_warehouse)
        print("commission_rub:", commission_rub)
        print("wb_logistics:", wb_logistics)
        print("tax_rub:", tax_rub)
        print("packaging:", packaging)
        print("fuel:", fuel)
        print("gift:", gift)
        print("SALARY_PER_ITEM:", SALARY_PER_ITEM)
        print("DEFECT_PERCENT:", DEFECT_PERCENT)

        cost_price = (
            purchase_price + delivery_to_warehouse + commission_rub + wb_logistics +
            tax_rub + packaging + fuel + gift + SALARY_PER_ITEM
        ) * (1 + DEFECT_PERCENT / 100)
        profit = sale_price - cost_price
        print(sale_price)
        print(cost_price)
        print(profit)


        # Обновляем таблицу
        cursor.execute("""
            UPDATE cards
            SET salePrice = ?, tax_rub = ?, wb_commission_rub = ?, cost_price = ?, profit_per_item = ?
            WHERE vendorCode = ?
        """, (sale_price, tax_rub, wb_commission_rub, cost_price, profit, vendor_code))

    conn.commit()
    conn.close()
    print("🎯 Таблица cards обновлена!")

def get_commission_rates_and_update_cards(WB_API_KEY: str, db_path: str = "wildberries_cards.db"):
    url = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
    headers = {
        "Authorization": WB_API_KEY
    }
    params = {
        "locale": "ru"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Ошибка при получении комиссии: {e}")
        return

    # Открываем соединение с БД
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    updated = 0
    for entry in data.get("report", []):
        subject_name = entry.get("subjectName")
        kgvp_supplier = entry.get("kgvpSupplier")
        print("Комиссия для товара", subject_name, kgvp_supplier)

        if subject_name and kgvp_supplier is not None:
            cursor.execute("""
                UPDATE cards
                SET commission_percent = ?
                WHERE subjectName = ?
            """, (kgvp_supplier, subject_name))
            updated += cursor.rowcount

    conn.commit()
    conn.close()

    print(f"✅ Обновлено строк в таблице cards: {updated}")

import sqlite3

def find_incomplete_cards(db_path="wildberries_cards.db"):
    # Критически важные поля для расчёта прибыли
    required_fields = [
        "purchase_price", "delivery_to_warehouse", "wb_commission_rub",
        "wb_logistics", "tax_rub", "packaging", "fuel", "gift",
        "defect_percent", "commission_percent"
    ]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    conditions = [f"({field} IS NULL OR {field} = '')" for field in required_fields]
    where_clause = " OR ".join(conditions)

    query = f"""
        SELECT vendorCode, nmID, imtID, {', '.join(required_fields)}
        FROM cards
        WHERE {where_clause}
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("✅ Все карточки заполнены корректно.")
        return

    ks = 0
    incomplete_rows = []
    print("🚫 Найдены карточки с неполными данными:\n")
    for row in rows:
        ks += 1
        vendor_code = row[0]
        incomplete_rows.append(row)
        nm_id = row[1]
        imt_name = row[2]
        missing_fields = [
            required_fields[i - 3] for i in range(3, len(row)) if row[i] in (None, '', 0)
        ]
        print(f"📦 {vendor_code} ({nm_id}) — {imt_name}")
        print(f"    ❗ Пропущены поля: {', '.join(missing_fields)}\n")
        print("Кол-во незаполненых до конца карточек", ks)

    if incomplete_rows:
        vendor_codes = [row[0] for row in incomplete_rows if row[0]]
        print("🛠️ Товары с неполными параметрами:\n" + ", ".join(vendor_codes))
    else:
        print("✅ Все товары имеют необходимые параметры.")


# === Точка входа ===
if __name__ == "__main__":
    #get_commission_rates_and_update_cards(WB_API_KEY)
    #update_cards_with_profit()
    find_incomplete_cards()



