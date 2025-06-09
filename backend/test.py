import psycopg2
import os

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

# Подключаемся к базе данных
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Обновляем cost_price для нужного vendorCode
vendor_code = "OppoA96128blue"
new_cost_price = 3816

cursor.execute(
    """
    UPDATE cards
    SET profit_per_item = %s
    WHERE vendorCode = %s
    """,
    (new_cost_price, vendor_code),
)

# Сохраняем изменения и закрываем соединение
conn.commit()
conn.close()

print(f"✅ Стоимость для {vendor_code} обновлена на {new_cost_price}")

import psycopg2

# Подключение к базе данных
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# VendorCode, который нужно проверить
vendor_code = "OppoA96128blue"

# Запрос cost_price и profit_per_item
cursor.execute(
    """
    SELECT cost_price, profit_per_item
    FROM cards
    WHERE vendorCode = %s
    """,
    (vendor_code,),
)

result = cursor.fetchone()

# Вывод результата
if result:
    cost_price, profit_per_item = result
    print(f"💡 Для vendorCode '{vendor_code}':")
    print(f"   Себестоимость (cost_price): {cost_price}")
    print(f"   Прибыль с единицы (profit_per_item): {profit_per_item}")
else:
    print(f"❌ Товар с vendorCode '{vendor_code}' не найден в таблице cards.")

conn.close()

