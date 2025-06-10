import pandas as pd
import psycopg2
import os

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

# Загружаем Excel-файл
df_excel = pd.read_excel("Аналитика К июнь с группировкой1.xlsx")

# Переименовываем колонки под базу данных
df_excel = df_excel.rename(columns={
    "vendorCode": "vendorCode",
    "zakup": "purchase_price",
    "доставка в См": "delivery_to_warehouse",
    "Комиссия ВБ, руб": "wb_commission_rub",
    "Логистика ВБ, руб": "wb_logistics",
    "Налог 12%, руб": "tax_rub",
    "Упаковка": "packaging",
    "Бензин": "fuel",
    "подарок+": "gift",
    "98% качество": "defect_percent",
    "Себестоимость": "cost_price"
})

# Оставляем только нужные столбцы
df_filtered = df_excel[[
    "vendorCode", "purchase_price", "delivery_to_warehouse", "wb_commission_rub",
    "wb_logistics", "tax_rub", "packaging", "fuel", "gift", "defect_percent", "cost_price"
]].dropna(subset=["vendorCode"])

# Подключение к базе данных
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Добавляем новые колонки в таблицу, если их нет
new_columns = {
    "purchase_price": "REAL",
    "delivery_to_warehouse": "REAL",
    "wb_commission_rub": "REAL",
    "wb_logistics": "REAL",
    "tax_rub": "REAL",
    "packaging": "REAL",
    "fuel": "REAL",
    "gift": "REAL",
    "defect_percent": "REAL",
    "cost_price": "REAL",
    "salePrice": "REAL",  # 🔧 добавлена колонка salePrice
    "profit_per_item": "REAL",  # 🔧 и сразу колонка прибыли
}

for column, col_type in new_columns.items():
    try:
        cursor.execute(f"ALTER TABLE cards ADD COLUMN {column} {col_type}")
    except Exception:
        conn.rollback()  # если ошибка — откатываем, но продолжаем
        pass

# Обновляем строки по vendorCode
for _, row in df_filtered.iterrows():
    try:
        gift = 0 if str(row['gift']).strip() == "" else row['gift']

        cursor.execute(
            """
            UPDATE cards
            SET purchase_price        = %s,
                delivery_to_warehouse = %s,
                wb_commission_rub     = %s,
                wb_logistics          = %s,
                tax_rub               = %s,
                packaging             = %s,
                fuel                  = %s,
                gift                  = %s,
                defect_percent        = %s,
                cost_price            = %s
            WHERE vendor_code = %s
            """,
            (
                row["purchase_price"],
                row["delivery_to_warehouse"],
                row["wb_commission_rub"],
                row["wb_logistics"],
                row["tax_rub"],
                row["packaging"],
                row["fuel"],
                gift,
                row["defect_percent"],
                row["cost_price"],
                str(row["vendorCode"]),
            ),
        )
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при обновлении vendorCode={row['vendorCode']}: {e}")

# Обновляем значения прибыли
try:
    cursor.execute("""
        UPDATE cards
        SET profit_per_item = salePrice - cost_price
        WHERE salePrice IS NOT NULL AND cost_price IS NOT NULL
    """)
except Exception as e:
    conn.rollback()
    print(f"❌ Ошибка при расчёте прибыли: {e}")

conn.commit()
cursor.close()
conn.close()

print("✅ Данные успешно импортированы в базу данных и прибыль рассчитана.")
