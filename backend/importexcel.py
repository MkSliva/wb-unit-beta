import pandas as pd
import sqlite3

# Загружаем Excel-файл
df_excel = pd.read_excel("Аналитика К июнь с группировкой.xlsx")

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
conn = sqlite3.connect("wildberries_cards.db")
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
    "cost_price": "REAL"
}

for column, col_type in new_columns.items():
    try:
        cursor.execute(f"ALTER TABLE cards ADD COLUMN {column} {col_type}")
    except sqlite3.OperationalError:
        pass  # колонка уже существует

# Обновляем строки по vendorCode
for _, row in df_filtered.iterrows():
    cursor.execute("""
        UPDATE cards SET
            purchase_price = ?,
            delivery_to_warehouse = ?,
            wb_commission_rub = ?,
            wb_logistics = ?,
            tax_rub = ?,
            packaging = ?,
            fuel = ?,
            gift = ?,
            defect_percent = ?,
            cost_price = ?
        WHERE vendorCode = ?
    """, (
        row["purchase_price"],
        row["delivery_to_warehouse"],
        row["wb_commission_rub"],
        row["wb_logistics"],
        row["tax_rub"],
        row["packaging"],
        row["fuel"],
        row["gift"],
        row["defect_percent"],
        row["cost_price"],
        str(row["vendorCode"])
    ))

conn.commit()
# Добавляем колонку для прибыли, если её ещё нет
try:
    cursor.execute("ALTER TABLE cards ADD COLUMN profit_per_item REAL")
except sqlite3.OperationalError:
    pass  # колонка уже существует

# Обновляем значения прибыли (profit_per_item = salePrice - cost_price)
cursor.execute("""
    UPDATE cards
    SET profit_per_item = salePrice - cost_price
    WHERE salePrice IS NOT NULL AND cost_price IS NOT NULL
""")

conn.commit()
conn.close()
print("✅ Прибыль на товар рассчитана и добавлена в базу.")

conn.close()
print("✅ Данные успешно импортированы в базу данных.")



def import_excel_if_missing(db_path="wildberries_cards.db", excel_path="Аналитика К июнь с группировкой.xlsx"):
    df_excel = pd.read_excel("Аналитика К июнь с группировкой.xlsx")

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
    conn = sqlite3.connect("wildberries_cards.db")
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
        "cost_price": "REAL"
    }

    for column, col_type in new_columns.items():
        try:
            cursor.execute(f"ALTER TABLE cards ADD COLUMN {column} {col_type}")
        except sqlite3.OperationalError:
            pass  # колонка уже существует

    # Обновляем строки по vendorCode
    for _, row in df_filtered.iterrows():
        cursor.execute("""
                       UPDATE cards
                       SET purchase_price        = ?,
                           delivery_to_warehouse = ?,
                           wb_commission_rub     = ?,
                           wb_logistics          = ?,
                           tax_rub               = ?,
                           packaging             = ?,
                           fuel                  = ?,
                           gift                  = ?,
                           defect_percent        = ?,
                           cost_price            = ?
                       WHERE vendorCode = ?
                       """, (
                           row["purchase_price"],
                           row["delivery_to_warehouse"],
                           row["wb_commission_rub"],
                           row["wb_logistics"],
                           row["tax_rub"],
                           row["packaging"],
                           row["fuel"],
                           row["gift"],
                           row["defect_percent"],
                           row["cost_price"],
                           str(row["vendorCode"])
                       ))

    conn.commit()
    # Добавляем колонку для прибыли, если её ещё нет
    try:
        cursor.execute("ALTER TABLE cards ADD COLUMN profit_per_item REAL")
    except sqlite3.OperationalError:
        pass  # колонка уже существует

    # Обновляем значения прибыли (profit_per_item = salePrice - cost_price)
    cursor.execute("""
                   UPDATE cards
                   SET profit_per_item = salePrice - cost_price
                   WHERE salePrice IS NOT NULL
                     AND cost_price IS NOT NULL
                   """)

    conn.commit()
    conn.close()
    print("✅ Прибыль на товар рассчитана и добавлена в базу.")

    conn.close()
    print("✅ Данные успешно импортированы в базу данных.")



