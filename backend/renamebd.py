import psycopg2

DB_URL = "postgresql://postgres:postgres@localhost:5432/wildberries"  # замени при необходимости

def rename_column():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    try:
        cursor.execute('ALTER TABLE sales RENAME COLUMN "orderscount1" TO "orderscount";')
        conn.commit()
        print("✅ Колонка успешно переименована")
    except Exception as e:
        print(f"❌ Ошибка при переименовании: {e}")
        conn.rollback()
    finally:
        conn.close()

rename_column()


