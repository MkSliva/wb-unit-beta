import psycopg2

# Подключение к базе данных
conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/wildberries")
cursor = conn.cursor()

# Удаление столбца orderscount
try:
    cursor.execute('ALTER TABLE sales DROP COLUMN "orderscount"')
    conn.commit()
    print("✅ Колонка 'orderscount' успешно удалена.")
except Exception as e:
    print(f"❌ Ошибка: {e}")
    conn.rollback()

cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'sales'
""")

# Получаем список всех имён колонок
columns = cursor.fetchall()

# Выводим каждую колонку
for col in columns:
    print(col[0])

# Закрываем соединение


cursor.close()
conn.close()

