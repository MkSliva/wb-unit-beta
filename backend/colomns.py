import psycopg2

# Подключение к базе данных
conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/wildberries")
cursor = conn.cursor()


cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'sales'
""")
columns = [row[0] for row in cursor.fetchall()]
print(columns)
