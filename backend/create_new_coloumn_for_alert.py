import psycopg2
import os

# Получаем URL базы данных из переменной окружения
# Если переменная окружения не установлена, используем значение по умолчанию
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries", # Замените на ваши данные
)

def create_purchase_batches_table():
    """
    Создает таблицу purchase_batches и индекс в базе данных.
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # SQL для создания таблицы purchase_batches
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS purchase_batches (
            id SERIAL PRIMARY KEY,
            vendor_code VARCHAR(255) NOT NULL,
            purchase_price NUMERIC(10, 2) NOT NULL,
            quantity_bought INTEGER NOT NULL,
            quantity_sold INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            start_date DATE NOT NULL,
            end_date DATE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """

        # SQL для создания индекса
        create_index_sql = """
        CREATE INDEX IF NOT EXISTS idx_purchase_batches_vendor_code_date ON purchase_batches (vendor_code, start_date);
        """

        print("Создание таблицы 'purchase_batches'...")
        cursor.execute(create_table_sql)
        print("Таблица 'purchase_batches' успешно создана или уже существует.")

        print("Создание индекса 'idx_purchase_batches_vendor_code_date'...")
        cursor.execute(create_index_sql)
        print("Индекс 'idx_purchase_batches_vendor_code_date' успешно создан или уже существует.")

        conn.commit()
        print("Все операции успешно выполнены и зафиксированы.")

    except psycopg2.Error as e:
        print(f"Ошибка базы данных: {e}")
        if conn:
            conn.rollback() # Откатить изменения в случае ошибки
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    create_purchase_batches_table()