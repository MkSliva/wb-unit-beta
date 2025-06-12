import psycopg2
import os

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

def get_column_names(table_name: str) -> list[str]:
    """
    Подключается к базе данных PostgreSQL и возвращает список названий столбцов
    для указанной таблицы.
    """
    conn = None
    column_names = []
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
        """
        cur.execute(query, (table_name,))

        for row in cur.fetchall():
            column_names.append(row[0])

        cur.close()

    except psycopg2.Error as e:
        print(f"Ошибка подключения или выполнения запроса к базе данных: {e}")
    finally:
        if conn:
            conn.close()
    return column_names

if __name__ == "__main__":
    # Указываем имя таблицы 'sales'
    my_table_name = 'sales'
    columns = get_column_names(my_table_name)

    if columns:
        print(f"Названия столбцов в таблице '{my_table_name}':")
        for col in columns:
            print(f"- {col}")
    else:
        print(f"Не удалось получить названия столбцов для таблицы '{my_table_name}'. Возможно, таблица не существует или произошла ошибка подключения.")