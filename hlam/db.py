import os
import psycopg2

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

def init_db():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cards (
            nmID INTEGER PRIMARY KEY,
            imtID INTEGER,
            vendorCode TEXT,
            brand TEXT,
            subjectName TEXT,
            vendorID INTEGER,
            price INTEGER,
            salePrice INTEGER
        )
        """
    )
    print(conn.status)
    conn.commit()
    conn.close()



def save_cards_to_db(cards):
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    for card in cards:
        cursor.execute(
            """
            INSERT INTO cards (
                nmID, imtID, vendorCode, brand, subjectName, price
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (nmID) DO UPDATE SET
                imtID = EXCLUDED.imtID,
                vendorCode = EXCLUDED.vendorCode,
                brand = EXCLUDED.brand,
                subjectName = EXCLUDED.subjectName,
                price = EXCLUDED.price
            """,
            (
                card["nmID"],
                card["imtID"],
                card["vendorCode"],
                card.get("brand"),
                card.get("subjectName"),
                card["price"],
            ),
        )

    conn.commit()
    conn.close()


init_db()