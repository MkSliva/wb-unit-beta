import sqlite3

def init_db():
    conn = sqlite3.connect("../backend/wildberries_cards.db")
    cursor = conn.cursor()
    cursor.execute("""
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
    """)
    conn.commit()
    conn.close()



def save_cards_to_db(cards):
    conn = sqlite3.connect("../backend/wildberries_cards.db")
    cursor = conn.cursor()

    for card in cards:
        cursor.execute('''
            INSERT OR REPLACE INTO cards (
                nmID, imtID, vendorCode, brand, subjectName, price
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            card["nmID"],
            card["imtID"],
            card["vendorCode"],
            card.get("brand"),
            card.get("subjectName"),
            card["price"]
        ))

    conn.commit()
    conn.close()


