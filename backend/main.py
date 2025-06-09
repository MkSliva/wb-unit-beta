from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi import Request
from datetime import datetime
import sqlite3
import pandas as pd

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    print(f"Error: {exc}")
    return JSONResponse(
        status_code=500,
        content={"message": f"Internal Server Error: {exc}"}
    )


@app.get("/api/sales_grouped_detailed_range")
def get_sales_grouped_detailed_range(
        start_date: str = Query(..., description="Start date в формате YYYY-MM-DD"),
        end_date: str = Query(..., description="End date в формате YYYY-MM-DD")
):
    conn = sqlite3.connect("/Users/kirillmorozov/PycharmProjects/PythonProject/backend/wildberries_cards.db")
    cursor = conn.cursor()

    query = """
            SELECT imtID, vendorCode, ordersCount, ad_spend, total_profit
            FROM sales
            WHERE date BETWEEN ? AND ? \
            """
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    df = df.fillna(0)
    print(df[["ad_spend", "imtID"]])

    conn.close()

    if df.empty:
        return {
            "message": f"No data between {start_date} and {end_date}",
            "data": [],
            "total_profit": 0
        }



    # Группировка по imtID
    grouped = df.groupby("imtID").agg({
        "ordersCount": "sum",
        "ad_spend": "sum",
        "total_profit": "sum"
    }).reset_index()

    # Добавим список артикулов к каждой группе
    grouped["vendorCodes"] = grouped["imtID"].apply(
        lambda imt: ", ".join(df[df["imtID"] == imt]["vendorCode"].unique())
    )

    total_profit = round(grouped["total_profit"].sum(), 2)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "total_profit": total_profit,
        "data": grouped.to_dict(orient="records")
    }


@app.get("/api/sales_by_imt")
def get_sales_by_imt(
    imt_id: int,
    start_date: str = Query(..., description="Start date в формате YYYY-MM-DD"),
    end_date: str = Query(..., description="End date в формате YYYY-MM-DD")
):
    conn = sqlite3.connect("/Users/kirillmorozov/PycharmProjects/PythonProject/backend/wildberries_cards.db")

    query = """
        SELECT nm_ID, vendorCode, date, ordersCount, ad_spend, total_profit, salePrice, cost_price
        FROM sales
        WHERE imtID = ? AND date BETWEEN ? AND ?
        ORDER BY date ASC
    """
    df = pd.read_sql_query(query, conn, params=(imt_id, start_date, end_date))
    conn.close()

    if df.empty:
        return {
            "message": f"No data for imtID {imt_id} between {start_date} and {end_date}",
            "data": []
        }

    df = df.fillna(0)

    grouped = df.groupby("vendorCode").agg({
        "ordersCount": "sum",
        "ad_spend": "sum",
        "total_profit": "sum",
        "salePrice": "mean",
        "cost_price": "mean"
    }).reset_index()

    return {
        "imtID": imt_id,
        "start_date": start_date,
        "end_date": end_date,
        "data": grouped.to_dict(orient="records")
    }


@app.get("/api/sales_by_imt_daily")
def get_sales_by_imt_daily(
    imt_id: int,
    start_date: str = Query(..., description="Start date в формате YYYY-MM-DD"),
    end_date: str = Query(..., description="End date в формате YYYY-MM-DD")
):
    conn = sqlite3.connect("/Users/kirillmorozov/PycharmProjects/PythonProject/backend/wildberries_cards.db")

    query = """
        SELECT date, SUM(ordersCount) AS ordersCount, SUM(ad_spend) AS ad_spend, SUM(total_profit) AS total_profit
        FROM sales
        WHERE imtID = ? AND date BETWEEN ? AND ?
        GROUP BY date
        ORDER BY date ASC
    """

    df = pd.read_sql_query(query, conn, params=(imt_id, start_date, end_date))
    conn.close()

    if df.empty:
        return {
            "message": f"No data for imtID {imt_id} between {start_date} and {end_date}",
            "data": []
        }

    df = df.fillna(0)

    return {
        "imtID": imt_id,
        "start_date": start_date,
        "end_date": end_date,
        "data": df.to_dict(orient="records")
    }

