from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi import Request
from datetime import datetime
from pydantic import BaseModel
import os
import psycopg2
import psycopg2.extras
import pandas as pd

app = FastAPI()

DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

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
    conn = psycopg2.connect(DB_URL)

    query = """
            SELECT imtID, vendorCode, ordersCount, ad_spend, salePrice,
                   purchase_price, delivery_to_warehouse, wb_commission_rub,
                   wb_logistics, tax_rub, packaging, fuel, gift, defect_percent
            FROM sales
            WHERE date BETWEEN %s AND %s
        """
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    conn.close()

    df = df.fillna(0)

    if df.empty:
        return {
            "message": f"No data between {start_date} and {end_date}",
            "data": [],
            "total_profit": 0
        }

    df["cost_price"] = (
        df["purchase_price"] + df["delivery_to_warehouse"] + df["wb_commission_rub"] +
        df["wb_logistics"] + df["tax_rub"] + df["packaging"] +
        df["fuel"] + df["gift"] + df["defect_percent"]
    )
    df["profit"] = (df["salePrice"] - df["cost_price"]) * df["ordersCount"] - df["ad_spend"]

    grouped = df.groupby("imtID").agg({
        "ordersCount": "sum",
        "ad_spend": "sum",
        "profit": "sum"
    }).reset_index()

    grouped["vendorCodes"] = grouped["imtID"].apply(
        lambda imt: ", ".join(df[df["imtID"] == imt]["vendorCode"].unique())
    )

    grouped = grouped.rename(columns={"profit": "total_profit"})
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
    conn = psycopg2.connect(DB_URL)

    query = """
        SELECT nm_ID, vendorCode, date, ordersCount, ad_spend, salePrice,
               purchase_price, delivery_to_warehouse, wb_commission_rub,
               wb_logistics, tax_rub, packaging, fuel, gift, defect_percent
        FROM sales
        WHERE imtID = %s AND date BETWEEN %s AND %s
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

    df["cost_price"] = (
        df["purchase_price"] + df["delivery_to_warehouse"] + df["wb_commission_rub"] +
        df["wb_logistics"] + df["tax_rub"] + df["packaging"] +
        df["fuel"] + df["gift"] + df["defect_percent"]
    )
    df["profit"] = (df["salePrice"] - df["cost_price"]) * df["ordersCount"] - df["ad_spend"]

    grouped = df.groupby("vendorCode").agg({
        "ordersCount": "sum",
        "ad_spend": "sum",
        "profit": "sum",
        "salePrice": "mean",
        "cost_price": "mean",
        "purchase_price": "mean",
        "delivery_to_warehouse": "mean",
        "wb_commission_rub": "mean",
        "wb_logistics": "mean",
        "tax_rub": "mean",
        "packaging": "mean",
        "fuel": "mean",
        "gift": "mean",
        "defect_percent": "mean"
    }).reset_index()

    grouped = grouped.rename(columns={"profit": "total_profit"})

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
    conn = psycopg2.connect(DB_URL)

    query = """
        SELECT date, ordersCount, ad_spend, salePrice,
               purchase_price, delivery_to_warehouse, wb_commission_rub,
               wb_logistics, tax_rub, packaging, fuel, gift, defect_percent
        FROM sales
        WHERE imtID = %s AND date BETWEEN %s AND %s
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

    df["cost_price"] = (
        df["purchase_price"] + df["delivery_to_warehouse"] + df["wb_commission_rub"] +
        df["wb_logistics"] + df["tax_rub"] + df["packaging"] +
        df["fuel"] + df["gift"] + df["defect_percent"]
    )
    df["profit"] = (df["salePrice"] - df["cost_price"]) * df["ordersCount"] - df["ad_spend"]

    grouped = df.groupby("date").agg({
        "ordersCount": "sum",
        "ad_spend": "sum",
        "profit": "sum"
    }).reset_index()

    grouped = grouped.rename(columns={"profit": "total_profit"})

    return {
        "imtID": imt_id,
        "start_date": start_date,
        "end_date": end_date,
        "data": grouped.to_dict(orient="records")
    }


class CostUpdate(BaseModel):
    vendorCode: str
    start_date: str
    purchase_price: float | None = None
    delivery_to_warehouse: float | None = None
    wb_commission_rub: float | None = None
    wb_logistics: float | None = None
    tax_rub: float | None = None
    packaging: float | None = None
    fuel: float | None = None
    gift: float | None = None
    defect_percent: float | None = None


@app.post("/api/update_costs")
def update_costs(update: CostUpdate):
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    fields = []
    values = []
    for field in [
        "purchase_price",
        "delivery_to_warehouse",
        "wb_commission_rub",
        "wb_logistics",
        "tax_rub",
        "packaging",
        "fuel",
        "gift",
        "defect_percent",
    ]:
        val = getattr(update, field)
        if val is not None:
            fields.append(f"{field} = %s")
            values.append(val)

    updated_rows = 0

    if fields:
        query = f"UPDATE sales SET {', '.join(fields)} WHERE vendorCode = %s AND date >= %s"
        cursor.execute(query, values + [update.vendorCode, update.start_date])
        cursor.execute(
            """
            UPDATE sales
            SET cost_price = COALESCE(purchase_price,0) + COALESCE(delivery_to_warehouse,0) +
                COALESCE(wb_commission_rub,0) + COALESCE(wb_logistics,0) + COALESCE(tax_rub,0) +
                COALESCE(packaging,0) + COALESCE(fuel,0) + COALESCE(gift,0) + COALESCE(defect_percent,0),
                total_profit = (salePrice - (COALESCE(purchase_price,0) + COALESCE(delivery_to_warehouse,0) +
                COALESCE(wb_commission_rub,0) + COALESCE(wb_logistics,0) + COALESCE(tax_rub,0) +
                COALESCE(packaging,0) + COALESCE(fuel,0) + COALESCE(gift,0) + COALESCE(defect_percent,0))) * ordersCount - ad_spend
            WHERE vendorCode = %s AND date >= %s
            """,
            (update.vendorCode, update.start_date),
        )
        updated_rows = cursor.rowcount

        # Обновляем базовую таблицу cards для будущих расчётов
        cursor.execute(
            f"UPDATE cards SET {', '.join(fields)} WHERE vendorCode = %s",
            values + [update.vendorCode],
        )

    conn.commit()
    conn.close()

    return {"updated": updated_rows}

