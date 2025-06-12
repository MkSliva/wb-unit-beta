from fastapi import FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List
import os
import psycopg2
import pandas as pd
import numpy as np
import requests  # Для Telegram уведомлений
from datetime import datetime, date, timedelta

app = FastAPI()

# ⚠️ Убедитесь, что переменная окружения DB_URL установлена
DB_URL = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@localhost:5432/wildberries",
)

# --- Telegram Bot API Настройки ---
# ⚠️ ОБЯЗАТЕЛЬНО УСТАНОВИТЕ ЭТИ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ НА ВАШЕМ СЕРВЕРЕ!
# Пример: export TELEGRAM_BOT_TOKEN="YOUR_BOT_TOKEN"
# Пример: export TELEGRAM_CHAT_ID="YOUR_CHAT_ID"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_TELEGRAM_CHAT_ID_HERE")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене лучше указать конкретные домены
    allow_methods=["*"],
    allow_headers=["*"]
)


# --- Глобальный обработчик исключений ---
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Логирование ошибки для отладки на сервере
    print(f"Server Error occurred at path: {request.url.path}")
    print(f"Exception Type: {type(exc).__name__}")
    print(f"Exception Detail: {exc}")
    import traceback
    traceback.print_exc()  # Вывод полного стека вызовов в логи сервера

    # Отправка уведомления в Telegram (необходимо закомментировать в продакшене, если не хотите спамить)
    # send_telegram_notification(f"🚨 Ошибка на сервере: {request.url.path} - {type(exc).__name__} - {exc}")

    return JSONResponse(
        status_code=500,
        content={"message": f"Internal Server Error: {type(exc).__name__} - {exc}"}
    )


# --- Вспомогательная функция для отправки Telegram уведомлений ---
def send_telegram_notification(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()  # Выбросить исключение для плохих статусов (4xx или 5xx)
        print("Telegram notification sent successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Telegram notification: {e}")


# --- Вспомогательная функция для обновления purchase_price в sales ---
def update_sales_purchase_prices(df: pd.DataFrame, conn_obj):
    if df.empty:
        return df

    # Убедимся, что 'date' в df - datetime
    df["date"] = pd.to_datetime(df["date"])

    vendor_codes = tuple(df["vendorcode"].unique())
    if not vendor_codes:  # Если нет vendor_codes, возвращаем df как есть
        return df

    # Получаем все партии для нужных vendor_codes
    query_batches = f"""
        SELECT vendor_code, purchase_price, quantity_bought, quantity_sold, start_date, end_date
        FROM purchase_batches
        WHERE vendor_code IN %s
        ORDER BY vendor_code, start_date DESC;
    """
    batches_df = pd.read_sql_query(query_batches, conn_obj, params=(vendor_codes,))
    batches_df["start_date"] = pd.to_datetime(batches_df["start_date"])
    # end_date может быть NULL, поэтому обрабатываем его отдельно. 'coerce' преобразует невалидные даты в NaT.
    batches_df["end_date"] = pd.to_datetime(batches_df["end_date"], errors='coerce')

    # Создаем временную копию purchase_price из sales, чтобы использовать ее как fallback
    df['original_sales_purchase_price'] = df['purchase_price']

    # Теперь для каждой строки в df (продажи), найдем подходящую purchase_price
    def get_purchase_price_for_sale(row, batches_df_local):  # Передаем batches_df как локальный аргумент
        vendor = row["vendorcode"]
        sale_date = row["date"]
        original_sales_price = row["original_sales_purchase_price"]  # Берем исходную цену из sales

        # Найти активную партию на данную дату продажи
        active_batches = batches_df_local[
            (batches_df_local["vendor_code"] == vendor) &
            (batches_df_local["start_date"] <= sale_date) &
            (
                    (batches_df_local["end_date"].isna()) |  # end_date is NULL (партия активна)
                    (batches_df_local["end_date"] >= sale_date)  # или end_date после или в день продажи
            )
            ].sort_values(by="start_date", ascending=False)  # Берем самую новую активную

        if not active_batches.empty:
            return active_batches.iloc[0]["purchase_price"]
        else:
            # Если партия не найдена, используем original_sales_purchase_price из таблицы sales
            return original_sales_price

    # Применяем эту функцию к каждой строке DataFrame
    df["purchase_price"] = df.apply(lambda row: get_purchase_price_for_sale(row, batches_df), axis=1)

    # Удаляем временный столбец
    df = df.drop(columns=['original_sales_purchase_price'])

    return df


@app.get("/api/test_telegram_notification")
async def test_telegram_notification(
        message: str = Query("Test message from FastAPI backend!", description="Message to send to Telegram")):
    """
    Тестовый эндпоинт для отправки уведомления в Telegram.
    Используется для проверки правильности настроек TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID.
    """
    try:
        send_telegram_notification(f"<b>Тестовое уведомление:</b> {message}")
        return {"status": "success", "message": "Test notification sent!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send test notification: {e}")


## Эндпоинты для получения данных о продажах

### Группировка продаж по диапазону дат
@app.get("/api/sales_grouped_detailed_range")
def get_sales_grouped_detailed_range(
        start_date: str = Query(..., description="Start date в формате YYYY-MM-DD"),
        end_date: str = Query(..., description="End date в формате YYYY-MM-DD")
):
    conn = None  # Инициализируем conn для блока finally
    try:
        conn = psycopg2.connect(DB_URL)

        query = """
                SELECT "imtID", \
                       "vendorCode", \
                       "ordersCount", \
                       "ad_spend", \
                       "actual_discounted_price",
                       "purchase_price",
                       "delivery_to_warehouse", \
                       "wb_commission_rub",
                       "wb_logistics", \
                       "tax_rub", \
                       "packaging", \
                       "fuel", \
                       "gift", \
                       "defect_percent", date
                FROM sales
                WHERE date BETWEEN %s \
                  AND %s \
                """
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        # Приводим названия столбцов к нижнему регистру для единообразия в pandas
        df.columns = df.columns.str.lower()
        df["date"] = pd.to_datetime(df["date"])  # Преобразуем дату для корректного сравнения

        if df.empty:
            return {
                "message": f"No data between {start_date} and {end_date}",
                "data": [],
                "total_profit": 0
            }

        df = df.fillna(0)  # Заполняем NaN нулями перед расчетами

        # --- ОБНОВЛЯЕМ purchase_price НА ОСНОВЕ ПАРТИЙ ИЛИ sales.purchase_price ---
        df = update_sales_purchase_prices(df, conn)
        # --- КОНЕЦ ОБНОВЛЕНИЯ ---

        # Пересчитываем cost_price и profit в pandas
        df["cost_price"] = (
                df["purchase_price"] + df["delivery_to_warehouse"] + df["wb_commission_rub"] +
                df["wb_logistics"] + df["tax_rub"] + df["packaging"] +
                df["fuel"] + df["gift"] + df["defect_percent"]
        )
        df["orderscount"] = df["orderscount"].astype(int)
        df["profit"] = (df["actual_discounted_price"] - df["cost_price"]) * df["orderscount"] - df["ad_spend"]

        df["investment_total"] = (
                df["purchase_price"] + df["delivery_to_warehouse"] + df["packaging"] +
                df["fuel"] + df["gift"]
        ) * df["orderscount"]

        grouped = df.groupby("imtid").agg({
            "orderscount": "sum",
            "ad_spend": "sum",
            "profit": "sum",
            "investment_total": "sum"
        }).reset_index()

        max_sales_per_vendorcode = df.loc[df.groupby('imtid')['orderscount'].idxmax()]
        # Выбираем только нужные столбцы и переименовываем vendorcode для ясности
        max_sales_per_vendorcode = max_sales_per_vendorcode[['imtid', 'vendorcode']].rename(
            columns={'vendorcode': 'best_selling_vendorcode'})

        # Объединяем полученную информацию с нашей основной группировкой
        grouped = pd.merge(grouped, max_sales_per_vendorcode, on='imtid', how='left')

        # Объединяем vendorCode для каждой imtID
        grouped["vendorcodes"] = grouped["imtid"].apply(
            lambda imt: ", ".join(str(v) for v in df[df["imtid"] == imt]["vendorcode"].unique())
        )

        grouped = grouped.rename(columns={"profit": "total_profit"})
        grouped["margin_percent"] = grouped.apply(
            lambda row: (row["total_profit"] / row["investment_total"] * 100)
            if row["investment_total"] != 0 else 0,
            axis=1,
        )
        grouped = grouped.drop(columns=["investment_total"])
        total_profit = round(grouped["total_profit"].sum(), 2)
        total_orders = int(df["orderscount"].sum())
        total_ad_spend = round(df["ad_spend"].sum(), 2)

        return {
            "start_date": start_date,
            "end_date": end_date,
            "total_profit": total_profit,
            "total_orders": total_orders,
            "total_ad_spend": total_ad_spend,
            "data": grouped.to_dict(orient="records")
        }
    finally:
        if conn:
            conn.close()


### Продажи по конкретной imtID, сгруппированные по vendorCode
@app.get("/api/sales_by_imt")
def get_sales_by_imt(
        imt_id: int,
        start_date: str = Query(..., description="Start date в формате YYYY-MM-DD"),
        end_date: str = Query(..., description="End date в формате YYYY-MM-DD")
):
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)

        query = """
                SELECT date, "ordersCount", "ad_spend", "salePrice", "purchase_price", "delivery_to_warehouse", "wb_commission_rub", "wb_logistics", "tax_rub", "packaging", "fuel", "gift", "defect_percent", "actual_discounted_price", "vendorCode"
                FROM sales
                WHERE "imtID" = %s \
                  AND date BETWEEN %s \
                  AND %s
                ORDER BY date ASC \
                """

        df = pd.read_sql_query(query, conn, params=(imt_id, start_date, end_date))
        df.columns = df.columns.str.lower()
        df["date"] = pd.to_datetime(df["date"])

        if df.empty:
            return {
                "message": f"No data for imtID {imt_id} between {start_date} and {end_date}",
                "data": []
            }

        df = df.fillna(0)  # Заполняем NaN нулями перед расчетами

        # --- ОБНОВЛЯЕМ purchase_price НА ОСНОВЕ ПАРТИЙ ИЛИ sales.purchase_price ---
        df = update_sales_purchase_prices(df, conn)
        # --- КОНЕЦ ОБНОВЛЕНИЯ ---

        # Пересчитываем cost_price и profit
        df["cost_price"] = (
                df["purchase_price"] + df["delivery_to_warehouse"] + df["wb_commission_rub"] +
                df["wb_logistics"] + df["tax_rub"] + df["packaging"] +
                df["fuel"] + df["gift"] + df["defect_percent"]
        )
        df["orderscount"] = df["orderscount"].astype(int)
        df["profit"] = (df["actual_discounted_price"] - df["cost_price"]) * df["orderscount"] - df["ad_spend"]

        grouped = df.groupby("vendorcode").agg({
            "orderscount": "sum",
            "ad_spend": "sum",
            "profit": "sum",
            "actual_discounted_price": "mean",  # Средняя цена продажи
            "cost_price": "mean",  # Средняя себестоимость
            "purchase_price": "mean",  # Теперь будет средняя из обновленных purchase_price
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
    finally:
        if conn:
            conn.close()


### Ежедневные продажи по конкретной imtID (УЖЕ ПОДХОДИТ ДЛЯ ГРАФИКОВ ПРИБЫЛИ, ЗАКАЗОВ, РЕКЛАМЫ)
@app.get("/api/sales_by_imt_daily")
def get_sales_by_imt_daily(
        imt_id: int,
        start_date: str = Query(..., description="Start date в формате YYYY-MM-DD"),
        end_date: str = Query(..., description="End date в формате YYYY-MM-DD")
):
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)

        query = """
                SELECT date, "ordersCount", "ad_spend", "salePrice", "purchase_price", "delivery_to_warehouse", "wb_commission_rub", "wb_logistics", "tax_rub", "packaging", "fuel", "gift", "defect_percent", "actual_discounted_price", "vendorCode"
                FROM sales
                WHERE "imtID" = %s \
                  AND date BETWEEN %s \
                  AND %s
                ORDER BY date ASC \
                """

        df = pd.read_sql_query(query, conn, params=(imt_id, start_date, end_date))
        df.columns = df.columns.str.lower()  # Приводим названия столбцов к нижнему регистру
        df["date"] = pd.to_datetime(df["date"])

        if df.empty:
            return {
                "message": f"No data for imtID {imt_id} between {start_date} and {end_date}",
                "data": []
            }

        df = df.fillna(0)  # Заполняем NaN нулями перед расчетами

        # --- ОБНОВЛЯЕМ purchase_price НА ОСНОВЕ ПАРТИЙ ИЛИ sales.purchase_price ---
        df = update_sales_purchase_prices(df, conn)
        # --- КОНЕЦ ОБНОВЛЕНИЯ ---

        # Пересчитываем cost_price и profit
        df["cost_price"] = (
                df["purchase_price"] + df["delivery_to_warehouse"] + df["wb_commission_rub"] +
                df["wb_logistics"] + df["tax_rub"] + df["packaging"] +
                df["fuel"] + df["gift"] + df["defect_percent"]
        )
        df["orderscount"] = df["orderscount"].astype(int)
        df["profit"] = (df["actual_discounted_price"] - df["cost_price"]) * df["orderscount"] - df["ad_spend"]

        grouped = df.groupby("date").agg({
            "orderscount": "sum",
            "ad_spend": "sum",
            "profit": "sum"
        }).reset_index()

        grouped = grouped.rename(columns={"profit": "total_profit"})

        # Преобразуем дату в ISO формат для JSON. Теперь это безопасно, так как 'date' - datetime.
        grouped["date"] = grouped["date"].dt.strftime("%Y-%m-%d")

        return {
            "imtID": imt_id,
            "start_date": start_date,
            "end_date": end_date,
            "data": grouped.to_dict(orient="records")
        }
    finally:
        if conn:
            conn.close()


@app.get("/api/sales_overall_daily")
def get_sales_overall_daily(
        start_date: str = Query(..., description="Start date в формате YYYY-MM-DD"),
        end_date: str = Query(..., description="End date в формате YYYY-MM-DD")):
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)

        query = """
                SELECT date, "ordersCount", "ad_spend", "salePrice", "purchase_price",
                       "delivery_to_warehouse", "wb_commission_rub", "wb_logistics", "tax_rub",
                       "packaging", "fuel", "gift", "defect_percent", "actual_discounted_price",
                       "vendorCode"
                FROM sales
                WHERE date BETWEEN %s
                  AND %s
                ORDER BY date ASC
                """

        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        df.columns = df.columns.str.lower()
        df["date"] = pd.to_datetime(df["date"])

        if df.empty:
            return {
                "message": f"No data between {start_date} and {end_date}",
                "data": []
            }

        df = df.fillna(0)

        df = update_sales_purchase_prices(df, conn)

        df["cost_price"] = (
                df["purchase_price"] + df["delivery_to_warehouse"] + df["wb_commission_rub"] +
                df["wb_logistics"] + df["tax_rub"] + df["packaging"] +
                df["fuel"] + df["gift"] + df["defect_percent"]
        )
        df["orderscount"] = df["orderscount"].astype(int)
        df["profit"] = (df["actual_discounted_price"] - df["cost_price"]) * df["orderscount"] - df["ad_spend"]

        grouped = df.groupby("date").agg({
            "orderscount": "sum",
            "ad_spend": "sum",
            "profit": "sum"
        }).reset_index()

        grouped = grouped.rename(columns={"profit": "total_profit"})
        grouped["date"] = grouped["date"].dt.strftime("%Y-%m-%d")

        return {
            "start_date": start_date,
            "end_date": end_date,
            "data": grouped.to_dict(orient="records")
        }
    finally:
        if conn:
            conn.close()


# --- НОВАЯ Pydantic модель для ответа истории закупочных цен по дням ---
class PurchasePriceHistoryDailyResponse(BaseModel):
    date: date
    purchase_price: float


class LatestCostsResponse(BaseModel):
    vendor_code: Optional[str] = None
    date: date
    purchase_price: float
    delivery_to_warehouse: float
    wb_commission_rub: float
    wb_logistics: float
    tax_rub: float
    packaging: float
    fuel: float
    gift: float
    defect_percent: float


class MissingCostEntry(BaseModel):
    vendor_code: str
    dates: List[date]


# --- НОВЫЙ Эндпоинт для получения ежедневной истории закупочных цен ---
@app.get("/api/purchase_price_history_daily", response_model=List[PurchasePriceHistoryDailyResponse])
async def get_purchase_price_history_daily(
        vendor_code: str = Query(..., description="Vendor code to fetch history for"),
        start_date: date = Query(..., description="Start date for history (YYYY-MM-DD)"),
        end_date: date = Query(..., description="End date for history (YYYY-MM-DD)")
):
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # Получаем все партии для данного vendor_code, которые пересекаются с запрошенным диапазоном
        query = """
                SELECT start_date, COALESCE(end_date, CURRENT_DATE) as end_date_effective, purchase_price
                FROM purchase_batches
                WHERE vendor_code = %s
                  AND (
                    (start_date <= %s AND COALESCE(end_date, '2100-01-01') >= %s) OR -- Партия началась до конца периода и закончилась после начала
                    (start_date >= %s AND start_date <= %s) -- Партия началась внутри периода
                    )
                ORDER BY start_date; \
                """
        # Adjusted end_date for query to avoid future dates for active batches for simplicity
        effective_query_end_date = min(end_date, date.today())
        cursor.execute(query, (vendor_code, effective_query_end_date, start_date, start_date, effective_query_end_date))
        batches_raw = cursor.fetchall()

        # Преобразуем в список словарей для удобства
        batches = []
        for b_start, b_end_eff, b_price in batches_raw:
            batches.append({
                "start_date": b_start,
                "end_date_effective": b_end_eff,
                "purchase_price": b_price
            })

        history_data = []
        current_date_iter = start_date
        while current_date_iter <= end_date:
            # Находим активную партию на текущую дату
            active_batch_price = 0.0  # Дефолтное значение, если партия не найдена
            found_batch_for_day = False

            # Ищем самую последнюю активную партию на текущую дату
            for batch in sorted(batches, key=lambda x: x['start_date'], reverse=True):
                if batch['start_date'] <= current_date_iter and batch['end_date_effective'] >= current_date_iter:
                    active_batch_price = batch['purchase_price']
                    found_batch_for_day = True
                    break  # Нашли самую актуальную партию на эту дату

            if found_batch_for_day:
                history_data.append(
                    PurchasePriceHistoryDailyResponse(date=current_date_iter, purchase_price=active_batch_price))
            # Если партия не найдена для конкретного дня, мы можем:
            # 1. Пропустить этот день (график будет прерывистым)
            # 2. Использовать 0 (как сейчас)
            # 3. Использовать last known price (сложнее)
            # 4. Или получить price из sales таблицы для этого дня.
            # Для начала оставляем 0, если не найдено, чтобы не ломать график.
            # Если нужно показывать только при наличии, тогда `if found_batch_for_day:`
            # Если всегда должен быть 0, то `else: history_data.append(...)` с 0.0
            else:
                history_data.append(PurchasePriceHistoryDailyResponse(date=current_date_iter, purchase_price=0.0))

            current_date_iter += timedelta(days=1)

        return history_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch purchase price history: {e}")
    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()


@app.get("/api/latest_costs", response_model=LatestCostsResponse)
async def get_latest_costs(vendor_code: str = Query(..., description="Vendor code")):
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        query = """
                SELECT "vendorCode", purchase_price, delivery_to_warehouse,
                       wb_commission_rub, wb_logistics, tax_rub, packaging,
                       fuel, gift, defect_percent, date
                FROM sales
                WHERE "vendorCode" = %s
                ORDER BY date DESC
                LIMIT 1
                """
        df = pd.read_sql_query(query, conn, params=(vendor_code,))
        df.columns = df.columns.str.lower()
        df = df.rename(columns={"vendorcode": "vendor_code"})
        if df.empty:
            raise HTTPException(status_code=404, detail="No data for vendor code")
        row = df.iloc[0]
        return LatestCostsResponse(**row.to_dict())
    finally:
        if conn:
            conn.close()


@app.get("/api/latest_costs_all", response_model=List[LatestCostsResponse])
async def get_latest_costs_all():
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        query = """
                SELECT DISTINCT ON ("vendorCode") "vendorCode", purchase_price, delivery_to_warehouse,
                       wb_commission_rub, wb_logistics, tax_rub, packaging, fuel, gift, defect_percent, date
                FROM sales
                ORDER BY "vendorCode", date DESC
                """
        df = pd.read_sql_query(query, conn)
        df.columns = df.columns.str.lower()
        df = df.rename(columns={"vendorcode": "vendor_code"})
        return df.to_dict(orient="records")
    finally:
        if conn:
            conn.close()


@app.get("/api/missing_costs", response_model=List[MissingCostEntry])
def get_missing_costs(
        start_date: str = Query(..., description="Start date YYYY-MM-DD"),
        end_date: str = Query(..., description="End date YYYY-MM-DD")):
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        query = """
                SELECT "vendorCode", date
                FROM sales
                WHERE date BETWEEN %s AND %s
                  AND (
                        purchase_price IS NULL OR purchase_price = 0 OR
                        delivery_to_warehouse IS NULL OR delivery_to_warehouse = 0 OR
                        wb_commission_rub IS NULL OR wb_commission_rub = 0 OR
                        wb_logistics IS NULL OR wb_logistics = 0 OR
                        tax_rub IS NULL OR tax_rub = 0 OR
                        packaging IS NULL OR packaging = 0 OR
                        fuel IS NULL OR fuel = 0 OR
                        gift IS NULL OR gift = 0
                      )
                ORDER BY "vendorCode", date
                """
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        if df.empty:
            return []
        df.columns = df.columns.str.lower()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        grouped = df.groupby("vendorcode")['date'].apply(lambda x: sorted(x.unique())).reset_index(name='dates')
        grouped = grouped.rename(columns={'vendorcode': 'vendor_code'})
        return grouped.to_dict(orient='records')
    finally:
        if conn:
            conn.close()


# --- НОВАЯ Pydantic модель для создания закупочной партии ---
class PurchaseBatchCreate(BaseModel):
    vendor_code: str = Field(..., description="Артикул товара")
    purchase_price: float = Field(..., gt=0, description="Закупочная цена за единицу")
    quantity_bought: int = Field(..., gt=0, description="Количество товаров в партии")
    start_date: date = Field(..., description="Дата, с которой партия считается активной (формат YYYY-MM-DD)")


class PurchaseBatchResponse(BaseModel):
    vendor_code: str
    purchase_price: float
    quantity_bought: int
    quantity_sold: int
    is_active: bool
    start_date: date
    end_date: Optional[date]


# --- НОВЫЙ Эндпоинт для создания закупочной партии ---
@app.post("/api/purchase_batches")
async def create_purchase_batch(batch: PurchaseBatchCreate):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # Деактивируем предыдущие активные партии для этого vendor_code
        # Все партии, которые были активны до start_date новой партии, деактивируются
        cursor.execute(
            """
            UPDATE purchase_batches
            SET is_active = FALSE,
                end_date  = %s
            WHERE vendor_code = %s
              AND is_active = TRUE
              AND (end_date IS NULL OR end_date >= %s);
            """,
            (batch.start_date, batch.vendor_code, batch.start_date)
        )

        # Вставляем новую партию
        cursor.execute(
            """
            INSERT INTO purchase_batches (vendor_code, purchase_price, quantity_bought, start_date, is_active)
            VALUES (%s, %s, %s, %s, TRUE) RETURNING id;
            """,
            (batch.vendor_code, batch.purchase_price, batch.quantity_bought, batch.start_date.strftime('%Y-%m-%d'))
        )
        batch_id = cursor.fetchone()[0]
        conn.commit()

        send_telegram_notification(
            f"✅ Новая закупочная партия добавлена для <b>{batch.vendor_code}</b>: "
            f"{batch.quantity_bought} шт. по {batch.purchase_price} руб. "
            f"Активна с {batch.start_date.strftime('%Y-%m-%d')}."
        )

        return {"message": "Purchase batch created successfully", "batch_id": batch_id}
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create purchase batch: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.get("/api/purchase_batches", response_model=List[PurchaseBatchResponse])
async def get_purchase_batches(vendor_code: str = Query(..., description="Артикул товара")):
    conn = None
    try:
        conn = psycopg2.connect(DB_URL)
        query = """
                SELECT vendor_code, purchase_price, quantity_bought, quantity_sold,
                       is_active, start_date, end_date
                FROM purchase_batches
                WHERE vendor_code = %s
                ORDER BY start_date DESC
                """
        df = pd.read_sql_query(query, conn, params=(vendor_code,))
        df.columns = df.columns.str.lower()
        return df.to_dict(orient="records")
    finally:
        if conn:
            conn.close()


# --- Модель для обновления других затрат (purchase_price теперь опционален) ---
class CostUpdate(BaseModel):
    vendorcode: str
    start_date: date  # Дата, начиная с которой обновляются расходы
    purchase_price: Optional[float] = None  # Теперь опционально, т.к. может быть задано через партии
    delivery_to_warehouse: Optional[float] = None
    wb_commission_rub: Optional[float] = None
    wb_logistics: Optional[float] = None
    tax_rub: Optional[float] = None
    packaging: Optional[float] = None
    fuel: Optional[float] = None
    gift: Optional[float] = None
    defect_percent: Optional[float] = None


@app.post("/api/update_costs")
def update_costs(update: CostUpdate):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        fields_to_update_sales = []
        values_to_update_sales = []
        fields_to_update_cards = []
        values_to_update_cards = []

        # Список полей, которые могут быть обновлены
        updatable_fields = [
            "purchase_price",
            "delivery_to_warehouse", "wb_commission_rub",
            "wb_logistics", "tax_rub", "packaging", "fuel", "gift", "defect_percent"
        ]

        for field in updatable_fields:
            val = getattr(update, field)
            if val is not None and not (isinstance(val, float) and np.isnan(val)):
                fields_to_update_sales.append(f'"{field}" = %s')
                values_to_update_sales.append(val)
                fields_to_update_cards.append(f'"{field}" = %s')
                values_to_update_cards.append(val)
            elif isinstance(val, float) and np.isnan(val):
                fields_to_update_sales.append(f'"{field}" = %s')
                values_to_update_sales.append(0)  # Отправляем 0 в базу, если на фронте было null
                fields_to_update_cards.append(f'"{field}" = %s')
                values_to_update_cards.append(0)

        updated_rows_sales = 0
        updated_rows_cards = 0

        if fields_to_update_sales:
            # Обновляем таблицу sales
            query_sales_update = f"UPDATE sales SET {', '.join(fields_to_update_sales)} WHERE \"vendorCode\" = %s AND date >= %s"
            cursor.execute(query_sales_update,
                           values_to_update_sales + [update.vendorcode, update.start_date.strftime('%Y-%m-%d')])
            updated_rows_sales = cursor.rowcount

            # Пересчитываем cost_price и total_profit в таблице sales
            cursor.execute(
                """
                UPDATE sales
                SET "cost_price"   = COALESCE("purchase_price", 0) + COALESCE("delivery_to_warehouse", 0) +
                                     COALESCE("wb_commission_rub", 0) + COALESCE("wb_logistics", 0) +
                                     COALESCE("tax_rub", 0) + COALESCE("packaging", 0) +
                                     COALESCE("fuel", 0) + COALESCE("gift", 0) + COALESCE("defect_percent", 0),
                    "total_profit" = (COALESCE("actual_discounted_price", 0) - (
                        COALESCE("purchase_price", 0) + COALESCE("delivery_to_warehouse", 0) +
                        COALESCE("wb_commission_rub", 0) + COALESCE("wb_logistics", 0) +
                        COALESCE("tax_rub", 0) + COALESCE("packaging", 0) +
                        COALESCE("fuel", 0) + COALESCE("gift", 0) + COALESCE("defect_percent", 0)
                        )) * COALESCE("ordersCount", 0) - COALESCE("ad_spend", 0)
                WHERE "vendorCode" = %s
                  AND date >= %s
                """,
                (update.vendorcode, update.start_date.strftime('%Y-%m-%d')),
            )

            # Обновляем базовую таблицу cards для будущих расчётов и согласованности
            if fields_to_update_cards:
                query_cards_update = f"UPDATE cards SET {', '.join(fields_to_update_cards)} WHERE \"vendor_code\" = %s"
                cursor.execute(query_cards_update, values_to_update_cards + [update.vendorcode])
                updated_rows_cards = cursor.rowcount

        conn.commit()
        return {"updated_sales_rows": updated_rows_sales, "updated_cards_rows": updated_rows_cards}
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# --- НОВАЯ логика для проверки остатков партии и отправки уведомлений ---
@app.get("/api/check_purchase_batches")
async def check_purchase_batches():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        query = """
                SELECT pb.id                             AS batch_id, \
                       pb.vendor_code, \
                       pb.purchase_price, \
                       pb.quantity_bought, \
                       COALESCE(SUM(s."ordersCount"), 0) AS total_orders_since_batch_start
                FROM purchase_batches pb \
                         LEFT JOIN \
                     sales s ON pb.vendor_code = s."vendorCode" AND s.date >= pb.start_date
                WHERE pb.is_active = TRUE
                GROUP BY pb.id, pb.vendor_code, pb.purchase_price, pb.quantity_bought
                HAVING pb.quantity_bought <= COALESCE(SUM(s."ordersCount"), 0)
                ; \
                """
        cursor.execute(query)
        batches_to_deactivate = cursor.fetchall()

        notifications_sent = []
        for batch_id, vendor_code, purchase_price, quantity_bought, total_orders_since_batch_start in batches_to_deactivate:
            cursor.execute(
                """
                UPDATE purchase_batches
                SET is_active  = FALSE,
                    end_date   = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s;
                """,
                (date.today().strftime('%Y-%m-%d'), batch_id)
            )
            message = (
                f"🚨 **Уведомление о партии!** 🚨\n\n"
                f"Товар: <b>{vendor_code}</b>\n"
                f"Текущая партия ({quantity_bought} шт. по {purchase_price} руб.) закончилась!\n"
                f"Всего продано из этой партии с {datetime.now().strftime('%Y-%m-%d')}: {total_orders_since_batch_start} шт.\n"
                f"<b>Необходимо добавить новую закупочную цену!</b>"
            )
            send_telegram_notification(message)
            notifications_sent.append(vendor_code)

        conn.commit()
        return {"message": "Batch check completed", "deactivated_batches": notifications_sent}

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to check purchase batches: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

