import requests  # Импортируем библиотеку для HTTP-запросов
from datetime import datetime, timedelta  # Для работы с датами
token = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDkyOTQ5MSwiaWQiOiIwMTk3NDIyNi0zY2VmLTdlMDctODgzNS1iMDlmNzdjMmJmZWMiLCJpaWQiOjIwMDEwODE3LCJvaWQiOjEzODY0NjcsInMiOjEwNzM3NTc5NTAsInNpZCI6IjE5MzhhN2NmLWE5YjMtNDhmOC1hYjUyLWVmMGMzMjU2OWJhYiIsInQiOmZhbHNlLCJ1aWQiOjIwMDEwODE3fQ._hZYYZ6vb3jDb_oAUMSs3nfy7CvgsMNUXy62eai2VSiwj1Q0zcdGoEVJK_nbNMsPAVpu8poWMKcyV8RP0V3x4Q"


def get_yesterdays_sales_data(nm_ids, token):
    """
    Запрашивает данные по продажам за вчерашний день у Wildberries по списку артикулов (nm_ids).
    Возвращает ответ в формате JSON.
    """

    # URL API WB для детализации продаж по артикулу
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"

    # Заголовок с авторизацией
    headers = {"Authorization": token}

    # Получаем вчерашнюю дату в формате YYYY-MM-DD
    yesterday = (datetime.utcnow() - timedelta(days=2)).date().isoformat()
    print(f"📅 Запрашиваем данные за дату: {yesterday}")

    # Формируем тело запроса с нужными параметрами
    payload = {
  "nmIDs": [
    270162488, 396572315
  ],
  "period": {
    "begin": "2025-06-03",
    "end": "2025-06-04"
  },
  "timezone": "Europe/Moscow",
  "aggregationLevel": "day"
}

    print(f"📦 Тело запроса:\n{payload}")

    try:
        print("📡 Отправка запроса к WB...")
        # Выполняем POST-запрос
        print(headers)
        print(payload)
        response = requests.post(url, headers=headers, json=payload)
        print(response)

        # Проверяем статус код. Если ошибка — возбуждаем исключение
        response.raise_for_status()

        print("✅ Данные успешно получены от WB.")
        return response.json()  # Возвращаем данные как JSON
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP ошибка: {http_err} — статус {response.status_code}")
    except Exception as err:
        print(f"❌ Ошибка при выполнении запроса: {err}")

    return None  # Возвращаем None при ошибке


if __name__ == "__main__":
    token = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDkyOTQ5MSwiaWQiOiIwMTk3NDIyNi0zY2VmLTdlMDctODgzNS1iMDlmNzdjMmJmZWMiLCJpaWQiOjIwMDEwODE3LCJvaWQiOjEzODY0NjcsInMiOjEwNzM3NTc5NTAsInNpZCI6IjE5MzhhN2NmLWE5YjMtNDhmOC1hYjUyLWVmMGMzMjU2OWJhYiIsInQiOmZhbHNlLCJ1aWQiOjIwMDEwODE3fQ._hZYYZ6vb3jDb_oAUMSs3nfy7CvgsMNUXy62eai2VSiwj1Q0zcdGoEVJK_nbNMsPAVpu8poWMKcyV8RP0V3x4Q"
    nm_ids = [270162488, 396572315]  # Пример: максимум 20 артикулов за раз

    print(f"🔢 Загружаем данные по {len(nm_ids)} артикулам...")

    result = get_yesterdays_sales_data(nm_ids, token)

    if result:
        print("📊 Ответ от WB:")
        print(result)
    else:
        print("⚠️ Нет данных. Проверьте токен или параметры запроса.")



