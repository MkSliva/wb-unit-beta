# Wildberries Dashboard

Простой проект для анализа продаж Wildberries. Состоит из FastAPI backend и React (Vite) frontend.

## Запуск backend
1. Установите Python 3.11.
2. Создайте файл `backend/api.env` на основе `backend/api.env.example` и укажите ваш `WB_API_KEY`.
3. Установите зависимости (пример):
   ```bash
   pip install fastapi pandas uvicorn psycopg2-binary
   ```
4. Запустите PostgreSQL через `docker-compose`:
   ```bash
   docker compose up -d
   ```
5. Запустите сервер:
   ```bash
   uvicorn backend.main:app --reload
   ```

Переменная `DB_URL` указывает строку подключения к PostgreSQL и может быть задана через переменную окружения. По умолчанию используется `postgresql://postgres:postgres@localhost:5432/wildberries`.

## Запуск frontend
1. Требуется Node.js 18+.
2. Установите зависимости:
   ```bash
   cd frontend/wb_unit_auto
   npm install
   ```
   Если после обновления репозитория появились новые зависимости, запустите
   `npm install` повторно, чтобы они установились.
3. Запустите режим разработки:
   ```bash
   npm run dev
   ```

Frontend доступен на `http://localhost:5173`, backend — на `http://localhost:8000`.

## Docker

Для развёртывания PostgreSQL используется `docker-compose.yml` в корне проекта. Запустите:

```bash
docker compose up -d
```

Это создаст контейнер с базой `wildberries` и пользователем `postgres`.
