# Wildberries Dashboard

Простой проект для анализа продаж Wildberries. Состоит из FastAPI backend и React (Vite) frontend.

## Запуск backend
1. Установите Python 3.11.
2. Создайте файл `backend/api.env` на основе `backend/api.env.example` и укажите ваш `WB_API_KEY`.
3. Установите зависимости (пример):
   ```bash
   pip install fastapi pandas uvicorn
   ```
4. Запустите сервер:
   ```bash
   uvicorn backend.main:app --reload
   ```

Переменная `DB_PATH` указывает путь к SQLite базе и может быть задана через переменную окружения. По умолчанию используется `backend/wildberries_cards.db`.

## Запуск frontend
1. Требуется Node.js 18+.
2. Установите зависимости:
   ```bash
   cd frontend/wb_unit_auto
   npm install
   ```
3. Запустите режим разработки:
   ```bash
   npm run dev
   ```

Frontend доступен на `http://localhost:5173`, backend — на `http://localhost:8000`.
