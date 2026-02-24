Запуск через консоль: uvicorn api.server:app --host 127.0.0.1 --port 8000             

Необходимо добавить .env файл в корень проекта такого формата:

OKX_DEMO=1
OKX_API_KEY=...
OKX_API_SECRET=...
OKX_API_PASSPHRASE=...

Данные вставлять без пробелов! Для получения данных нужно создать api key в демо-режиме аккаунта!

В Astras заменить файл market-settings-config.json на версию из этого проекта
