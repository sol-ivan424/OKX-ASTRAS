```md
В Astras заменить файл `market-settings-config.json` на версию из этого проекта.

Необходимо добавить `.env` файл в корень проекта в таком формате:

```env
OKX_DEMO=1
OKX_API_KEY=...
OKX_API_SECRET=...
OKX_API_PASSPHRASE=...
```

Данные вставлять без пробелов.  
Для получения данных нужно создать API key в демо-режиме аккаунта.

Запуск через консоль:

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
uvicorn api.server:app --host 127.0.0.1 --port 8000
```

`venv` не обязателен, но рекомендуется.
```
