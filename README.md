# Kurkuma — дашборд брака (write_offs)

HTML-отчёт ТОП-20 по дефектам и категориям из PostgreSQL `brak_team.write_offs`.

## Локальный запуск

```bash
pip install -r requirements.txt
cp .env.example .env   # укажите DB_PASSWORD
python write_offs_dashboard.py
```

Откройте http://127.0.0.1:8080/

## Фильтр WH

Справочник корпусов и блоков — `wh_buildings.json`.

## Деплой Vercel

См. [DEPLOY.md](DEPLOY.md).
