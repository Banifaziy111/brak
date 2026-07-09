# Kurkuma — дашборд брака (write_offs)

HTML-отчёт ТОП-20 по дефектам и категориям из PostgreSQL `brak_team.brak_data`
(через VIEW `brak_team.brak_data_norm` и weekly/nm matview).

## Локальный запуск

```bash
pip install -r requirements.txt
cp .env.example .env   # укажите DB_PASSWORD
python write_offs_dashboard.py
# или: python -m brak_dashboard
```

Откройте http://127.0.0.1:8080/

Страницы: `/` дашборд, `/digest` дайджест, `/actions` доска, `/reason` карточка, `/weekly`, `/details`, `/status`, `/nomenclature`.

UI: тёмный beauty-стиль CocosBeauty (чёрный / #F6005E, Montserrat+Oswald) + scroll-reveal анимации — `SHARED_CSS` / `SHARED_JS` в `brak_dashboard/dashboard.py`.

## Тесты

```bash
python -m pytest -q
```

## Фильтр WH

Справочник корпусов и блоков — `wh_buildings.json`.

## Деплой Vercel

См. [DEPLOY.md](DEPLOY.md).
