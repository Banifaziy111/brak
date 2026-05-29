# Деплой на Vercel

## 1. Установите CLI

```bash
npm i -g vercel
```

## 2. Переменные окружения в Vercel

В [vercel.com](https://vercel.com) → проект → **Settings → Environment Variables**:

| Переменная | Пример |
|------------|--------|
| `DB_HOST` | `31.207.77.167` |
| `DB_PORT` | `5432` |
| `DB_NAME` | `botdb` |
| `DB_USER` | `aperepechkin` |
| `DB_PASSWORD` | *(ваш пароль)* |
| `DB_SSLMODE` | `prefer` или `require` (опционально) |

Файл `.env` в репозиторий не попадает — только через панель Vercel.

После деплоя проверьте:
- `https://ваш-проект.vercel.app/health` — приложение запустилось
- `https://ваш-проект.vercel.app/health/db` — связь с PostgreSQL

## 3. PostgreSQL

Сервер БД должен принимать подключения **с интернета** (не только localhost).
При необходимости откройте порт `5432` для IP Vercel или используйте VPN/туннель.

## 4. Деплой из папки проекта

```bash
cd kurkuma
vercel login
vercel
```

Продакшен:

```bash
vercel --prod
```

## 5. Ограничения

- Запросы к БД тяжёлые (4 агрегации). В Vercel → Project → Settings → Functions → **Max Duration** (до 60 с на Pro).
- На Hobby лимит **10 с** — при таймауте фильтруйте один корпус WH.
- Точка входа: `app.py` / `write_offs_dashboard:app` (см. `pyproject.toml`).

## 6. Проверка

После деплоя: `https://ваш-проект.vercel.app/health` → `{"status":"ok"}`
