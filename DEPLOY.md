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

Файл `.env` в репозиторий не попадает — только через панель Vercel.

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

- Запросы к БД тяжёлые (4 агрегации) — на Hobby лимит **10 с** на функцию; в `vercel.json` указано 60 с (нужен **Pro**).
- При таймауте сузьте фильтр WH (один корпус) или перенесите БД ближе к Vercel.

## 6. Проверка

После деплоя: `https://ваш-проект.vercel.app/health` → `{"status":"ok"}`
