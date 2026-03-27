# Easy Stars Bot — Деплой на Railway с PostgreSQL

## Шаги для деплоя

### 1. Загрузи файлы на GitHub
Распакуй архив и залей папку в GitHub репозиторий.

### 2. Создай проект на Railway
1. Зайди на https://railway.com → New Project → Deploy from GitHub
2. Выбери свой репозиторий

### 3. Добавь PostgreSQL базу данных
1. В проекте нажми **"+ New"** → **"Database"** → **"Add PostgreSQL"**
2. Railway автоматически создаст переменную `DATABASE_URL` и подключит её к боту ✅

### 4. Добавь переменные окружения (Variables)

| Переменная | Значение |
|---|---|
| `BOT_TOKEN` | Токен твоего бота от @BotFather |
| `BOT_USERNAME` | Username бота (без @) |
| `ADMIN_IDS` | Твой Telegram ID (можно несколько через запятую) |
| `REFERRAL_BONUS` | 2 |
| `MIN_WITHDRAW` | 15 |
| `MAX_WITHDRAW` | 100 |
| `PENALTY_PERCENT` | 100 |
| `REPAY_PERCENT` | 50 |

> ⚠️ `DATABASE_URL` Railway добавит сам — не трогай её!

### 5. Задеплой
Railway сам установит зависимости и запустит бота. Готово! 🚀
