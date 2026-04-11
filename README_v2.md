# Polymarket Weather Tracker v2

Полностью автоматический трекер. Сам находит рынки, сам определяет
нужную метеостанцию из Rules каждого рынка, берёт прогноз именно
для неё, и сравнивает с котировками.

---

## Установка

```bash
pip install requests schedule
python polymarket_weather_tracker_v2.py
```

Больше ничего не нужно — API-ключи не требуются.

---

## Как это работает (по шагам)

```
Каждые 30 минут:

1. Polymarket API
   └── все активные рынки с "highest temperature"

2. Для каждого рынка → парсит страницу Rules
   └── ищет код станции Wunderground (NZWN, RKSI, EGLL...)
   └── если не нашёл в Rules → берёт из встроенного кэша 25 городов

3. Прогноз ИМЕННО для этой станции
   └── Wunderground прогноз по координатам станции
   └── если заблокирован → Open-Meteo fallback

4. Котировки рынка (Polymarket API)

5. Сравнение:
   └── если |прогноз - рыночный фаворит| > 1.5°C → бумажная ставка

6. Урегулирование вчерашних ставок
   └── Wunderground история → Open-Meteo archive fallback

7. Статистика в лог и БД
```

---

## Почему это важно (станция ≠ город)

Polymarket всегда урегулирует по конкретной метеостанции —
обычно аэропортовой. Аэропорт может быть на 1–3°C холоднее
центра города. Без учёта этого прогноз «для города» даёт
ложные разрывы которых на самом деле нет.

Пример:
- Wellington city прогноз: 20°C
- Wellington Airport (NZWN) факт: 17°C
- Рынок урегулируется по NZWN → 17°C

Трекер v2 берёт прогноз именно для NZWN, а не для города.

---

## Встроенный кэш станций (25 городов)

| Город       | Станция | Аэропорт                  |
|-------------|---------|---------------------------|
| Wellington  | NZWN    | Wellington Intl           |
| Seoul       | RKSI    | Incheon Intl              |
| London      | EGLL    | Heathrow                  |
| Tokyo       | RJTT    | Haneda                    |
| Sydney      | YSSY    | Kingsford Smith           |
| Paris       | LFPG    | Charles de Gaulle         |
| New York    | KJFK    | JFK International         |
| Dubai       | OMDB    | Dubai Intl                |
| Singapore   | WSSS    | Changi                    |
| Hong Kong   | VHHH    | Hong Kong Intl            |
| + ещё 15    | ...     | ...                       |

Новые города добавляются автоматически если найдены в Rules рынка.

---

## Файлы

| Файл                  | Содержимое                        |
|-----------------------|-----------------------------------|
| `polymarket_v2.db`    | Вся база данных (SQLite)          |
| `tracker_v2.log`      | Подробный лог                     |

---

## Просмотр результатов

```bash
sqlite3 polymarket_v2.db
```

```sql
-- Все бумажные ставки
SELECT city, station_code, bet_date, target_temp,
       buy_price, forecast_value, gap_celsius, outcome
FROM paper_bets
ORDER BY placed_at DESC;

-- Только разрешённые
SELECT city, bet_date, target_temp,
       ROUND(forecast_value,1) as forecast,
       ROUND(actual_temp,1) as actual,
       gap_celsius, outcome
FROM paper_bets
WHERE outcome IN ('win','loss')
ORDER BY resolved_at DESC;

-- Процент побед по городам
SELECT city,
       COUNT(*) as total,
       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) as wins,
       ROUND(100.0*SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END)/COUNT(*),1) as win_pct,
       ROUND(AVG(gap_celsius),2) as avg_gap
FROM paper_bets
WHERE outcome IN ('win','loss')
GROUP BY city
ORDER BY win_pct DESC;

-- ROI по порогу разрыва
SELECT ROUND(gap_celsius) as gap_bucket,
       COUNT(*) as n,
       SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) as wins
FROM paper_bets
WHERE outcome IN ('win','loss')
GROUP BY gap_bucket
ORDER BY gap_bucket;

-- Все найденные рынки
SELECT city, market_date, station_code, volume_usd
FROM markets
ORDER BY market_date;
```

---

## Настройки (начало скрипта)

| Параметр       | По умолчанию | Описание                              |
|----------------|--------------|---------------------------------------|
| GAP_THRESHOLD  | 1.5°C        | Минимальный разрыв для ставки         |
| MIN_VOLUME     | $500         | Мин. объём рынка                      |
| SCAN_INTERVAL  | 30 мин       | Интервал сканирования                 |
| DB_PATH        | v2.db        | Путь к базе                           |

---

## Фоновый запуск

**Linux/Mac:**
```bash
nohup python polymarket_weather_tracker_v2.py &
tail -f tracker_v2.log   # смотреть лог
```

**Windows:**
```
pythonw polymarket_weather_tracker_v2.py
```

---

## Когда смотреть на результаты

- **7 дней** — видно работает ли вообще
- **30 дней** — 50–100 ставок, статистически значимо
- **ROI > 0% при 50+ ставках** → можно думать о реальных деньгах

---

*Не финансовый совет. Только бумажные ставки.*
