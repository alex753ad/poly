#!/usr/bin/env python3
"""
Polymarket Weather Gap Tracker v2
===================================
Автоматически:
  1. Сканирует все активные погодные рынки Polymarket
  2. Парсит Rules каждого рынка → извлекает код станции Wunderground
  3. Берёт прогноз ИМЕННО для этой станции (Wunderground → fallback Open-Meteo)
  4. Сравнивает с котировками → фиксирует бумажные ставки
  5. Урегулирует вчерашние ставки по фактическим данным

Запуск:  python polymarket_weather_tracker_v2.py
Режим:   круглосуточно, сканирование каждые 30 минут
"""

import sqlite3
import json
import time
import re
import random
import logging
import os
import sys
from datetime import datetime, date, timedelta, timezone
from typing import Optional

try:
    import requests
except ImportError:
    print("Установите:  pip install requests schedule")
    sys.exit(1)

try:
    import schedule
except ImportError:
    print("Установите:  pip install schedule")
    sys.exit(1)

# ─────────────────────────────────────────────
# НАСТРОЙКИ
# ─────────────────────────────────────────────

# GAP_THRESHOLD больше не используется как единый порог —
# заменён на адаптивный: gap > ADAPTIVE_GAP_MULT × ср.ошибка_станции
ADAPTIVE_GAP_MULT  = 2.0   # ставим только если gap > 2× ср.ошибку прогноза станции
                           # (WR: 86.5% → 96.8%, P&L: -$78 → +$44 на истории)
GAP_THRESHOLD      = 1.5   # запасной порог если станция не в таблице ошибок
MAX_DAYS_AHEAD     = 2     # ставим только если до события ≤2 дня (WR +1% на истории)
MIN_VOLUME         = 500   # минимальный объём рынка $
MIN_SOURCES        = 1     # минимум источников для уверенности
MIN_BET_CONFIDENCE = 0.80  # мин. вероятность нашей стороны (поднят: зона 0.70-0.80 → WR 51%)
MAX_BUY_PRICE      = 0.99  # практически без ограничения: ставки ≥0.95 дают 100% win rate
MAX_OBVIOUS_PROB   = 0.997 # только абсолютно тривиальные сигналы отсекаем
DB_PATH            = os.environ.get("DB_PATH", "/data/polymarket_v2.db")
SCAN_INTERVAL      = 30    # минут между сканированиями
LOG_LEVEL          = logging.INFO

# Средняя ошибка прогноза Open-Meteo по станции (°C) — вычислена по 252 урегулированным ставкам.
# Обновляется автоматически каждые 30 дней функцией update_station_errors().
# Адаптивный порог: gap нужен > ADAPTIVE_GAP_MULT × эта ошибка.
STATION_FORECAST_ERRORS: dict = {
    "KORD": 6.2,  # Chicago O'Hare        n=15
    "EDDM": 5.1,  # Munich                n=10
    "EPWA": 5.1,  # Warsaw                n=5
    "KJFK": 4.3,  # New York JFK          n=17
    "RKSI": 3.9,  # Seoul Incheon         n=18
    "KATL": 3.7,  # Atlanta               n=9
    "LEMD": 3.5,  # Madrid                n=12
    "KSFO": 3.4,  # San Francisco         n=4
    "LFPG": 3.3,  # Paris CDG             n=12
    "NZWN": 2.6,  # Wellington            n=9
    "ZSPD": 2.5,  # Shanghai Pudong       n=12
    "KDFW": 2.5,  # Dallas FW             n=9
    "KDEN": 2.4,  # Denver                n=7
    "EGLL": 2.4,  # London Heathrow       n=14
    "KSEA": 2.4,  # Seattle-Tacoma        n=11
    "ZBAA": 2.2,  # Beijing Capital       n=8
    "VHHH": 2.1,  # Hong Kong             n=11
    "CYYZ": 2.0,  # Toronto Pearson       n=11
    "KLAX": 1.6,  # Los Angeles           n=10
    "SAEZ": 1.0,  # Buenos Aires Ezeiza   n=7
    "RCTP": 1.0,  # Taipei Taoyuan        n=6
    "KMIA": 0.8,  # Miami                 n=9
    "KIAH": 0.8,  # Houston IAH           n=9
    "WSSS": 0.4,  # Singapore Changi      n=8
    "LLBG": 0.4,  # Tel Aviv Ben Gurion   n=4
    "RJTT": 0.3,  # Tokyo Haneda          n=4
}

# Города с систематически плохим прогнозом (убытки на истории).
# С адаптивным порогом большинство из них автоматически получат высокий порог,
# но явный блеклист даёт дополнительную защиту.
BLACKLISTED_CITIES = {
    "Seoul", "Toronto", "Seattle", "Madrid", "Los Angeles", "Shanghai",
}

# ─────────────────────────────────────────────
# ЛОГГЕР
# ─────────────────────────────────────────────

# Определяем директорию для данных (Railway Volume или локальная)
_DATA_DIR = os.path.dirname(os.environ.get("DB_PATH", "polymarket_v2.db"))
if _DATA_DIR and not os.path.exists(_DATA_DIR):
    os.makedirs(_DATA_DIR, exist_ok=True)

_LOG_FILE = os.path.join(_DATA_DIR, "tracker_v2.log") if _DATA_DIR else "tracker_v2.log"

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("tracker_v2")

# ─────────────────────────────────────────────
# КЭШ ПРОГНОЗОВ (сбрасывается каждый скан)
# ─────────────────────────────────────────────

_forecast_cache: dict = {}  # (station_code, date_iso) -> float

def clear_forecast_cache():
    _forecast_cache.clear()

# ─────────────────────────────────────────────
# USER-AGENTS для ротации (анти-блокировка)
# ─────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
]

def random_headers(api_mode: bool = False) -> dict:
    base = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.5",
        # Accept-Encoding намеренно не задаётся — requests добавит его сам
        # и автоматически распакует gzip/br ответ. Если задать вручную,
        # requests отключает авторазархивирование и r.text становится бинарным мусором.
        "DNT": "1",
        "Connection": "keep-alive",
    }
    if api_mode:
        base["Accept"] = "application/json"
        base["Origin"] = "https://polymarket.com"
        base["Referer"] = "https://polymarket.com/"
    else:
        base["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    return base

def safe_get(url: str, params: dict = None, timeout: int = 15,
             retries: int = 3, delay: float = 2.0,
             api_mode: bool = False) -> Optional[requests.Response]:
    """HTTP GET с ретраями и ротацией User-Agent."""
    for attempt in range(retries):
        try:
            headers = random_headers(api_mode=api_mode)
            r = requests.get(url, params=params, headers=headers,
                             timeout=timeout, allow_redirects=True)
            if r.status_code == 429:
                wait = delay * (attempt + 1) * 3
                log.debug("Rate limit %s — ждём %.0fs", url[:50], wait)
                time.sleep(wait)
                continue
            if r.status_code == 200:
                if not r.text or not r.text.strip():
                    log.debug("Пустой ответ от %s (попытка %d)", url[:60], attempt + 1)
                    time.sleep(delay * (attempt + 1))
                    continue
                return r
            log.debug("HTTP %d для %s | body: %s", r.status_code, url[:60], r.text[:120])
            if r.status_code in (403, 503) and attempt < retries - 1:
                time.sleep(delay * (attempt + 2))
                continue
            return None
        except requests.exceptions.Timeout:
            log.debug("Timeout %s (попытка %d)", url[:50], attempt + 1)
        except requests.exceptions.ConnectionError:
            log.debug("ConnError %s (попытка %d)", url[:50], attempt + 1)
        except Exception as e:
            log.debug("Ошибка %s: %s", url[:50], e)
        time.sleep(delay * (attempt + 1))
    return None

def safe_json(r: requests.Response, context: str = "") -> Optional[dict]:
    """Безопасный парсинг JSON с диагностикой."""
    try:
        return r.json()
    except Exception as e:
        snippet = r.text[:200] if r and r.text else "<empty>"
        log.warning("JSON parse error%s: %s | response: %s",
                    f" [{context}]" if context else "", e, snippet)
        return None

# ─────────────────────────────────────────────
# БАЗА ДАННЫХ
# ─────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Рынки найденные на Polymarket
    c.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id       TEXT UNIQUE,
            slug            TEXT,
            question        TEXT,
            city            TEXT,
            market_date     TEXT,
            station_code    TEXT,
            station_lat     REAL,
            station_lon     REAL,
            station_source  TEXT,
            volume_usd      REAL,
            end_date        TEXT,
            discovered_at   TEXT,
            last_updated    TEXT
        )
    """)

    # Прогнозы по станциям
    c.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id     TEXT,
            station_code  TEXT,
            forecast_date TEXT,
            source        TEXT,
            max_temp      REAL,
            fetched_at    TEXT
        )
    """)

    # Котировки
    c.execute("""
        CREATE TABLE IF NOT EXISTS odds (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id    TEXT,
            temp_label   TEXT,
            probability  REAL,
            volume_usd   REAL,
            fetched_at   TEXT
        )
    """)

    # Бумажные ставки
    c.execute("""
        CREATE TABLE IF NOT EXISTS paper_bets (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id        TEXT,
            city             TEXT,
            bet_date         TEXT,
            station_code     TEXT,
            target_temp      TEXT,
            buy_price        REAL,
            forecast_value   REAL,
            market_leader    TEXT,
            leader_price     REAL,
            gap_celsius      REAL,
            placed_at        TEXT,
            actual_temp      REAL,
            resolved_at      TEXT,
            outcome          TEXT DEFAULT 'pending'
        )
    """)

    # Статистика
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            stat_date  TEXT UNIQUE,
            total      INTEGER,
            wins       INTEGER,
            losses     INTEGER,
            pending    INTEGER,
            roi_pct    REAL,
            avg_gap    REAL
        )
    """)

    conn.commit()
    conn.close()
    log.info("БД инициализирована: %s", DB_PATH)

# ─────────────────────────────────────────────
# ШАГ 1: ПОЛУЧАЕМ ВСЕ ПОГОДНЫЕ РЫНКИ POLYMARKET
# ─────────────────────────────────────────────

def fetch_all_weather_markets() -> list[dict]:
    """
    Запрашивает Polymarket Gamma API через Events endpoint.
    Структура: Events → Markets → Outcomes.
    """
    markets = []

    # Метод 1: Events API — правильная иерархия Polymarket
    # Events содержат Markets внутри. Фильтруем по объёму и активности.
    try:
        url = "https://gamma-api.polymarket.com/events"
        offset = 0
        while offset < 500:  # максимум 5 страниц
            params = {
                "active": "true",
                "closed": "false",
                "limit": 100,
                "offset": offset,
                "order": "volume",
                "ascending": "false",
            }
            r = safe_get(url, params=params, api_mode=True)
            if not r:
                break
            data = safe_json(r, "Events API")
            if data is None:
                break
            events = data if isinstance(data, list) else []
            if not events:
                break
            found_any = False
            for ev in events:
                title = (ev.get("title") or ev.get("name") or "").lower()
                if "temperature" in title:
                    found_any = True
                    slug = ev.get("slug", "")
                    for m in ev.get("markets", []):
                        if not m.get("slug"):
                            m["slug"] = slug
                        if not m.get("question"):
                            m["question"] = ev.get("title", "")
                        markets.append(m)
            if not found_any and offset > 0:
                break  # дальше погодных рынков не будет
            offset += 100
            time.sleep(0.3)
    except Exception as e:
        log.warning("Events API: %s", e)

    # Метод 2: Markets API — прямой запрос всех активных рынков
    try:
        url = "https://gamma-api.polymarket.com/markets"
        params = {
            "active": "true",
            "closed": "false",
            "limit": 100,
            "offset": 0,
            "order": "volume",
            "ascending": "false",
        }
        r = safe_get(url, params=params, api_mode=True)
        if r:
            data = safe_json(r, "Markets API")
            if data is not None:
                items = data if isinstance(data, list) else []
                for m in items:
                    q = (m.get("question") or m.get("title") or "").lower()
                    if "temperature" in q:
                        markets.append(m)
                log.debug("Markets API: проверено %d рынков", len(items))
    except Exception as e:
        log.warning("Markets API: %s", e)

    # Метод 3: Events по tag_id=12 (weather на Polymarket)
    try:
        url = "https://gamma-api.polymarket.com/events"
        params = {
            "tag_id": "12",
            "active": "true",
            "limit": 100,
        }
        r = safe_get(url, params=params, api_mode=True)
        if r:
            data = safe_json(r, "Tag API")
            if data is None:
                data = []
            events = data if isinstance(data, list) else []
            for ev in events:
                title = (ev.get("title") or "").lower()
                if "temperature" in title:
                    for m in ev.get("markets", []):
                        if not m.get("slug"):
                            m["slug"] = ev.get("slug", "")
                        if not m.get("question"):
                            m["question"] = ev.get("title", "")
                        markets.append(m)
    except Exception as e:
        log.debug("Tag API: %s", e)

    # Метод 4: поиск по ключевому слову "temperature" через markets endpoint
    if not markets:
        try:
            url = "https://gamma-api.polymarket.com/markets"
            for keyword in ["highest temperature", "temperature"]:
                params = {
                    "active": "true",
                    "closed": "false",
                    "limit": 100,
                    "order": "volume",
                    "ascending": "false",
                    "_q": keyword,
                }
                r = safe_get(url, params=params, api_mode=True)
                if r:
                    data = safe_json(r, f"Search API [{keyword}]")
                    if data:
                        items = data if isinstance(data, list) else data.get("data", [])
                        for m in items:
                            q = (m.get("question") or m.get("title") or "").lower()
                            if "temperature" in q:
                                markets.append(m)
                        if markets:
                            log.debug("Search API нашёл %d рынков по '%s'", len(markets), keyword)
                            break
                time.sleep(0.3)
        except Exception as e:
            log.debug("Search API: %s", e)

    # Дедупликация
    seen = set()
    unique = []
    for m in markets:
        mid = str(m.get("id") or m.get("conditionId") or
                  m.get("condition_id") or m.get("slug") or id(m))
        if mid not in seen:
            seen.add(mid)
            unique.append(m)

    log.info("Найдено %d уникальных погодных рынков", len(unique))
    for i, m in enumerate(unique):
        q = m.get("question") or m.get("title") or ""
        log.debug("  Рынок %d: %r", i+1, q[:140])
    if not unique:
        log.warning("Рынки не найдены — API мог изменить структуру ответа")
    return unique


def extract_market_info(market: dict) -> dict:
    """Извлекает город, дату и целевую температуру из вопроса рынка.

    Формат вопросов Polymarket:
      "Will the highest temperature in CITY be X°C on Month D?"
      "Will the highest temperature in CITY be X°C or higher on Month D?"
      "Will the highest temperature in CITY be between X-Y°F on Month D?"
    """
    question = market.get("question", "")
    slug     = market.get("slug", "")

    MONTHS = {"january":"01","february":"02","march":"03","april":"04",
              "may":"05","june":"06","july":"07","august":"08",
              "september":"09","october":"10","november":"11","december":"12"}

    # Город: "in CITY be"
    city_match = re.search(
        r"highest temperature in\s+([A-Z][a-zA-Z ]+?)\s+be\s+",
        question, re.IGNORECASE
    )
    city = city_match.group(1).strip() if city_match else ""

    # Дата: "on March 24" или "on March 24, 2026" в конце строки
    market_date = None
    dm = re.search(r"\bon\s+(\w+)\s+(\d{1,2})(?:,?\s*(\d{4}))?\s*\??$",
                   question, re.IGNORECASE)
    if dm:
        mon_str = dm.group(1).lower()
        day     = int(dm.group(2))
        year    = int(dm.group(3)) if dm.group(3) else date.today().year
        mon     = MONTHS.get(mon_str)
        if mon:
            market_date = f"{year}-{mon}-{day:02d}"

    # Целевая температура (конвертируем °F → °C)
    target_temp_c: Optional[float] = None
    temp_type = "exact"    # "exact" | "or_higher" | "range"
    target_raw = None      # строка для логов, напр. "13°C" / "16°C+" / "60-61°F"

    def to_c(val: float, unit: str) -> float:
        return round(val if unit.upper() == "C" else (val - 32) * 5 / 9, 1)

    m_range = re.search(
        r"between\s+(\d+\.?\d*)[\u2013\-](\d+\.?\d*)\s*°([CF])",
        question, re.I)
    m_higher = re.search(
        r"be\s+(\d+\.?\d*)\s*°([CF])\s+or\s+higher",
        question, re.I)
    m_exact = re.search(
        r"be\s+(\d+\.?\d*)\s*°([CF])(?:\s+on|\?)",
        question, re.I)

    if m_range:
        lo, hi, unit = float(m_range.group(1)), float(m_range.group(2)), m_range.group(3)
        target_temp_c = to_c((lo + hi) / 2, unit)
        temp_type     = "range"
        target_raw    = f"{lo}-{hi}°{unit.upper()}"
    elif m_higher:
        val, unit = float(m_higher.group(1)), m_higher.group(2)
        target_temp_c = to_c(val, unit)
        temp_type     = "or_higher"
        target_raw    = f"{val}°{unit.upper()}+"
    elif m_exact:
        val, unit = float(m_exact.group(1)), m_exact.group(2)
        target_temp_c = to_c(val, unit)
        temp_type     = "exact"
        target_raw    = f"{val}°{unit.upper()}"

    return {
        "market_id":    market.get("id") or market.get("conditionId", ""),
        "slug":         slug,
        "question":     question,
        "city":         city,
        "market_date":  market_date,
        "volume_usd":   float(market.get("volumeNum", 0) or 0),
        "end_date":     market.get("endDate", ""),
        "target_temp_c": target_temp_c,
        "temp_type":    temp_type,
        "target_raw":   target_raw,
    }

# ─────────────────────────────────────────────
# ШАГ 2: ПАРСИМ RULES → ИЗВЛЕКАЕМ СТАНЦИЮ
# ─────────────────────────────────────────────

# Известные станции (кэш чтобы не парсить каждый раз)
KNOWN_STATIONS = {
    "Wellington":  {"code": "NZWN",  "lat": -41.3272, "lon": 174.8052, "name": "Wellington Intl Airport"},
    "Seoul":       {"code": "RKSI",  "lat": 37.4691,  "lon": 126.4505, "name": "Incheon Intl Airport"},
    "London":      {"code": "EGLL",  "lat": 51.4775,  "lon": -0.4614,  "name": "London Heathrow"},
    "Tokyo":       {"code": "RJTT",  "lat": 35.5494,  "lon": 139.7798, "name": "Tokyo Haneda"},
    "Sydney":      {"code": "YSSY",  "lat": -33.9461, "lon": 151.1772, "name": "Sydney Kingsford Smith"},
    "Paris":       {"code": "LFPG",  "lat": 49.0097,  "lon": 2.5479,   "name": "Paris Charles de Gaulle"},
    "New York":    {"code": "KJFK",  "lat": 40.6413,  "lon": -73.7781, "name": "JFK International"},
    "Dubai":       {"code": "OMDB",  "lat": 25.2528,  "lon": 55.3644,  "name": "Dubai Intl Airport"},
    "Singapore":   {"code": "WSSS",  "lat": 1.3644,   "lon": 103.9915, "name": "Changi Airport"},
    "Hong Kong":   {"code": "VHHH",  "lat": 22.3080,  "lon": 113.9185, "name": "Hong Kong Intl"},
    "Bangkok":     {"code": "VTBS",  "lat": 13.6811,  "lon": 100.7470, "name": "Suvarnabhumi Airport"},
    "Mumbai":      {"code": "VABB",  "lat": 19.0896,  "lon": 72.8656,  "name": "Chhatrapati Shivaji"},
    "Los Angeles": {"code": "KLAX",  "lat": 33.9425,  "lon": -118.4081,"name": "LAX"},
    "Chicago":     {"code": "KORD",  "lat": 41.9742,  "lon": -87.9073, "name": "O'Hare Intl"},
    "Toronto":     {"code": "CYYZ",  "lat": 43.6772,  "lon": -79.6306, "name": "Pearson Intl"},
    "Berlin":      {"code": "EDDB",  "lat": 52.3667,  "lon": 13.5033,  "name": "Berlin Brandenburg"},
    "Madrid":      {"code": "LEMD",  "lat": 40.4719,  "lon": -3.5626,  "name": "Adolfo Suárez Madrid"},
    "Rome":        {"code": "LIRF",  "lat": 41.8003,  "lon": 12.2389,  "name": "Leonardo da Vinci"},
    "Amsterdam":   {"code": "EHAM",  "lat": 52.3105,  "lon": 4.7683,   "name": "Amsterdam Schiphol"},
    "Istanbul":    {"code": "LTFM",  "lat": 41.2753,  "lon": 28.7519,  "name": "Istanbul Airport"},
    "Moscow":      {"code": "UUEE",  "lat": 55.9736,  "lon": 37.4125,  "name": "Sheremetyevo"},
    "Cairo":       {"code": "HECA",  "lat": 30.1219,  "lon": 31.4056,  "name": "Cairo Intl"},
    "Nairobi":     {"code": "HKJK",  "lat": -1.3192,  "lon": 36.9275,  "name": "Jomo Kenyatta Intl"},
    "São Paulo":   {"code": "SBGR",  "lat": -23.4356, "lon": -46.4731, "name": "Guarulhos Intl"},
    "Buenos Aires":{"code": "SAEZ",  "lat": -34.8222, "lon": -58.5358, "name": "Ezeiza Intl"},
    "Mexico City": {"code": "MMMX",  "lat": 19.4363,  "lon": -99.0721, "name": "Benito Juárez Intl"},
    # ── US cities ──
    "Seattle":     {"code": "KSEA",  "lat": 47.4502,  "lon": -122.3088,"name": "Seattle-Tacoma Intl"},
    "Dallas":      {"code": "KDFW",  "lat": 32.8998,  "lon": -97.0403, "name": "Dallas/Fort Worth Intl"},
    "Atlanta":     {"code": "KATL",  "lat": 33.6407,  "lon": -84.4277, "name": "Hartsfield-Jackson"},
    "Miami":       {"code": "KMIA",  "lat": 25.7959,  "lon": -80.2870, "name": "Miami Intl"},
    "Denver":      {"code": "KDEN",  "lat": 39.8561,  "lon": -104.6737,"name": "Denver Intl"},
    "Houston":     {"code": "KIAH",  "lat": 29.9902,  "lon": -95.3368, "name": "George Bush Intercontinental"},
    "Phoenix":     {"code": "KPHX",  "lat": 33.4373,  "lon": -112.0078,"name": "Phoenix Sky Harbor"},
    "Las Vegas":   {"code": "KLAS",  "lat": 36.0840,  "lon": -115.1537,"name": "Harry Reid Intl"},
    "Boston":      {"code": "KBOS",  "lat": 42.3656,  "lon": -71.0096, "name": "Boston Logan"},
    "San Francisco":{"code": "KSFO", "lat": 37.6213,  "lon": -122.3790,"name": "San Francisco Intl"},
    # ── Asia ──
    "Shanghai":    {"code": "ZSPD",  "lat": 31.1443,  "lon": 121.8083, "name": "Pudong Intl"},
    "Taipei":      {"code": "RCTP",  "lat": 25.0777,  "lon": 121.2328, "name": "Taiwan Taoyuan Intl"},
    "Beijing":     {"code": "ZBAA",  "lat": 40.0799,  "lon": 116.6031, "name": "Beijing Capital Intl"},
    "Osaka":       {"code": "RJBB",  "lat": 34.4347,  "lon": 135.2440, "name": "Kansai Intl"},
    "Kuala Lumpur":{"code": "WMKK",  "lat": 2.7456,   "lon": 101.7099, "name": "KL Intl"},
    "Jakarta":     {"code": "WIII",  "lat": -6.1275,  "lon": 106.6537, "name": "Soekarno-Hatta Intl"},
    "Ho Chi Minh": {"code": "VVTS",  "lat": 10.8188,  "lon": 106.6520, "name": "Tan Son Nhat Intl"},
    "Hanoi":       {"code": "VVNB",  "lat": 21.2212,  "lon": 105.8072, "name": "Noi Bai Intl"},
    "Chennai":     {"code": "VOMM",  "lat": 12.9900,  "lon": 80.1693,  "name": "Chennai Intl"},
    "Karachi":     {"code": "OPKC",  "lat": 24.9065,  "lon": 67.1609,  "name": "Jinnah Intl"},
    # ── Europe ──
    "Munich":      {"code": "EDDM",  "lat": 48.3537,  "lon": 11.7750,  "name": "Munich Intl"},
    "Barcelona":   {"code": "LEBL",  "lat": 41.2971,  "lon": 2.0785,   "name": "Barcelona El Prat"},
    "Vienna":      {"code": "LOWW",  "lat": 48.1103,  "lon": 16.5697,  "name": "Vienna Intl"},
    "Warsaw":      {"code": "EPWA",  "lat": 52.1657,  "lon": 20.9671,  "name": "Warsaw Chopin"},
    "Zurich":      {"code": "LSZH",  "lat": 47.4647,  "lon": 8.5492,   "name": "Zürich Airport"},
    "Frankfurt":   {"code": "EDDF",  "lat": 50.0379,  "lon": 8.5622,   "name": "Frankfurt Airport"},
    "Brussels":    {"code": "EBBR",  "lat": 50.9010,  "lon": 4.4844,   "name": "Brussels Airport"},
    "Copenhagen":  {"code": "EKCH",  "lat": 55.6180,  "lon": 12.6508,  "name": "Copenhagen Airport"},
    "Stockholm":   {"code": "ESSA",  "lat": 59.6519,  "lon": 17.9186,  "name": "Stockholm Arlanda"},
    "Oslo":        {"code": "ENGM",  "lat": 60.1976,  "lon": 11.1004,  "name": "Oslo Gardermoen"},
    "Helsinki":    {"code": "EFHK",  "lat": 60.3172,  "lon": 24.9633,  "name": "Helsinki-Vantaa"},
    "Lisbon":      {"code": "LPPT",  "lat": 38.7756,  "lon": -9.1354,  "name": "Lisbon Humberto Delgado"},
    "Athens":      {"code": "LGAV",  "lat": 37.9364,  "lon": 23.9445,  "name": "Athens Intl"},
    "Prague":      {"code": "LKPR",  "lat": 50.1008,  "lon": 14.2600,  "name": "Václav Havel Airport"},
    "Budapest":    {"code": "LHBP",  "lat": 47.4298,  "lon": 19.2610,  "name": "Budapest Ferenc Liszt"},
    # ── Middle East & Africa ──
    "Riyadh":      {"code": "OERK",  "lat": 24.9578,  "lon": 46.6989,  "name": "King Khalid Intl"},
    "Tel Aviv":    {"code": "LLBG",  "lat": 32.0114,  "lon": 34.8867,  "name": "Ben Gurion Intl"},
    "Doha":        {"code": "OTHH",  "lat": 25.2731,  "lon": 51.6081,  "name": "Hamad Intl"},
    "Johannesburg":{"code": "FAJS",  "lat": -26.1392, "lon": 28.2460,  "name": "O.R. Tambo Intl"},
    "Casablanca":  {"code": "GMMN",  "lat": 33.3675,  "lon": -7.5898,  "name": "Mohammed V Intl"},
    # ── Oceania & Americas ──
    "Melbourne":   {"code": "YMML",  "lat": -37.6690, "lon": 144.8410, "name": "Melbourne Airport"},
    "Auckland":    {"code": "NZAA",  "lat": -37.0082, "lon": 174.7917, "name": "Auckland Airport"},
    "Vancouver":   {"code": "CYVR",  "lat": 49.1967,  "lon": -123.1815,"name": "Vancouver Intl"},
    "Bogotá":      {"code": "SKBO",  "lat": 4.7016,   "lon": -74.1469, "name": "El Dorado Intl"},
    "Lima":        {"code": "SPIM",  "lat": -12.0219, "lon": -77.1143, "name": "Jorge Chávez Intl"},
    "Santiago":    {"code": "SCEL",  "lat": -33.3930, "lon": -70.7858, "name": "Arturo Merino Benítez"},
}


def get_adaptive_gap_threshold(station_code: str) -> float:
    """
    Возвращает минимальный gap для ставки на данной станции.
    Формула: ADAPTIVE_GAP_MULT × ср.ошибка_прогноза_станции.
    Если станция не в таблице — используем GAP_THRESHOLD как запасной.
    """
    err = STATION_FORECAST_ERRORS.get(station_code)
    if err is None:
        return GAP_THRESHOLD
    return round(ADAPTIVE_GAP_MULT * err, 2)


def update_station_errors():
    """
    Пересчитывает среднюю ошибку прогноза по каждой станции из БД.
    Запускается автоматически раз в 30 дней. Обновляет STATION_FORECAST_ERRORS в памяти.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT station_code,
                   AVG(ABS(forecast_value - actual_temp)) as avg_err,
                   COUNT(*) as n
            FROM paper_bets
            WHERE outcome IN ('win','loss')
              AND actual_temp IS NOT NULL
              AND forecast_value IS NOT NULL
            GROUP BY station_code
            HAVING n >= 3
        """)
        rows = c.fetchall()
        conn.close()
        updated = 0
        for station_code, avg_err, n in rows:
            if avg_err is not None:
                old = STATION_FORECAST_ERRORS.get(station_code, None)
                STATION_FORECAST_ERRORS[station_code] = round(avg_err, 2)
                if old != round(avg_err, 2):
                    log.debug("Ошибка станции %s: %.2f°C (n=%d)", station_code, avg_err, n)
                updated += 1
        log.info("📐 Таблица ошибок станций обновлена: %d станций", updated)
    except Exception as e:
        log.warning("Не удалось обновить ошибки станций: %s", e)


def fetch_market_rules(slug: str) -> Optional[str]:
    """Парсит страницу рынка Polymarket и извлекает текст Rules."""
    url = f"https://polymarket.com/event/{slug}"
    r = safe_get(url, timeout=20)
    if not r:
        return None
    return r.text


def extract_station_from_rules(rules_text: str) -> Optional[str]:
    """
    Извлекает код станции Wunderground из текста правил рынка.
    Паттерны: 'NZWN', 'station EGLL', 'ICAO: RKSI', 'wunderground.com/.../NZWN'
    """
    if not rules_text:
        return None

    patterns = [
        # Прямое упоминание ICAO кода (4 заглавные буквы)
        r'\b([A-Z]{4})\b.*?(?:station|airport|wunderground)',
        r'(?:station|airport|ICAO)[:\s]+([A-Z]{4})\b',
        r'wunderground\.com/[^\s"\']*?/([A-Z]{4})\b',
        # В URL или JSON внутри страницы
        r'"stationId"\s*:\s*"([A-Z]{4})"',
        r'station[_\s]?id["\s:]+([A-Z]{4})',
        r'\b([A-Z]{3,4})\b(?=.*?wunderground)',
    ]

    for pat in patterns:
        m = re.search(pat, rules_text, re.IGNORECASE)
        if m:
            code = m.group(1).upper()
            # Исключаем общие слова которые могут совпасть
            exclude = {"HTTP", "HTML", "JSON", "TRUE", "NULL", "FROM", "WITH",
                       "THIS", "THAT", "WILL", "WHEN", "THAN", "THEN", "ALSO"}
            if code not in exclude and len(code) >= 3:
                log.debug("Найдена станция из rules: %s", code)
                return code
    return None


def get_station_for_market(market_info: dict, rules_text: Optional[str]) -> Optional[dict]:
    """
    Определяет станцию для рынка:
    1. Парсит из rules_text
    2. Ищет в кэше KNOWN_STATIONS по городу
    3. Возвращает None если не нашли
    """
    # Пробуем извлечь из правил
    if rules_text:
        code = extract_station_from_rules(rules_text)
        if code:
            # Ищем координаты в кэше
            for city, st in KNOWN_STATIONS.items():
                if st["code"] == code:
                    return {**st, "source": "parsed_from_rules"}
            # Если кода нет в кэше — возвращаем хотя бы код
            return {"code": code, "lat": None, "lon": None,
                    "name": code, "source": "parsed_from_rules"}

    # Fallback: кэш по названию города
    city = market_info.get("city", "")
    for known_city, station in KNOWN_STATIONS.items():
        if known_city.lower() in city.lower() or city.lower() in known_city.lower():
            log.debug("Станция из кэша для %s: %s", city, station["code"])
            return {**station, "source": "known_cache"}

    log.debug("Станция не найдена для города: %s", city)
    return None


def get_station_coordinates(station_code: str) -> Optional[tuple[float, float]]:
    """
    Получает координаты станции через Wunderground или кэш.
    Возвращает (lat, lon) или None.
    """
    # Сначала кэш
    for city, st in KNOWN_STATIONS.items():
        if st["code"] == station_code:
            return st["lat"], st["lon"]

    # Пробуем Wunderground API (без ключа, через веб)
    url = f"https://api.weather.com/v3/location/search"
    params = {"query": station_code, "language": "en-US", "format": "json"}
    r = safe_get(url, params=params, timeout=10)
    if r:
        data = safe_json(r, "weather.com location")
        if data:
            locs = data.get("location", {})
            lats = locs.get("latitude", [])
            lons = locs.get("longitude", [])
            if lats and lons:
                return float(lats[0]), float(lons[0])
    return None

# ─────────────────────────────────────────────
# ШАГ 3: ПРОГНОЗ ПО СТАНЦИИ
# ─────────────────────────────────────────────

def fetch_wunderground_forecast(station_code: str, lat: float, lon: float,
                                 target_date: date) -> Optional[float]:
    """
    Парсит страницу Wunderground для конкретной станции.
    Ищет прогноз максимальной температуры на target_date.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    urls_to_try = [
        f"https://www.wunderground.com/forecast/{lat:.4f},{lon:.4f}",
        f"https://www.wunderground.com/weather/{station_code}",
    ]

    for url in urls_to_try:
        r = safe_get(url, timeout=20)
        if not r:
            continue

        text = r.text

        # Ищем JSON с прогнозами в теге <script>
        json_patterns = [
            r'"summary"\s*:\s*(\{[^{}]*"high"[^{}]*\})',
            r'"forecasts"\s*:\s*(\[.*?\])',
            r'dailyForecast["\s:]+(\[.*?\])',
            r'"temperature"\s*:\s*\{[^}]*"max"\s*:\s*(\d+\.?\d*)',
        ]

        for pat in json_patterns:
            matches = re.findall(pat, text, re.DOTALL)
            for match in matches[:3]:
                try:
                    if match.startswith("{"):
                        data = json.loads(match)
                        temp = (data.get("high") or data.get("max") or
                                data.get("temperature", {}).get("max"))
                        if temp:
                            return _f_to_c(float(temp))
                except Exception:
                    pass

        # Ищем температуру напрямую в HTML
        # Паттерн: число °F рядом с "High" или "Max"
        temp_patterns = [
            r'(?:High|Max|high|max)[^\d]{0,20}(\d{1,3})\s*°?\s*F',
            r'(\d{1,3})\s*°?\s*F[^\d]{0,20}(?:High|Max)',
            r'"tempHigh"\s*:\s*(\d+\.?\d*)',
            r'"maxTemp"\s*:\s*(\d+\.?\d*)',
            r'data-temp-high="(\d+\.?\d*)"',
        ]
        for pat in temp_patterns:
            m = re.search(pat, text)
            if m:
                temp_f = float(m.group(1))
                if 20 < temp_f < 130:  # валидный диапазон °F
                    return _f_to_c(temp_f)

        time.sleep(1.5)  # пауза между запросами

    return None


def fetch_wunderground_history(station_code: str, target_date: date) -> Optional[float]:
    """
    Получает фактическую максимальную температуру из истории Wunderground.
    Используется для урегулирования ставок.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    url = f"https://www.wunderground.com/history/daily/{station_code}/date/{date_str}"

    r = safe_get(url, timeout=20)
    if not r:
        return None

    text = r.text

    # Паттерны для фактических данных
    patterns = [
        r'"actualMaxTemp"\s*:\s*(\d+\.?\d*)',
        r'"maxTemp"\s*:\s*(\d+\.?\d*)',
        r'(?:Max|High)\s+Temp[^\d]{0,30}(\d{1,3})\s*°?\s*[FC]',
        r'data-maxtemp="(\d+\.?\d*)"',
        r'"temperature_max"\s*:\s*(\d+\.?\d*)',
    ]

    for pat in patterns:
        m = re.search(pat, text)
        if m:
            temp = float(m.group(1))
            # Определяем единицы (если > 50 — скорее всего °F)
            if temp > 50:
                return _f_to_c(temp)
            return temp  # уже Celsius

    return None


def fetch_openmeteo_forecast(lat: float, lon: float, target_date: date) -> Optional[float]:
    """Fallback: Open-Meteo по координатам станции."""
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat, "longitude": lon,
            "daily": "temperature_2m_max",
            "timezone": "auto",
            "forecast_days": 7,
        }
        r = safe_get(url, params=params, timeout=10)
        if not r:
            return None
        data = safe_json(r, "Open-Meteo forecast")
        if not data:
            return None
        target_str = target_date.isoformat()
        if target_str in dates:
            idx = dates.index(target_str)
            return round(temps[idx], 1)
    except Exception as e:
        log.debug("Open-Meteo fallback: %s", e)
    return None


def fetch_openmeteo_history(lat: float, lon: float, target_date: date) -> Optional[float]:
    """Фактическая температура из Open-Meteo archive."""
    try:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat, "longitude": lon,
            "start_date": target_date.isoformat(),
            "end_date":   target_date.isoformat(),
            "daily": "temperature_2m_max",
            "timezone": "auto",
        }
        r = safe_get(url, params=params, timeout=10)
        if not r:
            return None
        data = safe_json(r, "Open-Meteo history")
        if not data:
            return None
        return round(temps[0], 1) if temps else None
    except Exception as e:
        log.debug("Open-Meteo history: %s", e)
    return None


def get_forecast_for_station(station: dict, target_date: date) -> Optional[float]:
    """Возвращает прогноз с кэшированием внутри одного скана."""
    cache_key = (station.get("code",""), target_date.isoformat())
    if cache_key in _forecast_cache:
        return _forecast_cache[cache_key]

    result = _get_forecast_uncached(station, target_date)
    if result is not None:
        _forecast_cache[cache_key] = result
    return result


def _get_forecast_uncached(station: dict, target_date: date) -> Optional[float]:
    """
    Главная функция прогноза:
    1. Пробует Wunderground
    2. Fallback на Open-Meteo
    """
    code = station.get("code", "")
    lat  = station.get("lat")
    lon  = station.get("lon")

    temp = None

    # Wunderground (если есть координаты)
    if lat and lon:
        log.debug("  Wunderground %s (%.4f, %.4f)...", code, lat, lon)
        temp = fetch_wunderground_forecast(code, lat, lon, target_date)
        if temp is not None:
            log.debug("  ✓ Wunderground: %.1f°C", temp)
            return temp

    # Fallback: Open-Meteo
    if lat and lon:
        log.debug("  Open-Meteo fallback %s...", code)
        temp = fetch_openmeteo_forecast(lat, lon, target_date)
        if temp is not None:
            log.debug("  ✓ Open-Meteo: %.1f°C", temp)
            return temp

    return None


def _f_to_c(f: float) -> float:
    return round((f - 32) * 5 / 9, 1)

# ─────────────────────────────────────────────
# ШАГ 4: КОТИРОВКИ POLYMARKET
# ─────────────────────────────────────────────

def fetch_market_odds(market_id: str) -> list[dict]:
    """Получает актуальные котировки для рынка."""
    url = f"https://gamma-api.polymarket.com/markets/{market_id}"
    r = safe_get(url, timeout=15, api_mode=True)
    if not r:
        return []
    try:
        m = safe_json(r, f"market_odds/{market_id}")
        if not m:
            return []
        outcomes = m.get("outcomes", "[]")
        prices   = m.get("outcomePrices", "[]")
        volume   = float(m.get("volumeNum", 0) or 0)

        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices, str):
            prices = json.loads(prices)

        result = []
        for i, label in enumerate(outcomes):
            prob = float(prices[i]) if i < len(prices) else 0.0
            result.append({
                "temp_label":  str(label),
                "probability": round(prob, 4),
                "volume_usd":  round(volume, 2),
            })
        return result
    except Exception as e:
        log.debug("Котировки %s: %s", market_id, e)
        return []

# ─────────────────────────────────────────────
# ШАГ 5: ПОИСК РАЗРЫВА
# ─────────────────────────────────────────────

def parse_temp_label(label: str) -> Optional[float]:
    label = label.replace("°C","").replace("°","").strip()
    m = re.search(r"-?\d+(\.\d+)?", label)
    return float(m.group()) if m else None


def find_gap(forecast_temp: float, odds: list[dict]) -> Optional[dict]:
    """Ищет разрыв между прогнозом и рыночным фаворитом."""
    if not odds:
        return None

    leader = max(odds, key=lambda x: x["probability"])
    leader_temp = parse_temp_label(leader["temp_label"])
    if leader_temp is None:
        return None

    gap = abs(forecast_temp - leader_temp)
    if gap < GAP_THRESHOLD:
        return None

    # Находим вариант ближайший к прогнозу
    best = min(
        [o for o in odds if parse_temp_label(o["temp_label"]) is not None],
        key=lambda o: abs(parse_temp_label(o["temp_label"]) - forecast_temp),
        default=None
    )
    if not best:
        return None

    return {
        "target_label": best["temp_label"],
        "target_temp":  parse_temp_label(best["temp_label"]),
        "buy_price":    best["probability"],
        "leader_label": leader["temp_label"],
        "leader_temp":  leader_temp,
        "leader_price": leader["probability"],
        "gap":          round(gap, 2),
    }


def find_gap_binary(forecast_temp: float,
                    target_temp_c: Optional[float],
                    temp_type: str,
                    odds: list[dict],
                    min_gap: float = GAP_THRESHOLD) -> Optional[dict]:
    """
    Для YES/NO рынков: сравнивает прогноз с целевой температурой из вопроса.

    Возвращает словарь совместимый с find_gap, плюс поле bet_side.
    target_label кодирует сторону ставки: "YES:13.0°C (exact)"
    """
    if target_temp_c is None or not odds:
        return None

    # Находим вероятности YES и NO
    yes_prob: Optional[float] = None
    no_prob:  Optional[float] = None
    for o in odds:
        lbl = o["temp_label"].strip().upper()
        if lbl in ("YES", "ДА", "Y"):
            yes_prob = o["probability"]
        elif lbl in ("NO", "НЕТ", "N"):
            no_prob = o["probability"]

    if yes_prob is None:
        return None          # не бинарный рынок — fallback на find_gap
    if no_prob is None:
        no_prob = round(1.0 - yes_prob, 4)

    # Что предсказывает прогноз?
    if temp_type == "or_higher":
        forecast_says_yes = forecast_temp >= target_temp_c
        gap = abs(forecast_temp - target_temp_c)
    elif temp_type == "range":
        # YES = temp попадает в диапазон (±0.6°C от середины)
        forecast_says_yes = abs(forecast_temp - target_temp_c) <= 0.6
        gap = abs(forecast_temp - target_temp_c)
    else:  # exact
        forecast_says_yes = abs(forecast_temp - target_temp_c) <= 0.5
        gap = abs(forecast_temp - target_temp_c)

    if gap < min_gap:
        return None

    bet_side  = "YES" if forecast_says_yes else "NO"
    buy_price = yes_prob if forecast_says_yes else no_prob

    label = f"{bet_side}:{target_temp_c}°C ({temp_type})"

    return {
        "target_label": label,
        "target_temp":  target_temp_c,
        "buy_price":    buy_price,
        "bet_side":     bet_side,
        "leader_label": "YES" if yes_prob >= no_prob else "NO",
        "leader_temp":  target_temp_c,
        "leader_price": max(yes_prob, no_prob),
        "gap":          round(gap, 2),
    }

# ─────────────────────────────────────────────
# ШАГ 6: УРЕГУЛИРОВАНИЕ
# ─────────────────────────────────────────────

def resolve_pending_bets():
    """Урегулирует все pending ставки с прошедшей датой."""
    # Берём ВСЕ просроченные pending (не только вчерашние — на случай пропуска)
    today = date.today()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT id, market_id, bet_date, target_temp, station_code, city
        FROM paper_bets
        WHERE outcome = 'pending' AND bet_date < ?
        ORDER BY bet_date
    """, (today.isoformat(),))
    pending = c.fetchall()
    conn.close()

    if not pending:
        log.info("  Нет просроченных ставок для урегулирования")
        return

    log.info("  Урегулируем %d ставок...", len(pending))
    resolved_count = 0
    no_data_count  = 0

    for bet_id, market_id, bet_date_str, target_label, station_code, city in pending:
        bet_date = date.fromisoformat(bet_date_str)

        # Ищем координаты станции — сначала по коду в кэше
        lat, lon = None, None
        for st_city, st in KNOWN_STATIONS.items():
            if st["code"] == station_code:
                lat, lon = st["lat"], st["lon"]
                break
        # Если не нашли по коду — пробуем по городу
        if lat is None and city in KNOWN_STATIONS:
            lat = KNOWN_STATIONS[city]["lat"]
            lon = KNOWN_STATIONS[city]["lon"]

        # Фактическая температура: Wunderground → Open-Meteo archive
        actual = None
        if station_code:
            actual = fetch_wunderground_history(station_code, bet_date)
        if actual is None and lat and lon:
            actual = fetch_openmeteo_history(lat, lon, bet_date)

        if actual is None:
            no_data_count += 1
            log.info("  ⏳ Нет данных: %s %s %s (попробуем позже)", city, station_code, bet_date_str)
            continue

        actual_rounded = round(actual)
        target_val = parse_temp_label(target_label)

        if target_val is not None:
            # Новый формат: "YES:13.0°C (exact)" или "NO:16.0°C (or_higher)"
            bet_side = "YES"
            clean_label = target_label
            if ":" in target_label and target_label[:3] in ("YES", "NO:"):
                parts = target_label.split(":", 1)
                bet_side    = parts[0].upper()
                clean_label = parts[1] if len(parts) > 1 else target_label

            if "or_higher" in clean_label or ("higher" in clean_label and "+" not in clean_label):
                condition_met = actual >= target_val
            elif "range" in clean_label:
                condition_met = abs(actual - target_val) <= 0.6
            elif "+" in clean_label:
                condition_met = actual >= target_val
            else:
                condition_met = abs(actual - target_val) <= 0.6

            # Выигрываем если наш прогноз оказался верным
            if bet_side == "YES":
                outcome = "win" if condition_met else "loss"
            else:
                outcome = "win" if not condition_met else "loss"
        else:
            outcome = "unknown"

        conn = sqlite3.connect(DB_PATH)
        c2 = conn.cursor()
        c2.execute("""
            UPDATE paper_bets
            SET actual_temp = ?, resolved_at = ?, outcome = ?
            WHERE id = ?
        """, (actual, datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), outcome, bet_id))
        conn.commit()
        conn.close()

        resolved_count += 1
        icon = "✅" if outcome == "win" else "❌"
        log.info("  %s %-15s %s  цель=%-25s  факт=%.1f°C → %s",
                 icon, city, bet_date_str, target_label, actual, outcome.upper())

    log.info("  Итого урегулировано: %d  |  Нет данных: %d", resolved_count, no_data_count)

# ─────────────────────────────────────────────
# СТАТИСТИКА
# ─────────────────────────────────────────────

def print_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM paper_bets")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM paper_bets WHERE outcome='win'")
    wins = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM paper_bets WHERE outcome='loss'")
    losses = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM paper_bets WHERE outcome='pending'")
    pending = c.fetchone()[0]
    c.execute("SELECT buy_price, outcome FROM paper_bets WHERE outcome IN ('win','loss')")
    resolved = c.fetchall()
    conn.close()

    stake = 10.0
    pnl = sum(
        (stake / p - stake) if o == "win" else -stake
        for p, o in resolved
    )
    roi = (pnl / max(len(resolved) * stake, 1)) * 100

    conn2 = sqlite3.connect(DB_PATH)
    c2 = conn2.cursor()
    c2.execute("SELECT AVG(gap_celsius) FROM paper_bets")
    avg_gap = c2.fetchone()[0] or 0.0

    # По городам
    c2.execute("""
        SELECT city,
               COUNT(*) as n,
               SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) as w,
               ROUND(AVG(gap_celsius),2) as ag
        FROM paper_bets
        WHERE outcome IN ('win','loss')
        GROUP BY city
        ORDER BY n DESC
        LIMIT 10
    """)
    by_city = c2.fetchall()
    conn2.close()

    log.info("=" * 60)
    log.info("📊 СТАТИСТИКА  |  %s", date.today())
    log.info("   Всего: %d  |  Ожидают: %d", total, pending)
    log.info("   Wins:  %d  |  Losses: %d", wins, losses)
    log.info("   P&L:   %+.2f$ (условная ставка $%.0f)", pnl, stake)
    log.info("   ROI:   %+.1f%%  |  Ср.разрыв: %.2f°C", roi, avg_gap)
    if by_city:
        log.info("   По городам (урегулированные):")
        for city, n, w, ag in by_city:
            wr = (w / n * 100) if n else 0
            log.info("     %-15s  %2d ставок  %2d побед  %.0f%%  разрыв %.1f°C",
                     city, n, w, wr, ag or 0)
    # Pending по городам
    conn_p = sqlite3.connect(DB_PATH)
    cp = conn_p.cursor()
    cp.execute("""
        SELECT city, COUNT(*) as n, ROUND(AVG(gap_celsius),1) as ag
        FROM paper_bets WHERE outcome='pending'
        GROUP BY city ORDER BY n DESC LIMIT 10
    """)
    pending_cities = cp.fetchall()
    conn_p.close()
    if pending_cities:
        log.info("   Ожидают урегулирования:")
        for city, n, ag in pending_cities:
            log.info("     %-15s  %2d ставок  разрыв %.1f°C", city, n, ag or 0)
    log.info("=" * 60)

    conn3 = sqlite3.connect(DB_PATH)
    c3 = conn3.cursor()
    c3.execute("""
        INSERT OR REPLACE INTO daily_stats (stat_date, total, wins, losses, pending, roi_pct, avg_gap)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (date.today().isoformat(), total, wins, losses, pending,
          round(roi, 2), round(avg_gap, 2)))
    conn3.commit()
    conn3.close()

# ─────────────────────────────────────────────
# ГЛАВНЫЙ ЦИКЛ
# ─────────────────────────────────────────────

def scan():
    clear_forecast_cache()   # сбрасываем кэш прогнозов перед новым сканом
    log.info("━" * 60)
    log.info("🔍 СКАНИРОВАНИЕ  %s UTC", datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M"))

    # 1. Получаем все погодные рынки
    raw_markets = fetch_all_weather_markets()
    if not raw_markets:
        log.warning("Рынки не найдены — проверьте интернет-соединение")
        return

    bets_placed = 0

    for raw in raw_markets:
        info = extract_market_info(raw)
        city        = info["city"]
        market_id   = info["market_id"]
        market_date = info["market_date"]
        volume      = info["volume_usd"]

        if not city or not market_date:
            log.debug("SKIP no city/date: city=%r date=%r q=%r",
                      city, market_date, info.get("question","")[:120])
            continue

        # Пропускаем города с систематически плохим прогнозом
        if city in BLACKLISTED_CITIES:
            log.debug("SKIP blacklisted city: %s", city)
            continue

        # Пропускаем прошедшие даты и слишком далёкие (>MAX_DAYS_AHEAD)
        try:
            mdate = date.fromisoformat(market_date)
        except Exception as e:
            log.debug("SKIP bad date %r: %s | q=%r", market_date, e, info.get("question","")[:80])
            continue
        if mdate < date.today():
            log.debug("SKIP past date %s: %s", market_date, city)
            continue
        days_ahead = (mdate - date.today()).days
        if days_ahead > MAX_DAYS_AHEAD:
            log.debug("SKIP too far ahead (%d дн.) для %s %s", days_ahead, city, market_date)
            continue

        if volume < MIN_VOLUME:
            log.debug("Мало объёма $%.0f для %s %s", volume, city, market_date)
            continue

        log.info("📍 %s %s  ($%.0f)", city, market_date, volume)

        # 2. Парсим Rules → станция
        rules_text = fetch_market_rules(info["slug"])
        station = get_station_for_market(info, rules_text)

        if not station:
            log.info("  ⚠️  Станция не определена — пропускаем")
            continue

        log.info("  📡 Станция: %s (%s)", station["code"], station.get("name",""))

        # Сохраняем рынок в БД
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO markets
              (market_id, slug, question, city, market_date,
               station_code, station_lat, station_lon, station_source,
               volume_usd, end_date, discovered_at, last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,
              COALESCE((SELECT discovered_at FROM markets WHERE market_id=?), ?),
              ?)
        """, (market_id, info["slug"], info["question"], city, market_date,
              station["code"], station.get("lat"), station.get("lon"),
              station.get("source",""),
              volume, info["end_date"],
              market_id, datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
              datetime.now(timezone.utc).replace(tzinfo=None).isoformat()))
        conn.commit()
        conn.close()

        # 3. Прогноз по станции
        mdate_obj = date.fromisoformat(market_date)
        forecast_temp = get_forecast_for_station(station, mdate_obj)

        if forecast_temp is None:
            log.info("  ⚠️  Прогноз недоступен")
            continue

        log.info("  🌡  Прогноз станции: %.1f°C", forecast_temp)

        # Сохраняем прогноз
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        source = "wunderground" if station.get("lat") else "open_meteo"
        c.execute("""
            INSERT INTO forecasts (market_id, station_code, forecast_date, source, max_temp, fetched_at)
            VALUES (?,?,?,?,?,?)
        """, (market_id, station["code"], market_date, source,
              forecast_temp, datetime.now(timezone.utc).replace(tzinfo=None).isoformat()))
        conn.commit()
        conn.close()

        # 4. Котировки
        odds = fetch_market_odds(market_id)
        if not odds:
            log.info("  ⚠️  Котировки недоступны")
            continue

        # Сохраняем котировки
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        for o in odds:
            c.execute("""
                INSERT INTO odds (market_id, temp_label, probability, volume_usd, fetched_at)
                VALUES (?,?,?,?,?)
            """, (market_id, o["temp_label"], o["probability"], o["volume_usd"], now))
        conn.commit()
        conn.close()

        # 5. Ищем разрыв с адаптивным порогом для данной станции
        station_code_for_gap = station.get("code", "")
        adaptive_threshold   = get_adaptive_gap_threshold(station_code_for_gap)
        target_temp_c = info.get("target_temp_c")
        temp_type     = info.get("temp_type", "exact")
        gap_info = find_gap_binary(forecast_temp, target_temp_c, temp_type, odds,
                                   min_gap=adaptive_threshold)
        if gap_info is None:
            gap_info = find_gap(forecast_temp, odds)
        # Доп. проверка: gap должен превышать адаптивный порог
        if gap_info and gap_info["gap"] < adaptive_threshold:
            log.info("  ⏭  Пропуск: gap=%.1f°C < адаптивный порог %.1f°C (станция %s, ошибка прогноза ~%.1f°C)",
                     gap_info["gap"], adaptive_threshold, station_code_for_gap,
                     STATION_FORECAST_ERRORS.get(station_code_for_gap, adaptive_threshold/ADAPTIVE_GAP_MULT))
            continue

        if not gap_info:
            leader = max(odds, key=lambda x: x["probability"])
            target_c = info.get("target_temp_c")
            target_str = (f"{target_c:.1f}°C ({info.get('temp_type','?')})"
                          if target_c is not None else info.get("target_raw") or "?")
            log.info("  ✓ Разрыва нет. Лидер: %s (%.0f%%) | Прогноз %.1f°C vs цель %s (Δ=%.1f°C)",
                     leader["temp_label"], leader["probability"] * 100,
                     forecast_temp, target_str,
                     abs(forecast_temp - target_c) if target_c is not None else 0)
            continue

        log.info("  ⚡ РАЗРЫВ! Прогноз %.1f°C | Цель %s | Ставка: %s (%.0f%%) | Δ=%.1f°C (порог %.1f°C)",
                 forecast_temp,
                 info.get("target_raw") or gap_info.get("target_label"),
                 gap_info["target_label"], gap_info["buy_price"] * 100,
                 gap_info["gap"], adaptive_threshold)

        # Фильтр качества сигнала
        # 1. Не ставим если наша вероятность покупки слишком низкая
        if gap_info["buy_price"] < MIN_BET_CONFIDENCE:
            log.info("  ⏭  Пропуск: buy_price=%.3f < мин. %.2f (слишком неуверенно)",
                     gap_info["buy_price"], MIN_BET_CONFIDENCE)
            continue
        # 2. Отсекаем только полностью тривиальные ставки (buy_price > MAX_BUY_PRICE=0.99)
        if gap_info["buy_price"] > MAX_BUY_PRICE:
            log.debug("  ⏭  Пропуск: buy_price=%.3f > макс. %.2f", gap_info["buy_price"], MAX_BUY_PRICE)
            continue
        # 2. Не ставим если рынок сам уже оценивает целевой исход как почти невозможный
        # (нет реального инф. преимущества — рынок согласен с нами)
        target_market_prob = None
        for o in odds:
            lbl = o["temp_label"].strip().upper()
            if gap_info["target_label"].startswith("NO") and lbl in ("NO","НЕТ"):
                target_market_prob = o["probability"]
            elif gap_info["target_label"].startswith("YES") and lbl in ("YES","ДА"):
                target_market_prob = o["probability"]
        if target_market_prob is not None and target_market_prob > MAX_OBVIOUS_PROB:
            log.info("  ⏭  Пропуск: рынок уже ценит нашу сторону в %.0f%% (нет преимущества)",
                     target_market_prob * 100)
            continue

        # Проверяем не делали ли уже ставку на этот рынок
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT id FROM paper_bets
            WHERE market_id=? AND outcome='pending'
        """, (market_id,))
        existing = c.fetchone()
        conn.close()

        if existing:
            log.debug("  Ставка уже есть для %s", market_id)
            continue

        # Сохраняем бумажную ставку
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT INTO paper_bets
              (market_id, city, bet_date, station_code,
               target_temp, buy_price, forecast_value,
               market_leader, leader_price, gap_celsius, placed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (market_id, city, market_date, station["code"],
              gap_info["target_label"], gap_info["buy_price"],
              forecast_temp,
              gap_info["leader_label"], gap_info["leader_price"],
              gap_info["gap"],
              datetime.now(timezone.utc).replace(tzinfo=None).isoformat()))
        conn.commit()
        conn.close()

        bets_placed += 1
        log.info("  📝 Бумажная ставка #%d сохранена", bets_placed)

        time.sleep(1)  # пауза между рынками

    # 6. Урегулирование
    log.info("📋 Проверяем вчерашние ставки...")
    resolve_pending_bets()

    # Статистика
    print_stats()

    log.info("✅ Сканирование завершено. Новых ставок: %d\n", bets_placed)

# ─────────────────────────────────────────────
# ЗАПУСК
# ─────────────────────────────────────────────

def check_connectivity() -> bool:
    """Быстрая проверка доступности Polymarket API при старте."""
    log.info("🔗 Проверка подключения к Polymarket API...")
    test_url = "https://gamma-api.polymarket.com/markets"
    r = safe_get(test_url, params={"limit": 1, "active": "true"}, api_mode=True, retries=2)
    if r is None:
        log.error("❌ Polymarket API недоступен. Проверьте интернет-соединение.")
        log.error("   URL: %s", test_url)
        return False
    data = safe_json(r, "connectivity check")
    if data is None:
        log.warning("⚠️  API ответил, но вернул не-JSON. Статус HTTP: %d", r.status_code)
        log.warning("   Начало ответа: %s", r.text[:300])
        return False
    log.info("✅ API доступен — ответ получен (элементов: %d)", len(data) if isinstance(data, list) else 1)
    return True


def main():
    log.info("🚀 Polymarket Weather Tracker v2 запущен")
    log.info("   БД: %s", os.path.abspath(DB_PATH))
    log.info("   Адаптивный порог: gap > %.1f× ошибка_станции  |  Мин. объём: $%d",
             ADAPTIVE_GAP_MULT, MIN_VOLUME)
    log.info("   MIN_BET_CONFIDENCE: %.2f  |  MAX_DAYS_AHEAD: %d дн.", MIN_BET_CONFIDENCE, MAX_DAYS_AHEAD)
    log.info("   Чёрный список городов: %s", ", ".join(sorted(BLACKLISTED_CITIES)) or "нет")
    log.info("   Станций в таблице ошибок: %d", len(STATION_FORECAST_ERRORS))
    log.info("   Интервал: каждые %d минут", SCAN_INTERVAL)

    init_db()
    check_connectivity()

    # Обновляем таблицу ошибок из накопленных данных
    update_station_errors()

    # Сразу при старте — догоняем пропущенные урегулирования
    log.info("📋 Проверка пропущенных урегулирований при старте...")
    resolve_pending_bets()
    print_stats()

    scan()  # первое полное сканирование

    schedule.every(SCAN_INTERVAL).minutes.do(scan)
    schedule.every().day.at("06:00").do(resolve_pending_bets)
    schedule.every().day.at("08:00").do(print_stats)
    schedule.every(30).days.do(update_station_errors)  # обновляем ошибки раз в 30 дней

    log.info("⏰ Следующее сканирование через %d мин. Ctrl+C для остановки.", SCAN_INTERVAL)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("⛔ Остановлено")
        print_stats()
