import re
import json
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
HEADERS  = {"User-Agent": "kalshi-dashboard/2.0"}

HIGH_TEMP_TICKERS = {
    "KXHIGHAUS":     "Austin",
    "KXHIGHTBOS":    "Boston",
    "KXHIGHCHI":     "Chicago",
    "KXHIGHTDAL":    "Dallas",
    "KXHIGHTEMPDEN": "Denver",
    "KXHIGHDEN":     "Denver",
    "KXHIGHTDC":     "Washington DC",
    "KXHIGHTHOU":    "Houston",
    "KXHIGHLAX":     "Los Angeles",
    "KXHIGHTLV":     "Las Vegas",
    "KXHIGHMIA":     "Miami",
    "KXHIGHTMIN":    "Minneapolis",
    "KXHIGHNY":      "NYC",
    "KXHIGHTOKC":    "Oklahoma City",
    "KXHIGHTPHX":    "Phoenix",
    "KXHIGHTSATX":   "San Antonio",
    "KXHIGHTSFO":    "San Francisco",
    "KXHIGHTSEA":    "Seattle",
    "KXHIGHPHIL":    "Philadelphia",
    "KXHIGHTATL":    "Atlanta",
    "KXHIGHTNOLA":   "New Orleans",
}

def date_to_text(d):
    try:
        return d.strftime("on %b %-d, %Y")
    except ValueError:
        return d.strftime("on %b %#d, %Y")

def to_float(x):
    try: return float(x)
    except: return None

def get_json(url, params=None):
    r = requests.get(url, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.json()

def build_series_list(all_series):
    best = {}
    for s in all_series:
        ticker = (s.get("ticker") or "").strip().upper()
        if ticker not in HIGH_TEMP_TICKERS:
            continue
        city = HIGH_TEMP_TICKERS[ticker]
        if city not in best:
            best[city] = {"city": city, "ticker": ticker}
        elif ticker.startswith("KX") and not best[city]["ticker"].startswith("KX"):
            best[city] = {"city": city, "ticker": ticker}
    for ticker, city in HIGH_TEMP_TICKERS.items():
        if city not in best:
            best[city] = {"city": city, "ticker": ticker}
    return sorted(best.values(), key=lambda x: x["city"])

def get_open_markets(series_ticker):
    markets, cursor = [], None
    while True:
        params = {"series_ticker": series_ticker, "status": "open", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        data   = get_json(f"{BASE_URL}/markets", params=params)
        batch  = data.get("markets", [])
        markets += batch
        nc = data.get("cursor")
        if not nc or nc == cursor or not batch:
            break
        cursor = nc
    return markets

def store_city(date_bucket, city, ticker, filtered):
    no_asks = [to_float(m.get("no_ask_dollars")) for m in filtered]
    clean   = [x for x in no_asks if x is not None]
    lowest  = min(clean) if clean else None
    rows = []
    for m in filtered:
        na  = to_float(m.get("no_ask_dollars"))
        ya  = to_float(m.get("yes_ask_dollars"))
        ttl = re.sub(r'\*\*([^*]+)\*\*', r'\1', m.get("title") or "").strip()
        bracket = re.search(r'(>|<|\d+[-–]\d+)\s*°', ttl)
        rows.append({
            "ticker":        m.get("ticker"),
            "title":         ttl,
            "bracket":       bracket.group(0) if bracket else ttl,
            "yes_ask":       ya,
            "no_ask":        na,
            "close_time":    m.get("close_time"),
            "lowest_no_ask": (na is not None and na == lowest),
        })
    date_bucket[city] = {"series_ticker": ticker, "markets": rows}

def fetch():
    today    = datetime.utcnow()
    tomorrow = today + timedelta(days=1)
    dates    = [date_to_text(today), date_to_text(tomorrow)]

    raw    = get_json(f"{BASE_URL}/series")
    series = build_series_list(raw.get("series", []))

    result = {
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "dates": {d: {} for d in dates},
    }

    for item in series:
        city, ticker = item["city"], item["ticker"]
        try:
            markets = get_open_markets(ticker)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                continue
            raise
        for date_text in dates:
            filtered = [m for m in markets if date_text in (m.get("title") or "")]
            if filtered:
                store_city(result["dates"][date_text], city, ticker, filtered)
        time.sleep(0.12)

    return result

if __name__ == "__main__":
    print("Fetching Kalshi temperature markets...")
    data = fetch()
    total = sum(len(v) for v in data["dates"].values())
    print(f"Done: {total} city-date slots, fetched at {data['fetched_at']}")
    Path("data.json").write_text(json.dumps(data, indent=2, ensure_ascii=False))
    print("Saved → data.json")
