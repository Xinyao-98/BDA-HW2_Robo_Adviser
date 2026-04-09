import argparse
import bisect
import json
import os
import ssl
import time
import urllib.request
from datetime import datetime, timedelta, timezone


COINGECKO_TREASURY_URL = "https://api.coingecko.com/api/v3/companies/public_treasury/bitcoin"
BINANCE_BTC_CHART_URL = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=1000"
COINGECKO_BTC_CHART_URL = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=365"
ALPHA_BASE = "https://www.alphavantage.co/query"
CACHE_DIR = ".cache_dat_mnav"
ALPHA_MIN_INTERVAL_SECONDS = 1.2
LOOKBACK_DAYS = 365
MNAV_SCALE = 1.41

_last_alpha_call_ts = 0.0


def fetch_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
        return json.loads(resp.read().decode("utf-8"))


def ensure_cache_dir() -> None:
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)


def cache_path(name: str) -> str:
    return os.path.join(CACHE_DIR, name)


def load_cached_json(name: str):
    path = cache_path(name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_cached_json(name: str, data) -> None:
    ensure_cache_dir()
    with open(cache_path(name), "w", encoding="utf-8") as f:
        json.dump(data, f)


def fetch_alpha_json(params: dict, cache_name: str):
    global _last_alpha_call_ts

    cached = load_cached_json(cache_name)
    if cached is not None:
        return cached

    # Respect free-tier burst limit to reduce "Please spread out requests".
    now = time.time()
    wait = ALPHA_MIN_INTERVAL_SECONDS - (now - _last_alpha_call_ts)
    if wait > 0:
        time.sleep(wait)

    data = fetch_json(alpha_url(params))
    _last_alpha_call_ts = time.time()

    # Cache successful payloads only.
    if extract_alpha_error(data) is None:
        save_cached_json(cache_name, data)
    return data


def extract_alpha_error(data: dict) -> str | None:
    """
    Alpha Vantage often returns error payloads instead of time series.
    """
    if not isinstance(data, dict):
        return "Unexpected non-dict API response."
    for k in ("Note", "Information", "Error Message"):
        if k in data and data[k]:
            return str(data[k])
    return None


def alpha_url(params: dict) -> str:
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{ALPHA_BASE}?{query}"


def normalize_date_key(date_str: str) -> str:
    """
    Normalize possible date formats to YYYY-MM-DD.
    Example inputs:
    - 2026-04-08
    - 2026-04-08 00:00:00
    - 2026-04-08T00:00:00Z
    """
    s = str(date_str).strip()
    if "T" in s:
        s = s.split("T")[0]
    if " " in s:
        s = s.split(" ")[0]
    if len(s) >= 10:
        s = s[:10]
    datetime.strptime(s, "%Y-%m-%d")
    return s


def get_btc_holding_from_coingecko(ticker: str) -> float:
    data = load_cached_json("coingecko_treasury_bitcoin.json")
    if data is None:
        data = fetch_json(COINGECKO_TREASURY_URL)
        save_cached_json("coingecko_treasury_bitcoin.json", data)
    companies = data.get("companies", [])
    ticker = ticker.upper()
    for c in companies:
        symbol = str(c.get("symbol", "")).upper()  # e.g. MSTR.US
        if symbol == f"{ticker}.US" or symbol == ticker:
            return float(c.get("total_holdings", 0.0))
    raise RuntimeError(f"Ticker {ticker} not found in CoinGecko public treasury list.")


def get_company_basics_from_alpha(ticker: str, api_key: str) -> tuple[float, float]:
    data = fetch_alpha_json(
        {"function": "OVERVIEW", "symbol": ticker, "apikey": api_key},
        cache_name=f"alpha_overview_{ticker}.json",
    )
    err = extract_alpha_error(data)
    if err:
        raise RuntimeError(f"Alpha Vantage overview error: {err}")
    shares = float(data.get("SharesOutstanding", "0") or 0)
    market_cap = float(data.get("MarketCapitalization", "0") or 0)
    if shares <= 0 or market_cap <= 0:
        raise RuntimeError(f"Invalid Alpha Vantage overview for {ticker}: {data}")
    return shares, market_cap


def get_stock_daily_close(ticker: str, api_key: str) -> dict:
    endpoints = [
        {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": ticker,
            "outputsize": "compact",
            "apikey": api_key,
        },
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "outputsize": "compact",
            "apikey": api_key,
        },
    ]
    last_err = None
    for p in endpoints:
        data = fetch_alpha_json(p, cache_name=f"alpha_stock_{ticker}_{p['function']}.json")
        err = extract_alpha_error(data)
        if err:
            last_err = err
            continue
        ts = data.get("Time Series (Daily)", {})
        out = {}
        for d, row in ts.items():
            nd = normalize_date_key(d)
            if "5. adjusted close" in row:
                out[nd] = float(row["5. adjusted close"])
            elif "4. close" in row:
                out[nd] = float(row["4. close"])
        if out:
            return out
    raise RuntimeError(f"Cannot get stock time series for {ticker}. Last API message: {last_err}")


def get_btc_daily_close(api_key: str) -> dict:
    del api_key  # BTC price now comes from Binance public endpoint.
    # Source priority:
    # 1) Binance (fast, no key) - can be region-blocked in CI (HTTP 451)
    # 2) CoinGecko market_chart fallback (no key)
    out = {}

    # Try Binance first
    try:
        data = load_cached_json("binance_btc_klines.json")
        if data is None:
            data = fetch_json(BINANCE_BTC_CHART_URL)
            save_cached_json("binance_btc_klines.json", data)

        if isinstance(data, list) and data:
            for item in data:
                if not isinstance(item, list) or len(item) < 5:
                    continue
                ts_ms, price = item[0], item[4]  # close price
                day = datetime.utcfromtimestamp(float(ts_ms) / 1000.0).strftime("%Y-%m-%d")
                out[day] = float(price)
        if out:
            return out
    except Exception as e:
        print(f"[WARN] Binance BTC source unavailable: {e}")

    # Fallback to CoinGecko
    data = load_cached_json("coingecko_btc_market_chart.json")
    if data is None:
        data = fetch_json(COINGECKO_BTC_CHART_URL)
        save_cached_json("coingecko_btc_market_chart.json", data)

    prices = data.get("prices", []) if isinstance(data, dict) else []
    for item in prices:
        if not isinstance(item, list) or len(item) < 2:
            continue
        ts_ms, price = item[0], item[1]
        day = datetime.utcfromtimestamp(float(ts_ms) / 1000.0).strftime("%Y-%m-%d")
        out[day] = float(price)

    if not out:
        raise RuntimeError("Cannot get BTC time series from both Binance and CoinGecko.")
    return out


def align_btc_to_stock_days(stock_map: dict, btc_map: dict) -> list[tuple[str, float, float]]:
    """
    Align BTC price to each stock trading day.
    If BTC price on that exact day is missing, use the most recent prior BTC day.
    """
    btc_days = sorted(btc_map.keys())
    if not btc_days:
        return []

    aligned = []
    for day in sorted(stock_map.keys()):
        idx = bisect.bisect_right(btc_days, day) - 1
        if idx < 0:
            continue
        btc_day = btc_days[idx]
        aligned.append((day, stock_map[day], btc_map[btc_day]))
    return aligned


def filter_recent_days(series_map: dict, days: int) -> dict:
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()
    out = {}
    for d, v in series_map.items():
        try:
            if datetime.strptime(d, "%Y-%m-%d").date() >= cutoff:
                out[d] = v
        except ValueError:
            continue
    return out


def main():
    parser = argparse.ArgumentParser(description="Free API mNAV proxy chart for DAT company.")
    parser.add_argument("--ticker", default="MSTR", help="Company ticker, e.g. MSTR/MARA/RIOT.")
    parser.add_argument("--api-key", default=os.getenv("ALPHAVANTAGE_API_KEY"), help="Free Alpha Vantage API key.")
    parser.add_argument("--output-csv", default="dat_mnav_free.csv", help="Output CSV path.")
    parser.add_argument("--output-html", default="dat_mnav_free.html", help="Output HTML chart path.")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable local cache and force live API calls.",
    )
    args = parser.parse_args()

    if args.no_cache and os.path.exists(CACHE_DIR):
        for fn in os.listdir(CACHE_DIR):
            try:
                os.remove(os.path.join(CACHE_DIR, fn))
            except OSError:
                pass

    if not args.api_key:
        raise RuntimeError("Missing Alpha Vantage API key. Set ALPHAVANTAGE_API_KEY or pass --api-key.")

    ticker = args.ticker.upper()
    btc_holdings = get_btc_holding_from_coingecko(ticker)
    shares_outstanding, market_cap_latest = get_company_basics_from_alpha(ticker, args.api_key)
    stock_map = get_stock_daily_close(ticker, args.api_key)
    btc_map = get_btc_daily_close(args.api_key)
    stock_map = filter_recent_days(stock_map, LOOKBACK_DAYS)
    btc_map = filter_recent_days(btc_map, LOOKBACK_DAYS)

    aligned_rows = align_btc_to_stock_days(stock_map, btc_map)
    if not aligned_rows:
        raise RuntimeError(
            f"No usable aligned dates. stock_points={len(stock_map)}, btc_points={len(btc_map)}. "
            "Please check if your Alpha Vantage key is valid and not rate-limited."
        )

    rows = []
    for d, stock_close, btc_close in aligned_rows:
        btc_nav = btc_holdings * btc_close
        if btc_nav <= 0:
            continue
        mnav_proxy = ((stock_close * shares_outstanding) / btc_nav) * MNAV_SCALE
        rows.append((d, mnav_proxy))

    if not rows:
        raise RuntimeError("Aligned dates exist, but mNAV rows are empty after filtering.")

    with open(args.output_csv, "w", encoding="utf-8") as f:
        f.write("date,ticker,mnav_proxy\n")
        for d, m in rows:
            f.write(f"{d},{ticker},{m}\n")

    traces = [
        {
            "x": [d for d, _ in rows],
            "y": [m for _, m in rows],
            "name": ticker,
            "mode": "lines",
            "type": "scatter",
        }
    ]
    updated_dt_utc = datetime.now(timezone.utc)
    updated_at_utc8 = (updated_dt_utc + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{ticker} mNAV 近似值-時間圖</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #0a0a0a;
      --panel: #101214;
      --text: #f4f5f7;
      --muted: #9aa3ad;
      --accent: #00e5a8;
      --line: #1f252d;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(1200px 700px at 20% -10%, #1a1f26 0%, var(--bg) 50%);
      color: var(--text);
      font-family: "Segoe UI", Arial, sans-serif;
    }}
    .container {{
      max-width: 980px;
      margin: 0 auto;
      padding: 18px 16px 24px;
    }}
    .hero {{
      border: 1px solid var(--line);
      background: linear-gradient(180deg, #12161b 0%, #0f1216 100%);
      border-radius: 14px;
      padding: 18px 20px;
      margin-bottom: 14px;
    }}
    .hero h1 {{
      margin: 0;
      font-size: 22px;
      font-weight: 700;
    }}
    .accent {{ color: var(--accent); }}
    .panel {{
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 16px 40px rgba(0, 0, 0, 0.38);
    }}
    #chart {{ width: 100%; height: 440px; }}
    .notes {{
      margin-top: 14px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
    }}
    .notes p {{ margin: 4px 0; }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <h1><span class="accent">{ticker}</span> mNAV近似值-時間圖</h1>
    </section>
    <section class="panel">
      <div id="chart"></div>
    </section>
    <section class="notes">
      <p><strong>本程式皆使用免費 API：</strong></p>
      <p>1. CoinGecko API：抓取 MSTR 公司 BTC 持倉量。</p>
      <p>2. Alpha Vantage API：抓取流通股數與股價時間序列。</p>
      <p>3. Binance API：抓取 BTC 日收盤價（不可用時 fallback 到 CoinGecko）。</p>
      <p><strong>指標計算</strong></p>
      <p>- mNAV 近似值 =（股價 × 流通股數）/（BTC 持倉量 × BTC 價格）× {MNAV_SCALE}</p>
      <p>- 最新市值（Alpha Vantage 概覽）：{market_cap_latest:,.0f} 美元。</p>
      <p><strong>備註</strong></p>
      <p>- 本數據為估算值，非公司官方正式 mNAV。</p>
      <p>- 免費 API 可能有更新延遲、限流或欄位變動。</p>
      <p>- 股價與 BTC 價格來源不同，可能有些微誤差。</p>
      <p>資料最後更新時間（UTC+8）：{updated_at_utc8}</p>
    </section>
  </div>
  <script>
    const traces = {json.dumps(traces)};
    traces[0].name = "{ticker}（Strategy）";
    traces[0].line = {{ color: "#00e5a8", width: 2.6 }};
    Plotly.newPlot("chart", traces, {{
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "#0f1216",
      font: {{ color: "#f4f5f7", family: "Segoe UI, Arial, sans-serif", size: 13 }},
      margin: {{ t: 20, r: 24, b: 56, l: 64 }},
      hovermode: "x unified",
      xaxis: {{ title: "日期", gridcolor: "#1f252d" }},
      yaxis: {{ title: "mNAV 近似值", gridcolor: "#1f252d" }},
      legend: {{ orientation: "h", x: 0, y: 1.1 }}
    }}, {{ responsive: true, displaylogo: false }});
  </script>
</body>
</html>
"""
    with open(args.output_html, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Saved CSV: {args.output_csv}")
    print(f"Saved HTML: {args.output_html}")
    print(f"Cache directory: {CACHE_DIR}")


if __name__ == "__main__":
    main()
