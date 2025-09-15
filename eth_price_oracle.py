# eth_price_oracle.py
# A robust, rate-limited historical ETH price oracle with local caching and multiple fallbacks.
# - Primary source: CoinGecko market_chart/range
# - Fallback: CryptoCompare histominute
# - Caches prices to ./prices/eth_price_<5min_bucket>[_gbp].json
# - All networking is rate-limited (default 2 req/sec) and retries on 429/5xx with exponential backoff.

import os
import json
import time
import threading
from typing import Tuple, Optional

import requests

def _mkdir_p(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

class RateLimiter:
    """Simple token-bucket rate limiter for max requests per second (RPS)."""
    def __init__(self, max_rps: float = 2.0):
        self.max_rps = max(0.1, float(max_rps))
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        with self._lock:
            now = time.time()
            min_interval = 1.0 / self.max_rps
            delta = now - self._last
            if delta < min_interval:
                time.sleep(min_interval - delta)
            self._last = time.time()

class EthPriceOracle:
    def __init__(self, cache_dir: str = "prices", max_rps: float = 2.0, session: Optional[requests.Session] = None):
        self.cache_dir = cache_dir
        _mkdir_p(self.cache_dir)
        self.ratelimiter = RateLimiter(max_rps=max_rps)
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "COR-TxAnalyzer/1.0 (+https://github.com)"})

    @staticmethod
    def _bucket_5min(ts: int) -> int:
        return int(ts) - (int(ts) % 300)

    def _cache_path(self, bucket_ts: int, currency: str) -> str:
        suffix = "" if currency.lower() == "usd" else f"_{currency.lower()}"
        return os.path.join(self.cache_dir, f"eth_price_{bucket_ts}{suffix}.json")

    def _read_cache(self, bucket_ts: int, currency: str) -> Optional[float]:
        path = self._cache_path(bucket_ts, currency)
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    data = json.load(f)
                key = "price_usd" if currency.lower() == "usd" else f"price_{currency.lower()}"
                if isinstance(data, dict) and key in data:
                    return float(data[key])
        except Exception:
            return None
        return None

    def _write_cache(self, bucket_ts: int, price: float, source: str, currency: str) -> None:
        path = self._cache_path(bucket_ts, currency)
        try:
            key = "price_usd" if currency.lower() == "usd" else f"price_{currency.lower()}"
            with open(path, "w") as f:
                json.dump({
                    "ts_bucket": bucket_ts,
                    key: float(price),
                    "source": source,
                    "fetched_at": int(time.time())
                }, f, indent=2)
        except Exception:
            pass

    def _http_get(self, url: str, params: dict, timeout: float = 15.0, retries: int = 4) -> dict:
        last_err = None
        for attempt in range(retries):
            self.ratelimiter.wait()
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait_s = float(retry_after) if retry_after else (1.5 * (2 ** attempt))
                    time.sleep(wait_s)
                    last_err = Exception(f"429 Too Many Requests (attempt {attempt+1})")
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last_err = e
                time.sleep(1.2 * (2 ** attempt))
        raise RuntimeError(f"HTTP GET failed after retries for {url} with {params}: {last_err}")

    # -------------------- Remote fetchers --------------------
    def _fetch_from_coingecko(self, center_ts: int, currency: str) -> Optional[Tuple[float, str]]:
        # Fetch 30-minute window to be safe, find nearest minute price
        start = center_ts - 30 * 60
        end   = center_ts + 30 * 60
        url   = "https://api.coingecko.com/api/v3/coins/ethereum/market_chart/range"
        params = {"vs_currency": currency.lower(), "from": str(start), "to": str(end)}
        data = self._http_get(url, params, timeout=20.0, retries=4)
        prices = data.get("prices") or []
        if not prices:
            return None
        # prices is [[ms, price], ...]
        nearest = None
        min_delta = 10**18
        for ms, px in prices:
            ts = int(ms // 1000)
            d  = abs(ts - center_ts)
            if d < min_delta:
                min_delta = d
                nearest = float(px)
        if nearest is not None:
            return nearest, "coingecko"
        return None

    def _fetch_from_cryptocompare(self, center_ts: int, currency: str) -> Optional[Tuple[float, str]]:
        # Use histominute — ask for 60 samples ending at center_ts and pick nearest
        url = "https://min-api.cryptocompare.com/data/v2/histominute"
        params = {"fsym": "ETH", "tsym": currency.upper(), "toTs": str(center_ts), "limit": "60"}
        data = self._http_get(url, params, timeout=20.0, retries=4)
        d = data.get("Data") or {}
        arr = d.get("Data") or []
        if not arr:
            return None
        # Each item: {"time": 123, "close": 1234.56, ...}
        nearest = None
        min_delta = 10**18
        for itm in arr:
            ts = int(itm.get("time", 0))
            px = float(itm.get("close", 0.0))
            delta = abs(ts - center_ts)
            if delta < min_delta and px > 0:
                min_delta = delta
                nearest = px
        if nearest is not None:
            return float(nearest), "cryptocompare"
        return None

    # -------------------- Public API --------------------
    def _get_eth_price_fiat_at_ts(self, ts: int, currency: str) -> Tuple[float, str]:
        """Return (price, source) for currency 'usd' or 'gbp' with 5-min bucket cache."""
        if not isinstance(ts, int):
            ts = int(ts)
        currency = currency.lower()
        if currency not in ("usd", "gbp"):
            raise ValueError("currency must be 'usd' or 'gbp'")

        bucket = self._bucket_5min(ts)
        cached = self._read_cache(bucket, currency)
        if cached:
            return cached, "cache"

        # Try CoinGecko first
        try:
            res = self._fetch_from_coingecko(ts, currency)
            if res:
                price, src = res
                self._write_cache(bucket, price, src, currency)
                return price, src
        except Exception:
            pass

        # Fallback: CryptoCompare
        res = self._fetch_from_cryptocompare(ts, currency)
        if res:
            price, src = res
            self._write_cache(bucket, price, src, currency)
            return price, src

        # As a last resort, widen CG window
        try:
            start = ts - 2 * 3600
            end   = ts + 2 * 3600
            url   = "https://api.coingecko.com/api/v3/coins/ethereum/market_chart/range"
            params = {"vs_currency": currency, "from": str(start), "to": str(end)}
            data = self._http_get(url, params, timeout=25.0, retries=5)
            prices = data.get("prices") or []
            if prices:
                vals = sorted(float(p[1]) for p in prices if len(p) == 2)
                if vals:
                    median = vals[len(vals)//2]
                    self._write_cache(bucket, median, "coingecko_median", currency)
                    return median, "coingecko_median"
        except Exception:
            pass

        # If nothing works, store sentinel zero to avoid hot-looping
        self._write_cache(bucket, 0.0, "unavailable", currency)
        return 0.0, "unavailable"

    def get_eth_price_usd_at_ts(self, ts: int) -> Tuple[float, str]:
        return self._get_eth_price_fiat_at_ts(ts, "usd")

    def get_eth_price_gbp_at_ts(self, ts: int) -> Tuple[float, str]:
        return self._get_eth_price_fiat_at_ts(ts, "gbp")

if __name__ == "__main__":
    import sys
    ts = int(time.time()) if len(sys.argv) < 2 else int(sys.argv[1])
    oracle = EthPriceOracle(max_rps=2.0)
    px_usd, src_usd = oracle.get_eth_price_usd_at_ts(ts)
    px_gbp, src_gbp = oracle.get_eth_price_gbp_at_ts(ts)
    print(f"ETH @ {ts}: ${px_usd:.2f} ({src_usd}) | £{px_gbp:.2f} ({src_gbp})")
