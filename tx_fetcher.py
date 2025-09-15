# tx_fetcher.py
# Etherscan-based transaction indexer with robust rate limiting and caching.
# - Pulls normal txns, internal txns, and Cortensor token transfers
# - Computes per-tx ETH in/out, gas, USD price using EthPriceOracle
# - Classifies COR buys/sells/transfers, staking rewards, stake/unstake (pool)
# - NEW: node_reward tagging and internal transfer tagging (ETH & COR)
# - Computes per-address running balances incl. staked COR
# - Honors a configurable start block (default Cortensor genesis often 20926952; file keeps prior default)
# - Respects Etherscan 5 rps limit by defaulting to 2 rps; configurable.
#
# Requires: keys.json with {"etherscan_api_key": "YOUR_KEY"}

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Iterable, Set

import requests

from eth_price_oracle import EthPriceOracle

# ---------------- Configuration ----------------
CORTENSOR_TOKEN_ADDRESS = "0x8e0EeF788350f40255D86DFE8D91ec0AD3a4547F".lower()
REWARDS_CONTRACT        = "0x6876e661AE0F740C9132B7B8f26f7D245cFc62C1".lower()
STAKING_POOL_ADDRESS    = "0x634DAEeCF243c844263D206e1DcF68F310e6BB19".lower()  # special no-tax stake transfers
NODE_REWARD_SENDER      = "0xD0b2A999de3302a74A8Ac9C9c8bD7E37A984eB01".lower()  # NEW: node reward distributor

# Keep previous default to avoid changing other behavior
DEFAULT_START_BLOCK = 20800000

CACHE_DIR  = "cache"
PRICES_DIR = "prices"
os.makedirs(CACHE_DIR,  exist_ok=True)
os.makedirs(PRICES_DIR, exist_ok=True)

LOG = logging.getLogger("tx_fetcher")
if not LOG.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ERC-20 Transfer event topic
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ---------------- Helpers ----------------
def _load_keys(path: str = "keys.json") -> Dict[str, str]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _read_api_key(path="keys.json") -> Optional[str]:
    k = _load_keys(path)
    return k.get("etherscan_api_key")

def _parse_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def _parse_float_str_wei_to_eth(wei_str: str) -> float:
    try:
        return int(wei_str) / 1e18
    except Exception:
        try:
            return float(wei_str) / 1e18
        except Exception:
            return 0.0

def _ts_to_datestr(ts: int) -> str:
    return datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S UTC")

def _mkdir_p(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def load_my_addresses(path: str = "data.json") -> List[str]:
    """
    Load user's own addresses from data.json.
    Supports either:
      - dict with "addresses": [list]
      - dict with address keys
      - a plain list of addresses
    Returns lowercased addresses.
    """
    try:
        with open(path, "r") as f:
            data = json.load(f)
        addrs: List[str] = []
        if isinstance(data, dict):
            if "addresses" in data and isinstance(data["addresses"], list):
                addrs = [a for a in data["addresses"] if isinstance(a, str) and a.lower().startswith("0x")]
            else:
                addrs = [k for k in data.keys() if isinstance(k, str) and k.lower().startswith("0x")]
        elif isinstance(data, list):
            addrs = [a for a in data if isinstance(a, str) and a.lower().startswith("0x")]
        return [a.lower() for a in addrs]
    except Exception:
        return []

# Global cache of "my addresses" for internal-transfer tagging
MY_ADDRESSES: Set[str] = set(load_my_addresses())

# ---------------- Etherscan Client ----------------
class EtherscanClient:
    def __init__(self, api_key: str, max_rps: float = 2.0, network: str = "api"):
        self.api_key = api_key or ""
        self.base = f"https://{network}.etherscan.io/api"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "COR-TxAnalyzer/1.0 (+https://github.com)"})
        self.max_rps = max(0.1, float(max_rps))
        self._last = 0.0

    def _rl_wait(self):
        now = time.time()
        min_interval = 1.0 / self.max_rps
        delta = now - self._last
        if delta < min_interval:
            time.sleep(min_interval - delta)
        self._last = time.time()

    def _get(self, params: dict, cache_key: Optional[str] = None, retries: int = 3) -> dict:
        if cache_key:
            path = os.path.join(CACHE_DIR, f"{cache_key}.json")
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        return json.load(f)
                except Exception:
                    pass

        last_err = None
        for attempt in range(retries):
            self._rl_wait()
            try:
                p = dict(params)
                if self.api_key:
                    p["apikey"] = self.api_key
                r = self.session.get(self.base, params=p, timeout=20)
                if r.status_code == 429:
                    # be gentle; exponential-ish backoff
                    time.sleep(1.2 * (2 ** attempt))
                    last_err = Exception("429 Too Many Requests")
                    continue
                r.raise_for_status()
                data = r.json()
                if cache_key:
                    try:
                        with open(os.path.join(CACHE_DIR, f"{cache_key}.json"), "w") as f:
                            json.dump(data, f, indent=2)
                    except Exception:
                        pass
                return data
            except Exception as e:
                last_err = e
                time.sleep(1.0 * (2 ** attempt))

        raise RuntimeError(f"Etherscan GET failed: params={params}, last_err={last_err}")

    # ---- Endpoints ----
    def get_block_number_by_time(self, ts: int, closest: str = "before") -> int:
        params = {"module": "block", "action": "getblocknobytime", "timestamp": str(int(ts)), "closest": closest}
        data = self._get(params, cache_key=f"blk_by_time_{ts}_{closest}")
        if str(data.get("status")) == "1" and str(data.get("message")).lower() == "ok":
            return _parse_int(data.get("result"), 0)
        if closest == "after":
            return self.get_block_number_by_time(ts, closest="before")
        return 0

    def get_normal_txs(self, address: str, start_block: int, end_block: int) -> List[dict]:
        params = {
            "module": "account", "action": "txlist",
            "address": address, "startblock": str(start_block), "endblock": str(end_block),
            "page": "1", "offset": "10000", "sort": "asc"
        }
        data = self._get(params, cache_key=f"txlist_{address}_{start_block}_{end_block}")
        if str(data.get("status")) == "1":
            return data.get("result", [])
        if data.get("message") == "No transactions found":
            return []
        return data.get("result", []) if isinstance(data.get("result"), list) else []

    def get_internal_txs(self, address: str, start_block: int, end_block: int) -> List[dict]:
        params = {
            "module": "account", "action": "txlistinternal",
            "address": address, "startblock": str(start_block), "endblock": str(end_block),
            "page": "1", "offset": "10000", "sort": "asc"
        }
        data = self._get(params, cache_key=f"txintern_{address}_{start_block}_{end_block}")
        if str(data.get("status")) == "1":
            return data.get("result", [])
        if data.get("message") == "No transactions found":
            return []
        return data.get("result", []) if isinstance(data.get("result"), list) else []

    def get_token_txs_for_contract(self, address: str, token_contract: str, start_block: int, end_block: int) -> List[dict]:
        params = {
            "module": "account", "action": "tokentx",
            "contractaddress": token_contract, "address": address,
            "startblock": str(start_block), "endblock": str(end_block),
            "page": "1", "offset": "10000", "sort": "asc"
        }
        data = self._get(params, cache_key=f"tokentx_{token_contract}_{address}_{start_block}_{end_block}")
        if str(data.get("status")) == "1":
            return data.get("result", [])
        if data.get("message") == "No transactions found":
            return []
        return data.get("result", []) if isinstance(data.get("result"), list) else []

    def get_logs(self, from_block: int, to_block: int, address: Optional[str] = None, topic0: Optional[str] = None) -> List[dict]:
        """
        Lightweight wrapper around Etherscan Logs API to fetch event logs in a block range.
        Use address=CORTENSOR_TOKEN_ADDRESS and topic0=ERC20_TRANSFER_TOPIC to pull ERC-20 Transfer logs.
        """
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": str(int(from_block)),
            "toBlock": str(int(to_block)),
        }
        if address:
            params["address"] = address
        if topic0:
            params["topic0"] = topic0
        cache_key = f"logs_{address or 'any'}_{from_block}_{to_block}_{(topic0 or 'any')[:10]}"
        data = self._get(params, cache_key=cache_key, retries=3)
        # Etherscan returns {"status":"0","message":"No records found"} when empty
        if str(data.get("status")) == "1":
            return data.get("result", []) or []
        if str(data.get("status")) == "0" and "No records" in str(data.get("message","")):
            return []
        return data.get("result", []) if isinstance(data.get("result"), list) else []

# ---------------- Core logic ----------------
def _compute_eth_flows(normal: List[dict], internals: List[dict], addr_lower: str) -> Dict[str, dict]:
    """
    Build a dict txhash -> eth flow object:
    {
      "eth_in": float,
      "eth_out": float,
      "gas_eth": float,
      "timestamp": int,
      "blockNumber": int,
      "from": str,
      "to": str,
      "sources": set(...)   # NEW: may include 'internal_in', 'internal_out', 'internal'
    }
    """
    flows: Dict[str, dict] = {}

    # Normal transactions (top-level)
    for t in normal:
        try:
            h = t.get("hash")
            if not h:
                continue
            frm = (t.get("from") or "").lower()
            to  = (t.get("to") or "").lower()
            ts  = _parse_int(t.get("timeStamp") or t.get("timestamp") or 0, 0)
            blk = _parse_int(t.get("blockNumber") or 0, 0)
            val_eth = _parse_float_str_wei_to_eth(t.get("value", "0"))

            # gas: use (gasUsed * gasPrice) if available, else gas * gasPrice (upper bound)
            gas_used = _parse_int(t.get("gasUsed", t.get("gas", "0")), 0)
            gas_price = _parse_int(t.get("gasPrice", "0"), 0)
            gas_eth = (gas_used * gas_price) / 1e18 if gas_used and gas_price else 0.0

            e = flows.setdefault(h, {
                "eth_in": 0.0, "eth_out": 0.0, "gas_eth": 0.0,
                "timestamp": ts, "blockNumber": blk,
                "from": frm, "to": to,
                "sources": set()
            })
            # Direction and gas attribution
            if frm == addr_lower:
                e["eth_out"] += val_eth
                e["gas_eth"] += gas_eth  # gas paid by sender
            elif to == addr_lower:
                e["eth_in"] += val_eth

            # NEW: internal-transfer ETH tagging vs your own addresses
            if frm == addr_lower and to in MY_ADDRESSES and to != addr_lower:
                e["sources"].update({"internal_out", "internal"})
            if to == addr_lower and frm in MY_ADDRESSES and frm != addr_lower:
                e["sources"].update({"internal_in", "internal"})

            # Keep the earliest timestamp/block per tx if multiples merge
            if not e.get("timestamp"):
                e["timestamp"] = ts
            if not e.get("blockNumber"):
                e["blockNumber"] = blk
            if not e.get("from"):
                e["from"] = frm
            if not e.get("to"):
                e["to"] = to
        except Exception:
            continue

    # Internal transactions (value transfers triggered by contracts)
    for t in internals:
        try:
            h = t.get("hash")
            if not h:
                continue
            frm = (t.get("from") or "").lower()
            to  = (t.get("to") or "").lower()
            ts  = _parse_int(t.get("timeStamp") or t.get("timestamp") or 0, 0)
            blk = _parse_int(t.get("blockNumber") or 0, 0)
            val_eth = _parse_float_str_wei_to_eth(t.get("value", "0"))

            e = flows.setdefault(h, {
                "eth_in": 0.0, "eth_out": 0.0, "gas_eth": 0.0,
                "timestamp": ts, "blockNumber": blk,
                "from": frm, "to": to,
                "sources": set()
            })

            if to == addr_lower:
                e["eth_in"] += val_eth
            elif frm == addr_lower:
                e["eth_out"] += val_eth
                # gas for internals is already counted on the parent normal tx

            # NEW: internal-transfer ETH tagging vs your own addresses
            if frm == addr_lower and to in MY_ADDRESSES and to != addr_lower:
                e["sources"].update({"internal_out", "internal"})
            if to == addr_lower and frm in MY_ADDRESSES and frm != addr_lower:
                e["sources"].update({"internal_in", "internal"})

            if not e.get("timestamp"):
                e["timestamp"] = ts
            if not e.get("blockNumber"):
                e["blockNumber"] = blk
            if not e.get("from"):
                e["from"] = frm
            if not e.get("to"):
                e["to"] = to
        except Exception:
            continue

    return flows

def _compute_cor_flows(token_txs: List[dict], addr_lower: str) -> Dict[str, dict]:
    """
    Build a dict txhash -> COR token flow object:
    {
      "cor_in": float,
      "cor_out": float,
      "froms": set(...),
      "tos": set(...),
      "stake_tag": "stake"|"unstake"|None,
      "sources": set(...)   # may include 'incoming_external','self_out','staking_reward','node_reward','internal*'
    }
    """
    flows: Dict[str, dict] = {}

    for t in token_txs:
        try:
            if (t.get("contractAddress") or "").lower() != CORTENSOR_TOKEN_ADDRESS:
                continue
            h = t.get("hash")
            if not h:
                continue
            frm = (t.get("from") or "").lower()
            to  = (t.get("to") or "").lower()
            ts  = _parse_int(t.get("timeStamp") or t.get("timestamp") or 0, 0)
            blk = _parse_int(t.get("blockNumber") or 0, 0)
            # value normalized by token decimals (Etherscan returns raw 18)
            amt = _parse_float_str_wei_to_eth(t.get("value", "0"))

            e = flows.setdefault(h, {
                "cor_in": 0.0, "cor_out": 0.0,
                "froms": set(), "tos": set(),
                "stake_tag": None,
                "sources": set(),
                "timestamp": ts, "blockNumber": blk,
                "last_from": "", "last_to": ""
            })

            if to == addr_lower:
                e["cor_in"] += amt
                e["sources"].add("incoming_external")
            if frm == addr_lower:
                e["cor_out"] += amt
                e["sources"].add("self_out")

            # Special staking pool (no tax)
            if to == STAKING_POOL_ADDRESS and frm == addr_lower:
                e["stake_tag"] = "stake"
            if frm == STAKING_POOL_ADDRESS and to == addr_lower:
                e["stake_tag"] = "unstake"

            # Rewards contract (existing staking rewards)
            if frm == REWARDS_CONTRACT and to == addr_lower:
                e["sources"].add("staking_reward")

            # NEW: Node reward sender (monthly node rewards)
            if frm == NODE_REWARD_SENDER and to == addr_lower:
                e["sources"].add("node_reward")

            # NEW: internal transfer tagging vs your own addresses
            if frm == addr_lower and to in MY_ADDRESSES and to != addr_lower:
                e["sources"].update({"internal_out", "internal"})
            if to == addr_lower and frm in MY_ADDRESSES and frm != addr_lower:
                e["sources"].update({"internal_in", "internal"})

            e["froms"].add(frm)
            e["tos"].add(to)
            e["last_from"] = frm
            e["last_to"] = to

            if not e.get("timestamp"):
                e["timestamp"] = ts
            if not e.get("blockNumber"):
                e["blockNumber"] = blk
        except Exception:
            continue

    return flows

def _classify_trade(eth_flow: dict, cor_flow: dict) -> Tuple[str, float, float]:
    """
    Return (type, tax_cor_estimate, tax_rate)
    Known types: stake, unstake, staking_reward, node_reward, buy, sell,
                 internal_transfer, airdrop_or_other, transfer, eth_transfer, unknown
    """
    eth_in  = float(eth_flow.get("eth_in", 0.0))
    eth_out = float(eth_flow.get("eth_out", 0.0))
    cor_in  = float(cor_flow.get("cor_in", 0.0))
    cor_out = float(cor_flow.get("cor_out", 0.0))

    # Use combined sources (ETH + COR) for robust tagging in Sources column and logic
    sources = set(eth_flow.get("sources", set())) | set(cor_flow.get("sources", set()))

    # Stake/unstake first (no tax)
    st = cor_flow.get("stake_tag")
    if st == "stake":
        return "stake", 0.0, 0.0
    if st == "unstake":
        return "unstake", 0.0, 0.0

    # Rewards
    if "staking_reward" in sources and cor_in > 0 and eth_out == 0.0 and eth_in == 0.0:
        return "staking_reward", 0.0, 0.0

    # NEW: node_reward (no tax)
    if "node_reward" in sources and cor_in > 0 and eth_out == 0.0 and eth_in == 0.0:
        return "node_reward", 0.0, 0.0

    # NEW: internal transfers (no tax)
    if ("internal_in" in sources or "internal_out" in sources) and (cor_in > 0 or cor_out > 0):
        return "internal_transfer", 0.0, 0.0

    # Heuristic buy/sell (5% token tax)
    if cor_in > 0 and eth_out > 0 and eth_in == 0:
        # Buy: received COR, spent ETH => received is 95% of gross
        tax_cor = (cor_in * 0.05) / 0.95
        return "buy", tax_cor, 0.05
    if cor_out > 0 and eth_in > 0 and eth_out == 0:
        # Sell: sent COR, received ETH => tax is 5% of tokens sold
        tax_cor = cor_out * 0.05
        return "sell", tax_cor, 0.05

    # Airdrop/other incoming (no ETH flow)
    if cor_in > 0 and eth_in == 0 and eth_out == 0 and "incoming_external" in sources:
        return "airdrop_or_other", 0.0, 0.0

    # Generic transfer (self/external mixed)
    if cor_in > 0 or cor_out > 0:
        return "transfer", 0.0, 0.0

    # ETH-only moves
    if eth_in != 0.0 or eth_out != 0.0:
        return "eth_transfer", 0.0, 0.0

    return "unknown", 0.0, 0.0

# ----------- Refinement for buys: pull logs to get actual tax & router fee -----------
def _topics_addr(topic: str) -> str:
    try:
        # last 20 bytes
        return ("0x" + topic[-40:]).lower()
    except Exception:
        return ""

def _refine_buy_with_logs(client: EtherscanClient, tx_hash: str, block_num: int,
                          my_addr_lower: str, eth_out_usd: float) -> Optional[Dict[str, float]]:
    """
    For a COR 'buy' tx, fetch ERC-20 Transfer logs for the COR contract at the tx block,
    filter to this tx hash, and compute:
      - net_to_user_cor
      - router_fee_cor      (router -> others)
      - router_received_cor (others -> router)
      - actual_tax_cor      (to COR token contract address)
      - gross_from_pool_cor = router_received_cor + actual_tax_cor
      - unit_price_usd      = eth_out_usd / gross_from_pool_cor
      - router_fee_usd, tax_usd at that unit price
    Returns None if logs unavailable.
    """
    tx_hash_l = (tx_hash or "").lower()
    if not tx_hash_l or not block_num:
        return None

    try:
        logs = client.get_logs(from_block=block_num, to_block=block_num,
                               address=CORTENSOR_TOKEN_ADDRESS, topic0=ERC20_TRANSFER_TOPIC)
    except Exception:
        return None

    if not logs:
        return None

    transfers = []
    for lg in logs:
        if (lg.get("transactionHash") or "").lower() != tx_hash_l:
            continue
        topics = lg.get("topics") or []
        if len(topics) < 3:
            continue
        frm = _topics_addr(topics[1])
        to  = _topics_addr(topics[2])
        data_hex = lg.get("data") or "0x0"
        try:
            val = int(data_hex, 16) / 1e18
        except Exception:
            val = 0.0
        transfers.append({"from": frm, "to": to, "value": float(val)})

    if not transfers:
        return None

    # Identify router as sender of tokens to our wallet in this tx
    to_me = [t for t in transfers if t["to"] == my_addr_lower]
    if not to_me:
        return None

    router_addr = to_me[0]["from"]
    net_to_user = sum(t["value"] for t in to_me)

    # Tokens the router forwarded elsewhere in this same tx (e.g. Uniswap Fee Collector)
    router_fee_cor = sum(t["value"] for t in transfers if t["from"] == router_addr and t["to"] != my_addr_lower)

    # Tokens received by the router in this tx (typically from the pool)
    router_received_cor = sum(t["value"] for t in transfers if t["to"] == router_addr)

    # Actual tax observed in logs: Transfers whose recipient is the COR token contract itself
    actual_tax_cor = sum(t["value"] for t in transfers if t["to"] == CORTENSOR_TOKEN_ADDRESS)

    gross_from_pool_cor = router_received_cor + actual_tax_cor
    if gross_from_pool_cor <= 0:
        return None

    unit_price_usd = (eth_out_usd / gross_from_pool_cor) if eth_out_usd is not None else None
    tax_usd = (actual_tax_cor * unit_price_usd) if unit_price_usd is not None else None
    router_fee_usd = (router_fee_cor * unit_price_usd) if unit_price_usd is not None else None

    return {
        "net_to_user_cor": float(net_to_user),
        "router_fee_cor": float(router_fee_cor),
        "router_received_cor": float(router_received_cor),
        "actual_tax_cor": float(actual_tax_cor),
        "gross_from_pool_cor": float(gross_from_pool_cor),
        "unit_price_usd": float(unit_price_usd) if unit_price_usd is not None else None,
        "tax_usd": float(tax_usd) if tax_usd is not None else None,
        "router_fee_usd": float(router_fee_usd) if router_fee_usd is not None else None,
    }

# ---------------- Public API ----------------
def index_transactions_for_addresses(
    addresses: Iterable[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    start_block_override: Optional[int] = None,
    etherscan_key_path: str = "keys.json",
    rate_limit_rps: float = 2.0,
) -> List[dict]:
    """
    Index and classify ETH + COR flows for a set of addresses in a date/block range.
    Returns a flat list of per-address transaction rows suitable for CSV export.
    """
    # Refresh MY_ADDRESSES each run (in case data.json changed)
    global MY_ADDRESSES
    MY_ADDRESSES = set(load_my_addresses())

    keys = _load_keys(etherscan_key_path)
    api_key = keys.get("etherscan_api_key", "")

    client = EtherscanClient(api_key=api_key, max_rps=rate_limit_rps)
    oracle = EthPriceOracle(cache_dir=PRICES_DIR, max_rps=rate_limit_rps)

    # Resolve block range
    if start_block_override and start_block_override > 0:
        start_block = start_block_override
    else:
        start_block = DEFAULT_START_BLOCK

    if start_date is not None:
        start_ts = int(start_date.timestamp())
        sb = client.get_block_number_by_time(start_ts, closest="before")
        start_block = sb or start_block
    if end_date is not None:
        end_ts = int(end_date.timestamp())
        end_block = client.get_block_number_by_time(end_ts, closest="after")
    else:
        end_block = 99999999  # latest

    LOG.info(f"Block range: {start_block} → {end_block}")

    raw_rows: List[dict] = []
    addr_set = {a.lower() for a in addresses if isinstance(a, str) and a.startswith("0x")}

    for addr in addr_set:
        LOG.info(f"Fetching for {addr}")

        normal   = client.get_normal_txs(addr, start_block, end_block)
        internal = client.get_internal_txs(addr, start_block, end_block)
        token    = client.get_token_txs_for_contract(addr, CORTENSOR_TOKEN_ADDRESS, start_block, end_block)

        eth_flows = _compute_eth_flows(normal, internal, addr)
        cor_flows = _compute_cor_flows(token, addr)

        # Union all tx hashes for this address
        all_hashes = set(eth_flows.keys()) | set(cor_flows.keys())
        for h in sorted(all_hashes):
            ef = eth_flows.get(h, {
                "eth_in": 0.0, "eth_out": 0.0, "gas_eth": 0.0,
                "timestamp": 0, "blockNumber": 0,
                "from": "", "to": "", "sources": set()
            })
            cf = cor_flows.get(h, {
                "cor_in": 0.0, "cor_out": 0.0,
                "sources": set(), "froms": set(), "tos": set(),
                "stake_tag": None, "timestamp": 0, "blockNumber": 0,
                "last_from": "", "last_to": ""
            })

            ts  = ef.get("timestamp") or cf.get("timestamp") or 0
            blk = ef.get("blockNumber") or cf.get("blockNumber") or 0

            # ETH price at timestamp (USD). GBP optional if your oracle supports it.
            eth_usd, src = (0.0, "unavailable")
            if ts:
                try:
                    eth_usd, src = oracle.get_eth_price_usd_at_ts(ts)
                except Exception:
                    eth_usd, src = (0.0, "unavailable")

            trade_type, tax_cor, tax_rate = _classify_trade(ef, cf)

            # Derived values
            eth_in_usd   = ef["eth_in"]  * eth_usd
            eth_out_usd  = ef["eth_out"] * eth_usd
            gas_usd      = ef["gas_eth"] * eth_usd
            net_eth      = ef["eth_in"] - ef["eth_out"] - ef["gas_eth"]
            net_usd      = net_eth * eth_usd

            # Unit price for buy/sell only (ETH per COR or USD per COR as needed)
            unit_price_usd = None
            tax_usd = 0.0
            router_fee_cor = 0.0
            router_fee_usd = 0.0
            cor_gross_from_pool = None  # will be filled by refinement

            if trade_type == "buy" and cf["cor_in"] > 0:
                # Initial estimate (5% back-out) — works when logs unavailable
                gross_tokens_est = cf["cor_in"] + tax_cor
                unit_price_usd = (eth_out_usd / gross_tokens_est) if gross_tokens_est > 0 else None
                tax_usd = tax_cor * unit_price_usd if unit_price_usd is not None else 0.0

                # ---- Refinement using logs: actual tax + router fee + gross from pool
                try:
                    refine = _refine_buy_with_logs(client, h, blk, addr, eth_out_usd)
                except Exception:
                    refine = None

                if refine and refine.get("gross_from_pool_cor", 0.0) > 0:
                    # Replace estimates with observed values
                    actual_tax_cor = float(refine.get("actual_tax_cor", tax_cor) or 0.0)
                    unit_price_usd = float(refine.get("unit_price_usd", unit_price_usd) or (unit_price_usd or 0.0))
                    tax_usd        = float(refine.get("tax_usd", tax_usd) or tax_usd)
                    router_fee_cor = float(refine.get("router_fee_cor", 0.0) or 0.0)
                    router_fee_usd = float(refine.get("router_fee_usd", 0.0) or 0.0)
                    cor_gross_from_pool = float(refine.get("gross_from_pool_cor", 0.0) or 0.0)
                    # Keep the 'est' field names for compatibility, but values are now actuals
                    tax_cor = actual_tax_cor

            elif trade_type == "sell" and cf["cor_out"] > 0:
                unit_price_usd = (eth_in_usd / cf["cor_out"]) if cf["cor_out"] > 0 else None
                tax_usd = tax_cor * unit_price_usd if unit_price_usd is not None else 0.0

            # Sources column = union of ETH + COR sources (+ stake tag if present)
            sources_union = set(ef.get("sources", set())) | set(cf.get("sources", set()))
            if cf.get("stake_tag"):
                sources_union.add(cf["stake_tag"])
            if router_fee_cor and router_fee_cor > 0:
                sources_union.add("uniswap_protocol_fee")

            row = {
                "address": addr,
                "tx_hash": h,
                "block": blk,
                "timestamp": ts,
                "date_utc": _ts_to_datestr(ts) if ts else "",
                "from": ef.get("from") or cf.get("last_from") or "",
                "to": ef.get("to") or cf.get("last_to") or "",
                "eth_in": round(ef["eth_in"], 18),
                "eth_out": round(ef["eth_out"], 18),
                "gas_fee_eth": round(ef["gas_eth"], 18),
                "eth_usd": round(eth_usd, 6),
                "eth_in_usd": round(eth_in_usd, 2),
                "eth_out_usd": round(eth_out_usd, 2),
                "gas_fee_usd": round(gas_usd, 2),
                "net_eth": round(net_eth, 18),
                "net_usd": round(net_usd, 2),
                "cor_in": round(cf.get("cor_in", 0.0), 8),
                "cor_out": round(cf.get("cor_out", 0.0), 8),
                "trade_type": trade_type,
                "tax_rate": tax_rate,
                "tax_cor_est": round(tax_cor, 8),
                "tax_usd_est": round(tax_usd, 6),
                "unit_price_usd": (round(unit_price_usd, 6) if isinstance(unit_price_usd, float) else None),
                "tag_sources": ";".join(sorted(sources_union)),
                "price_source": src,
            }
            # Optional new fields (safe for downstream code – UI ignores unknown keys)
            if router_fee_cor:
                row["router_fee_cor"] = round(router_fee_cor, 8)
                row["router_fee_usd"] = round(router_fee_usd, 6)
            if cor_gross_from_pool:
                row["cor_gross_from_pool"] = round(cor_gross_from_pool, 8)

            raw_rows.append(row)

    # ---------------- Running balances (per-address) ----------------
    # Keep the existing logic intact so "after" columns continue to work
    raw_rows.sort(key=lambda r: (r.get("address",""), r.get("block", 0), r.get("timestamp", 0), r.get("tx_hash","")))
    state_by_addr: Dict[str, Dict[str, float]] = {}
    final_rows: List[dict] = []

    for r in raw_rows:
        addr = r["address"]
        st = state_by_addr.setdefault(addr, {
            "eth": 0.0,
            "cor_wallet": 0.0,
            "cor_staked": 0.0
        })

        # ETH balance delta
        st["eth"] += (r.get("eth_in", 0.0) - r.get("eth_out", 0.0) - r.get("gas_fee_eth", 0.0))

        # COR wallet delta
        wallet_delta = r.get("cor_in", 0.0) - r.get("cor_out", 0.0)

        # stake/unstake adjustments (owned stays constant for stake moves)
        if r["trade_type"] == "stake":
            # wallet -> staked
            st["cor_wallet"] += wallet_delta  # wallet decreases (negative)
            st["cor_staked"] += (-wallet_delta)  # staked increases by the amount moved
        elif r["trade_type"] == "unstake":
            st["cor_wallet"] += wallet_delta  # wallet increases
            st["cor_staked"] -= wallet_delta  # staked decreases
        else:
            # all other cases simply change wallet (rewards/buy/sell/transfer/internal)
            st["cor_wallet"] += wallet_delta

        owned = st["cor_wallet"] + st["cor_staked"]

        r_out = dict(r)
        r_out["eth_balance_after"]    = round(st["eth"], 6)
        r_out["cor_wallet_after"]     = round(st["cor_wallet"], 6)
        r_out["cor_staked_after"]     = round(st["cor_staked"], 6)
        r_out["cor_owned_after"]      = round(owned, 6)

        final_rows.append(r_out)

    return final_rows

# CLI test
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Index ETH & COR flows for addresses.")
    parser.add_argument("--addresses", nargs="+", required=True, help="One or more 0x addresses")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (UTC)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (UTC)")
    parser.add_argument("--start-block", type=int, default=DEFAULT_START_BLOCK)
    parser.add_argument("--rps", type=float, default=2.0, help="Max requests per second (Etherscan & price sources)")
    args = parser.parse_args()

    def parse_date(d: Optional[str]) -> Optional[datetime]:
        if not d:
            return None
        return datetime.strptime(d, "%Y-%m-%d")

    rows = index_transactions_for_addresses(
        addresses=args.addresses,
        start_date=parse_date(args.start),
        end_date=parse_date(args.end),
        start_block_override=args.start_block,
        rate_limit_rps=args.rps,
    )
    out = "tx_index_output.json"
    with open(out, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"Wrote {len(rows)} rows to {out}")
