import os
import sys
import json
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from web3 import Web3

def get_current_prices():
    """Fetch current prices directly from APIs"""
    return fetch_prices()

# Initialize Web3 with multiple fallback providers
def init_web3():
    providers = [
        "https://ethereum-rpc.publicnode.com",
        "https://eth.llamarpc.com",
        "https://rpc.ankr.com/eth"
    ]

    for provider_url in providers:
        try:
            web3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={'timeout': 10}))
            if web3.is_connected():
                print(f"Connected via {provider_url}")
                return web3
        except Exception as e:
            print(f"Failed to connect to {provider_url}: {e}")

    raise ConnectionError("Could not connect to any Ethereum provider")

web3 = init_web3()

# Path handling for both dev and compiled exe
def get_data_path(filename):
    """Get the correct path for data files"""
    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, filename)

# Contract addresses
CORTENSOR_TOKEN_ADDRESS = Web3.to_checksum_address("0x8e0EeF788350f40255D86DFE8D91ec0AD3a4547F")
STAKING_CONTRACT_ADDRESS = Web3.to_checksum_address("0x634DAEeCF243c844263D206e1DcF68F310e6BB19")
REWARDS_CONTRACT_ADDRESS = Web3.to_checksum_address("0x6876e661AE0F740C9132B7B8f26f7D245cFc62C1")
GECKO_POOL_ID = "eth_0x8981dc572dfb436d7d23f1287dee031f833234b9"

# ABIs
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function"
    }
]

STAKING_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "", "type": "address"}],
        "name": "shares",
        "outputs": [
            {"internalType": "uint256", "name": "amount", "type": "uint256"},
            {"internalType": "uint256", "name": "stakedTime", "type": "uint256"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

REWARDS_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "stakeHolder", "type": "address"}],
        "name": "rewardOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [{"internalType": "address", "name": "", "type": "address"}],
        "name": "staked",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [],
        "name": "fixedAPR",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

# Contract instances
token_contract = web3.eth.contract(address=CORTENSOR_TOKEN_ADDRESS, abi=ERC20_ABI)
staking_contract = web3.eth.contract(address=STAKING_CONTRACT_ADDRESS, abi=STAKING_ABI)
rewards_contract = web3.eth.contract(address=REWARDS_CONTRACT_ADDRESS, abi=REWARDS_ABI)

def time_ago(timestamp):
    """Convert timestamp to human-readable time ago string"""
    if not timestamp or timestamp == 0:
        return "Unknown"
    now = datetime.now()
    then = datetime.fromtimestamp(timestamp)
    delta = now - then
    seconds = int(delta.total_seconds())

    if seconds < 60:
        return f"{seconds} sec ago"
    elif seconds < 3600:
        return f"{seconds // 60} min {seconds % 60} sec ago"
    elif seconds < 86400:
        return f"{seconds // 3600} hr {(seconds % 3600) // 60} min ago"
    else:
        return f"{seconds // 86400} days ago"

def load_addresses_from_data_json():
    try:
        with open(get_data_path("data.json"), "r") as f:
            data = json.load(f)

            # Handle both formats:
            if isinstance(data, dict):
                if "addresses" in data:
                    return [
                        Web3.to_checksum_address(addr)
                        for addr in data["addresses"]
                        if Web3.is_address(addr)
                    ]
                else:
                    return [
                        Web3.to_checksum_address(k)
                        for k in data.keys()
                        if Web3.is_address(k)
                    ]
            elif isinstance(data, list):
                return [
                    Web3.to_checksum_address(addr)
                    for addr in data
                    if Web3.is_address(addr)
                ]
    except Exception as e:
        print(f"Error loading addresses: {e}")
    return []

def load_existing_data():
    """Load existing data from JSON file including all fields"""
    try:
        with open(get_data_path("data.json"), "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    except Exception as e:
        print(f"Error loading existing data: {e}")
        return {}

def fetch_prices():
    """Fetch ETH, COR and BTC prices with retries and fallbacks"""
    retries = 3
    for attempt in range(retries):
        try:
            # Get ETH and BTC prices from CoinGecko
            price_response = requests.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "ethereum,bitcoin", "vs_currencies": "usd"},
                timeout=10
            )
            price_response.raise_for_status()
            price_data = price_response.json()
            eth_price = price_data["ethereum"]["usd"]
            btc_price = price_data["bitcoin"]["usd"]

            # Cortensor price with GeckoTerminal, fallback if needed
            try:
                network, pool = GECKO_POOL_ID.split('_', 1)
                cortensor_response = requests.get(
                    f"https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool}",
                    timeout=10
                )
                cortensor_response.raise_for_status()
                cortensor_price = float(
                    cortensor_response.json()["data"]["attributes"]["base_token_price_usd"]
                )
            except Exception as e:
                print(f"GeckoTerminal API failed, using fallback price: {e}")
                cortensor_price = 0.0001 * eth_price  # simple conservative fallback

            return eth_price, cortensor_price, btc_price

        except Exception as e:
            print(f"Price fetch attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(10)

    return 0.0, 0.0, 0.0

def _get_rewards_data(checksum):
    """Fetch rewards data for a single checksum address"""
    try:
        claimable_rewards = rewards_contract.functions.rewardOf(checksum).call()
        apr = rewards_contract.functions.fixedAPR().call() / 100  # Convert to decimal
        return {
            "claimable_rewards": claimable_rewards / 1e18,
            "current_apr": apr
        }
    except Exception as e:
        print(f"Error fetching rewards for {checksum}: {e}")
        return {"claimable_rewards": 0, "current_apr": 0}

def _process_address(checksum, eth_price, cortensor_price, token_decimals):
    """Compute all metrics for one address; returns (checksum, data) or None."""
    try:
        # ETH balance
        eth_balance = web3.eth.get_balance(checksum) / 1e18

        # Cortensor token balance
        balance = token_contract.functions.balanceOf(checksum).call()
        cortensor_balance = balance / (10 ** token_decimals)

        # Staking info
        shares = staking_contract.functions.shares(checksum).call()
        staked_amount = shares[0] / 1e18
        staked_time = shares[1]
        staked_time_ago = time_ago(staked_time)

        # Rewards info
        rewards_data = _get_rewards_data(checksum)
        claimable_rewards = rewards_data["claimable_rewards"]
        current_apr = rewards_data["current_apr"]

        cortensor_total = cortensor_balance + staked_amount
        total_value_usd = cortensor_total * cortensor_price
        rewards_value_usd = claimable_rewards * cortensor_price

        result_data = {
            "eth_balance": round(eth_balance, 4),
            "eth_value_usd": round(eth_balance * eth_price, 2),
            "cortensor_balance": round(cortensor_balance, 4),
            "staked_balance": round(staked_amount, 4),
            "cortensor_value_usd": round(total_value_usd, 6),
            "time_staked_ago": staked_time_ago,
            "claimable_rewards": round(claimable_rewards, 4),
            "current_apr": round(current_apr, 4),
            "rewards_value_usd": round(rewards_value_usd, 6),
            "daily_reward": round(staked_amount * current_apr / 365, 4)
        }

        print(f"{checksum} -> ETH: {eth_balance:.4f} (${eth_balance * eth_price:.2f}), "
              f"CORTENSOR: {cortensor_balance:.4f}, STAKED: {staked_amount:.4f}, "
              f"REWARDS: {claimable_rewards:.4f}, APR: {current_apr:.2%}, "
              f"TOTAL $: {total_value_usd:.2f}, AGO: {staked_time_ago}")

        return checksum, result_data
    except Exception as e:
        print(f"Error processing {checksum}: {e}")
        return None

def fetch_data_stream(miner_ids, callback=None, max_workers=8):
    """
    Streamed fetching with controlled concurrency.
    - Calls `callback(addr, data)` as each result arrives.
    - Returns a dict of all results at the end.
    """
    results = {}
    eth_price, cortensor_price, _ = fetch_prices()

    # Cache token decimals once
    try:
        token_decimals = token_contract.functions.decimals().call()
    except Exception:
        token_decimals = 18

    # Normalize inputs to checksum first (fast validation pass)
    checksums = []
    for addr in miner_ids:
        if not Web3.is_address(addr):
            continue
        checksums.append(Web3.to_checksum_address(addr))

    if not checksums:
        return results

    def task(addr_checksum):
        return _process_address(addr_checksum, eth_price, cortensor_price, token_decimals)

    # Limit concurrency to be friendly with RPCs; 8 is a safe default
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(task, cs): cs for cs in checksums}
        for future in as_completed(future_map):
            try:
                res = future.result()
                if res:
                    addr, data = res
                    results[addr] = data
                    if callback:
                        callback(addr, data)
            except Exception as e:
                print(f"Worker error: {e}")

    return results

def fetch_data(miner_ids, btc_price=None):
    """
    Backwards-compatible function: returns a dict once all addresses complete.
    Internally uses the streaming fetch for robustness.
    """
    return fetch_data_stream(miner_ids, callback=None, max_workers=8)

def save_miner_data(new_data):
    """Save miner data, forcing updates to USD values while preserving other fields."""
    try:
        existing_data = load_existing_data()

        for address, new_values in new_data.items():
            if address in existing_data:
                preserved_fields = {
                    k: v for k, v in existing_data[address].items()
                    if not k.endswith('_value_usd') and k not in new_values
                }
                new_values.update(preserved_fields)

            existing_data[address] = new_values

        with open(get_data_path("data.json"), "w") as f:
            json.dump(existing_data, f, indent=4)

        print("✅ JSON updated with new USD values.")
    except Exception as e:
        print(f"❌ Error saving data: {e}")

if __name__ == "__main__":
    try:
        addresses = load_addresses_from_data_json()
        if not addresses:
            print("No valid addresses found in data.json")
            sys.exit(1)

        print(f"Fetching data for {len(addresses)} addresses...")
        updated_data = fetch_data_stream(addresses)

        if updated_data:
            save_miner_data(updated_data)
            print("Successfully updated miner data while preserving existing fields")
        else:
            print("No data was fetched")
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
