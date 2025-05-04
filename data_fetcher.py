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

# Cortensor token and staking contract addresses
CORTENSOR_TOKEN_ADDRESS = Web3.to_checksum_address("0x8e0EeF788350f40255D86DFE8D91ec0AD3a4547F")
STAKING_CONTRACT_ADDRESS = Web3.to_checksum_address("0x634DAEeCF243c844263D206e1DcF68F310e6BB19")
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

# Contract instances
token_contract = web3.eth.contract(address=CORTENSOR_TOKEN_ADDRESS, abi=ERC20_ABI)
staking_contract = web3.eth.contract(address=STAKING_CONTRACT_ADDRESS, abi=STAKING_ABI)

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
        with open("data.json", "r") as f:
            data = json.load(f)
            
            # Handle both formats:
            if isinstance(data, dict):
                if "addresses" in data:  # New format with "addresses" key
                    return [
                        Web3.to_checksum_address(addr) 
                        for addr in data["addresses"] 
                        if Web3.is_address(addr)
                    ]
                else:  # Old format with address keys
                    return [
                        Web3.to_checksum_address(k) 
                        for k in data.keys() 
                        if Web3.is_address(k)
                    ]
            elif isinstance(data, list):  # Legacy format
                return [
                    Web3.to_checksum_address(addr) 
                    for addr in data 
                    if Web3.is_address(addr)
                ]
    except Exception as e:
        print(f"Error loading addresses: {e}")
    return []

def fetch_prices():
    """Fetch ETH and Cortensor prices with retries and fallbacks"""
    retries = 3
    for attempt in range(retries):
        try:
            # ETH price
            eth_response = requests.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "ethereum", "vs_currencies": "usd"},
                timeout=10
            )
            eth_response.raise_for_status()
            eth_price = eth_response.json()["ethereum"]["usd"]

            # Cortensor price with fallback
            try:
                cortensor_response = requests.get(
                    f"https://api.geckoterminal.com/api/v2/networks/{GECKO_POOL_ID.split('_')[0]}/pools/{GECKO_POOL_ID.split('_')[1]}",
                    timeout=10
                )
                cortensor_response.raise_for_status()
                cortensor_price = float(cortensor_response.json()["data"]["attributes"]["base_token_price_usd"])
            except Exception as e:
                print(f"GeckoTerminal API failed, using fallback price: {e}")
                cortensor_price = 0.0001 * eth_price  # Example fallback ratio

            return eth_price, cortensor_price
            
        except Exception as e:
            print(f"Price fetch attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2)
    
    return 0.0, 0.0  # Return safe defaults if all retries fail

def fetch_data(miner_ids):
    """Fetch all data using ThreadPoolExecutor for better reliability"""
    results = {}
    eth_price, cortensor_price = fetch_prices()
    
    def process_address(addr):
        try:
            if not Web3.is_address(addr):
                return None
                
            checksum = Web3.to_checksum_address(addr)

            # ETH balance
            eth_balance = web3.eth.get_balance(checksum) / 1e18

            # Cortensor token balance
            balance = token_contract.functions.balanceOf(checksum).call()
            decimals = token_contract.functions.decimals().call()
            cortensor_balance = balance / (10 ** decimals)

            # Staking info
            shares = staking_contract.functions.shares(checksum).call()
            staked_amount = shares[0] / 1e18
            staked_time = shares[1]
            staked_time_ago = time_ago(staked_time)

            cortensor_total = cortensor_balance + staked_amount

            result_data = {
                "eth_balance": round(eth_balance, 4),
                "eth_value_usd": round(eth_balance * eth_price, 2),
                "cortensor_balance": round(cortensor_balance, 4),
                "staked_balance": round(staked_amount, 4),
                "cortensor_value_usd": round(cortensor_total * cortensor_price, 2),
                "time_staked_ago": staked_time_ago
            }

            print(f"{checksum} -> ETH: {eth_balance:.4f} (${eth_balance * eth_price:.2f}), "
                  f"CORTENSOR: {cortensor_balance:.4f}, STAKED: {staked_amount:.4f}, "
                  f"TOTAL $: {(cortensor_total * cortensor_price):.2f}, AGO: {staked_time_ago}")

            return checksum, result_data
        except Exception as e:
            print(f"Error processing {addr}: {e}")
            return None

    # Use ThreadPoolExecutor with limited workers
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_address, addr): addr for addr in miner_ids}
        for future in as_completed(futures, timeout=30):
            result = future.result()
            if result:
                addr, data = result
                results[addr] = data

    return results

if __name__ == "__main__":
    try:
        addresses = load_addresses_from_data_json()
        if not addresses:
            print("No valid addresses found in data.json")
            sys.exit(1)
            
        print(f"Fetching data for {len(addresses)} addresses...")
        updated_data = fetch_data(addresses)
        
        data_path = get_data_path("data.json")
        with open(data_path, "w") as f:
            json.dump(updated_data, f, indent=4)
            
        print(f"Successfully updated {data_path}")
    except Exception as e:
        print(f"Critical error: {e}")
        sys.exit(1)
