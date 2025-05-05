import json
from web3 import Web3
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import base64
from tqdm import tqdm
import threading
from queue import Queue

# --- Configuration ---
RPC_URL = "https://ethereum-rpc.publicnode.com"
ENCRYPTED_FILE = "stakers_addresses.enc"
OUTPUT_FILE = "staker_balances.json"
ENCRYPTION_KEY = b'Lm\\\xa5\x88O\x91\x19C\xdf\x0b\x88\x9a\x00\x18[a&\xc8\xa1\\0\xc5\xcb\x97\xaaP\xe24\x1cF\x7f'

# Contract setup
STAKING_CONTRACT_ADDRESS = Web3.to_checksum_address("0x634DAEeCF243c844263D206e1DcF68F310e6BB19")
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

# --- Web3 Setup ---
web3 = Web3(Web3.HTTPProvider(RPC_URL))
staking_contract = web3.eth.contract(address=STAKING_CONTRACT_ADDRESS, abi=STAKING_ABI)

# --- Decryption Functions ---
def decrypt_data(encrypted_data: str) -> str:
    encrypted_data = base64.b64decode(encrypted_data)
    iv = encrypted_data[:16]
    ct = encrypted_data[16:]
    cipher = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv=iv)
    return unpad(cipher.decrypt(ct), AES.block_size).decode()

def load_encrypted_addresses():
    with open(ENCRYPTED_FILE, "r") as f:
        return json.loads(decrypt_data(f.read()))

# --- Balance Fetching ---
def get_staked_balance(address):
    try:
        return staking_contract.functions.shares(Web3.to_checksum_address(address)).call()[0]
    except Exception as e:
        print(f"Error fetching balance: {e}")
        return 0

def run_in_thread(callback=None):
    """Run the stake position analysis in a thread"""
    def worker():
        print("Decrypting addresses...")
        addresses = load_encrypted_addresses()
        
        print("Fetching staked balances...")
        balances = []
        for address in tqdm(addresses):
            raw_balance = get_staked_balance(address)
            if raw_balance > 0:
                balances.append({
                    "raw_balance": raw_balance,
                    "staked_balance": raw_balance / (10 ** 18)  # Convert to human-readable
                })
        
        # Sort by balance (descending)
        balances.sort(key=lambda x: x["raw_balance"], reverse=True)
        
        # Create numbered output without addresses
        result = {
            str(i+1): {
                "position": i+1,
                "staked_balance": item["staked_balance"],
                "raw_balance": str(item["raw_balance"])  # String to preserve precision
            }
            for i, item in enumerate(balances)
        }
        
        with open(OUTPUT_FILE, "w") as f:
            json.dump(result, f, indent=2)
        
        print(f"\nSaved {len(result)} active stakers to {OUTPUT_FILE}")
        if result:
            top = result["1"]
            print(f"Top staker has {top['staked_balance']:,.2f} tokens")
        
        if callback:
            callback()
    
    thread = threading.Thread(target=worker)
    thread.daemon = True
    thread.start()
    return thread

if __name__ == "__main__":
    run_in_thread().join()