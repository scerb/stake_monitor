import json
import os
import time
from web3 import Web3
from tqdm import tqdm
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes
import base64

# --- Configuration ---
RPC_URL = "https://ethereum-rpc.publicnode.com"
CONTRACT_ADDRESS = Web3.to_checksum_address("0x634DAEeCF243c844263D206e1DcF68F310e6BB19")
START_BLOCK = 20926952
BLOCK_STEP = 1000
STAKERS_JSON = "stakers_addresses.enc"  # Changed extension to indicate encrypted file
PROGRESS_FILE = "last_block.txt"
ENCRYPTION_KEY = b'Lm\\\xa5\x88O\x91\x19C\xdf\x0b\x88\x9a\x00\x18[a&\xc8\xa1\\0\xc5\xcb\x97\xaaP\xe24\x1cF\x7f'  # CHANGE THIS TO A RANDOM 32-BYTE KEY

# --- Encryption Functions ---
def encrypt_data(data: str, key: bytes) -> str:
    cipher = AES.new(key, AES.MODE_CBC)
    ct_bytes = cipher.encrypt(pad(data.encode(), AES.block_size))
    iv = cipher.iv
    return base64.b64encode(iv + ct_bytes).decode()

def decrypt_data(encrypted_data: str, key: bytes) -> str:
    encrypted_data = base64.b64decode(encrypted_data)
    iv = encrypted_data[:AES.block_size]
    ct = encrypted_data[AES.block_size:]
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    return unpad(cipher.decrypt(ct), AES.block_size).decode()

# --- Event Signatures ---
STAKE_EVENT = "Stake(address,uint256)"
stake_topic = Web3.keccak(text=STAKE_EVENT).hex()

# --- Web3 Setup ---
web3 = Web3(Web3.HTTPProvider(RPC_URL))
END_BLOCK = web3.eth.block_number

# --- Load progress ---
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return int(f.read().strip())
    return START_BLOCK

# --- Save progress ---
def save_progress(block):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(block))

# --- Load or initialize staker addresses ---
def load_stakers():
    if os.path.exists(STAKERS_JSON):
        with open(STAKERS_JSON, "rb") as f:
            encrypted_data = f.read().decode()
            decrypted_data = decrypt_data(encrypted_data, ENCRYPTION_KEY)
            return set(json.loads(decrypted_data))
    return set()

def save_stakers(addresses):
    data_to_save = json.dumps(list(addresses))
    encrypted_data = encrypt_data(data_to_save, ENCRYPTION_KEY)
    with open(STAKERS_JSON, "wb") as f:
        f.write(encrypted_data.encode())

# --- Main Processing ---
staker_addresses = load_stakers()
start_block = load_progress()
print(f"Resuming from block {start_block} to {END_BLOCK}...")

for block in tqdm(range(start_block, END_BLOCK, BLOCK_STEP)):
    from_block = block
    to_block = min(block + BLOCK_STEP - 1, END_BLOCK)

    try:
        logs = web3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": CONTRACT_ADDRESS
        })

        print(f"Fetched {len(logs)} logs from blocks {from_block} to {to_block}")

        for log in logs:
            try:
                topic0 = log['topics'][0].hex()

                if topic0 == stake_topic:
                    if len(log['topics']) < 2:
                        continue

                    user = Web3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                    staker_addresses.add(user)

            except Exception as log_err:
                print(f"Failed to parse log: {log_err}")

        # Save progress and state
        save_progress(to_block + 1)
        save_stakers(staker_addresses)

        time.sleep(0.1)

    except Exception as e:
        print(f"Error at blocks {from_block}–{to_block}: {e}")

print(f"\nSaved {len(staker_addresses)} unique staker addresses to encrypted file {STAKERS_JSON}")

# To generate a proper encryption key (run this once and store securely):
# from Crypto.Random import get_random_bytes
# key = get_random_bytes(32)
# print(key)