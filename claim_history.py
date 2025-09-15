import json
import logging
from web3 import Web3
from datetime import datetime
import os
import threading
import time
from queue import Queue

# Configure logging
LOG_FILE = "reward_claims.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DATA_FILE = "data.json"
CLAIM_HISTORY_FILE = "claim_history.json"
START_BLOCK = 20926951
BLOCKS_PER_CALL = 1000
MAX_REQUESTS_PER_SECOND = 4
TOKEN_ADDRESS = "0x8e0EeF788350f40255D86DFE8D91ec0AD3a4547F"
REWARDS_CONTRACT = "0x6876e661AE0F740C9132B7B8f26f7D245cFc62C1"
TOKEN_ABI = [{
    "anonymous": False,
    "inputs": [
        {"indexed": True, "name": "from", "type": "address"},
        {"indexed": True, "name": "to", "type": "address"},
        {"indexed": False, "name": "value", "type": "uint256"}
    ],
    "name": "Transfer",
    "type": "event"
}]

# RPC Providers
RPC_PROVIDERS = [
    "https://eth.llamarpc.com",
    "https://rpc.ankr.com/eth",
    "https://cloudflare-eth.com",
    "https://ethereum.publicnode.com"
]

class RewardScanner:
    def __init__(self):
        self.web3 = self._get_web3_connection()
        self.request_counter = 0
        self.last_request_time = time.time()
        self.lock = threading.Lock()
        self.data_lock = threading.Lock()
        self.token_contract = self.web3.eth.contract(
            address=Web3.to_checksum_address(TOKEN_ADDRESS),
            abi=TOKEN_ABI
        )
        self.addresses = self._load_addresses()
        self.claim_history = self._load_claim_history()

    def _get_web3_connection(self):
        """Try multiple RPC providers until we find a working one"""
        for provider_url in RPC_PROVIDERS:
            try:
                web3 = Web3(Web3.HTTPProvider(provider_url, request_kwargs={'timeout': 30}))
                if web3.is_connected():
                    logger.info(f"Successfully connected to {provider_url}")
                    logger.info(f"Chain ID: {web3.eth.chain_id}")
                    logger.info(f"Latest block: {web3.eth.block_number}")
                    return web3
            except Exception as e:
                logger.warning(f"Failed to connect to {provider_url}: {str(e)}")
        raise Exception("Failed to connect to any Ethereum node")

    def _load_addresses(self):
        """Load only valid 0x addresses from data.json"""
        if not os.path.exists(DATA_FILE):
            logger.warning(f"{DATA_FILE} not found, creating new file")
            return set()

        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)

            if not isinstance(data, dict):
                raise ValueError("Root element must be a dictionary")

            addresses = set()
            for address in data.keys():
                if isinstance(address, str) and address.startswith("0x") and Web3.is_address(address):
                    addresses.add(address.lower())
            logger.info(f"Loaded {len(addresses)} addresses for claim scan")
            return addresses

        except Exception as e:
            logger.error(f"Error loading {DATA_FILE}: {str(e)}")
            backup_name = f"{DATA_FILE}.corrupted.{int(time.time())}"
            os.rename(DATA_FILE, backup_name)
            logger.info(f"Created backup of corrupted file: {backup_name}")
            return set()

    def _load_claim_history(self):
        """Load existing claim history from claim_history.json"""
        if not os.path.exists(CLAIM_HISTORY_FILE):
            logger.info(f"{CLAIM_HISTORY_FILE} not found, creating new file")
            return {}

        try:
            with open(CLAIM_HISTORY_FILE, 'r') as f:
                data = json.load(f)

            if not isinstance(data, dict):
                raise ValueError("Root element must be a dictionary")

            validated_data = {}
            for address, address_data in data.items():
                if not isinstance(address_data, dict):
                    logger.warning(f"Skipping invalid address entry: {address}")
                    continue

                validated_data[address.lower()] = {
                    'total_claimed': float(address_data.get('total_claimed', 0)),
                    'claim_count': int(address_data.get('claim_count', 0)),
                    'last_claim_block': address_data.get('last_claim_block'),
                    'last_scanned_block': int(address_data.get('last_scanned_block', START_BLOCK - 1)),
                    'claim_history': list(address_data.get('claim_history', []))
                }

            return validated_data
        except Exception as e:
            logger.error(f"Error loading {CLAIM_HISTORY_FILE}: {str(e)}")
            backup_name = f"{CLAIM_HISTORY_FILE}.corrupted.{int(time.time())}"
            os.rename(CLAIM_HISTORY_FILE, backup_name)
            logger.info(f"Created backup of corrupted file: {backup_name}")
            return {}

    def _rate_limited_request(self):
        """Ensure we don't exceed the rate limit"""
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < 1.0 / MAX_REQUESTS_PER_SECOND:
                time.sleep((1.0 / MAX_REQUESTS_PER_SECOND) - elapsed)
            self.last_request_time = time.time()
            self.request_counter += 1

    def save_claim_history(self):
        """Save claim history data with atomic write pattern to prevent corruption"""
        temp_file = f"{CLAIM_HISTORY_FILE}.tmp"
        try:
            with open(temp_file, 'w') as f:
                json.dump(self.claim_history, f, indent=2)
            os.replace(temp_file, CLAIM_HISTORY_FILE)
        except Exception as e:
            logger.error(f"Error saving claim history: {str(e)}")
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def get_transfers_for_address(self, address, from_block, to_block):
        """Get transfers for a specific address in a block range"""
        self._rate_limited_request()

        try:
            event_signature = self.web3.keccak(text="Transfer(address,address,uint256)").hex()
            logs = self.web3.eth.get_logs({
                'fromBlock': from_block,
                'toBlock': to_block,
                'address': Web3.to_checksum_address(TOKEN_ADDRESS),
                'topics': [
                    event_signature,
                    None,
                    f"0x{'0'*24}{address[2:].lower()}"
                ]
            })

            transfers = []
            for log in logs:
                try:
                    transfer = self.token_contract.events.Transfer().process_log(log)
                    if transfer.args['from'].lower() == REWARDS_CONTRACT.lower():
                        block = self.web3.eth.get_block(transfer.blockNumber)
                        transfers.append({
                            'amount': float(transfer.args['value'] / 10**18),
                            'block': transfer.blockNumber,
                            'timestamp': block.timestamp,
                            'tx_hash': transfer.transactionHash.hex()
                        })
                except Exception as e:
                    logger.error(f"Error processing log: {str(e)}")
                    continue

            return transfers

        except Exception as e:
            logger.error(f"Error getting logs for blocks {from_block}-{to_block}: {str(e)}")
            return []

    def process_address(self, address):
        """Process a single address to find new reward claims"""
        try:
            address_lower = address.lower()
            current_block = self.web3.eth.block_number

            with self.data_lock:
                if address_lower not in self.claim_history:
                    self.claim_history[address_lower] = {
                        'total_claimed': 0.0,
                        'claim_count': 0,
                        'last_claim_block': None,
                        'last_scanned_block': START_BLOCK - 1,
                        'claim_history': []
                    }

                address_data = self.claim_history[address_lower]
                from_block = max(address_data['last_scanned_block'] + 1, START_BLOCK)
                to_block = current_block

            if from_block > to_block:
                return

            logger.info(f"Processing address {address} from block {from_block} to {to_block}")

            for chunk_start in range(from_block, to_block + 1, BLOCKS_PER_CALL):
                chunk_end = min(chunk_start + BLOCKS_PER_CALL - 1, to_block)
                transfers = self.get_transfers_for_address(address, chunk_start, chunk_end)

                if transfers:
                    with self.data_lock:
                        for transfer in transfers:
                            self.claim_history[address_lower]['total_claimed'] += transfer['amount']
                            self.claim_history[address_lower]['claim_count'] += 1
                            self.claim_history[address_lower]['last_claim_block'] = transfer['block']
                            self.claim_history[address_lower]['claim_history'].append({
                                'amount': transfer['amount'],
                                'timestamp': transfer['timestamp'],
                                'block': transfer['block'],
                                'tx_hash': transfer['tx_hash']
                            })
                        self.claim_history[address_lower]['last_scanned_block'] = chunk_end
                        logger.info(f"Found {len(transfers)} new claims for {address} in blocks {chunk_start}-{chunk_end}")
                else:
                    with self.data_lock:
                        self.claim_history[address_lower]['last_scanned_block'] = chunk_end

            self.save_claim_history()

        except Exception as e:
            logger.error(f"Error processing address {address}: {str(e)}")

    def scan_all_addresses(self):
        """Scan all addresses in data.json for new reward claims"""
        current_block = self.web3.eth.block_number
        logger.info(f"Starting scan of all addresses. Current block: {current_block}")

        address_queue = Queue()
        for address in self.addresses:
            address_queue.put(address)

        def worker():
            while not address_queue.empty():
                address = address_queue.get()
                try:
                    self.process_address(address)
                except Exception as e:
                    logger.error(f"Error processing address {address}: {str(e)}")
                finally:
                    address_queue.task_done()

        threads = []
        for _ in range(4):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)

        address_queue.join()
        for t in threads:
            t.join()

        logger.info("Completed scan of all addresses")

def main():
    scanner = RewardScanner()
    scanner.scan_all_addresses()

if __name__ == "__main__":
    main()
