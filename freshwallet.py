import asyncio
import hashlib
import json
import logging
import os
import time
import aiohttp
from pymongo import MongoClient, IndexModel, ASCENDING
from telegram import Bot
from telegram.ext import Updater
from web3 import Web3
from web3.middleware import geth_poa_middleware  # Import middleware
from tenacity import retry, stop_after_attempt, wait_exponential
from motor.motor_asyncio import AsyncIOMotorClient


        

# Setup Logging
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(), logging.FileHandler("bot.log")])
logger = logging.getLogger(__name__)

# Load Environment Variables
from dotenv import load_dotenv
load_dotenv()

INFURA_URL = os.getenv('INFURA_URL_BOT2')
DB_URI = os.getenv('DB_URI')
TELEGRAM_BOT_TOKEN_BOT2 = os.getenv('TELEGRAM_BOT_TOKEN_BOT2')
TELEGRAM_CHAT_ID_BOT2 = os.getenv('TELEGRAM_CHAT_ID_BOT2')
ETHERSCAN_API_KEY_BOT2 = os.getenv('ETHERSCAN_API_KEY_BOT2', 'ETHERSCAN_API_KEY_BOT3')

# Global Variables

client = None
db = None
wallets_collection = None
contracts_collection = None
ABI_CACHE = {}
ERC20_CACHE = {}  # Add a new cache for ERC20 checks

telegram_bot = Bot(token=TELEGRAM_BOT_TOKEN_BOT2)

updater = Updater(TELEGRAM_BOT_TOKEN_BOT2)
dispatcher = updater.dispatcher

class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = 0
        self.time_window = 0

    async def __aenter__(self):
        current_time = asyncio.get_running_loop().time()
        if current_time - self.time_window > self.period:
            self.time_window = current_time
            self.calls = 0
        self.calls += 1
        if self.calls > self.max_calls:
            sleep_time = self.period - (current_time - self.time_window)
            await asyncio.sleep(sleep_time)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

class CustomSemaphore:
    def __init__(self, value):
        self._semaphore = asyncio.Semaphore(value)

    async def __aenter__(self):
        await self._semaphore.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()

rate_limiter = RateLimiter(max_calls=10, period=1)
semaphore = CustomSemaphore(5)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def request_infura(url):
    async with rate_limiter, semaphore:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 429:
                    print("Rate limit exceeded, retrying...")
                    raise Exception("Rate limit exceeded")
                response.raise_for_status()
                return await response.json()
        
class BlockchainMonitor:
    CREATION_TIMESTAMP_CACHE = {}
    def __init__(self):
        self.w3 = None  # move w3 to an instance variable
        self.alerted_contracts = set()  # Initialize an empty set to keep track of alerted contracts
        self.tasks = []
        self.one_hour_ago = time.time() - 3600  # Now one_hour_ago is an attribute of the class

    async def initialize(self):
        await self.connect_to_blockchain()
        await self.initialize_mongo()

    async def connect_to_blockchain(self):
        self.w3 = Web3(Web3.HTTPProvider(INFURA_URL))
        if self.w3.is_connected():
            self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            logger.info("Connected to the blockchain")
        else:
            logger.error("Connection failed")
            raise ConnectionError("Failed to connect to the blockchain")

    
    async def initialize_mongo(self):
        self.client = AsyncIOMotorClient(DB_URI)
        self.db = self.client['erc20_monitor']
        self.wallets_collection = self.db['wallets']
        self.contracts_collection = self.db['contracts']

        index_model_wallets = [IndexModel([("wallet_address", ASCENDING)], background=True),
                       IndexModel([("contract_address", ASCENDING)], background=True),
                       IndexModel([("timestamps", ASCENDING)], background=True)]

        await self.wallets_collection.create_indexes(index_model_wallets)

        




    async def periodic_purge(self):  # Added async
        logger.info("Periodic purge started")
        try:
            while True:
                two_hours_ago = time.time() - 7200
                await self.wallets_collection.delete_many({'timestamps.0': {'$lt': two_hours_ago}})  # use self.wallets_collection
                await asyncio.sleep(7200)  # Maintained the sleep duration to 7200 seconds (2 hours)
        except Exception as e:
            logger.exception(f"Error in periodic_purge: {e}")

    async def monitor_blocks(self):  # Change this line to make monitor_blocks async
        logger.info("Monitor blocks started")
        try:
            while True:
                latest_block_number = self.w3.eth.block_number  # Changed w3 to self.w3
                await self.process_block(latest_block_number)  # Added await here
                await asyncio.sleep(10)  # Adjusted sleep duration to 10 seconds
        except Exception as e:
            logger.exception(f"Error in monitor_blocks: {e}")

    async def start(self):
            try:
                purge_task = asyncio.create_task(self.periodic_purge())
                monitor_task = asyncio.create_task(self.monitor_blocks())
                self.tasks.append(purge_task)
                self.tasks.append(monitor_task)
                await asyncio.gather(*self.tasks)
            except Exception as e:
                logger.exception(f"An error occurred: {e}")

    async def stop(self):
            # Cancel all tasks
            for task in self.tasks:
                task.cancel()
            # Wait for all tasks to be cancelled
            await asyncio.gather(*self.tasks, return_exceptions=True)  


    async def block_is_available(self, block_number):
                try:
                    self.w3.eth.get_block(block_number)  # Add await here
                    return True
                except Exception as e:  # Catch exceptions to log the error
                    logger.error(f"Error fetching block {block_number}: {e}")
                    raise  # re-raise the exception to trigger retry 
 
    async def is_known_erc20_contract(self, contract_address):
        contract_address = Web3.to_checksum_address(contract_address)

        return ERC20_CACHE.get(contract_address, None)

    async def process_block(self, block_number):
        block_number = int(block_number)
        logger.info(f"Processing block {block_number}")
        try:
            if not await self.block_is_available(block_number):
                logger.warning(f"Block {block_number} not available yet. Retrying...")
                await asyncio.sleep(2.75)  # Adding a delay before retrying
                return await self.process_block(block_number)  # Recursive retry

            # Create filters for the ERC20 Transfer and Approval events
            erc20_transfer_event_signature = "Transfer(address,address,uint256)"
            erc20_approval_event_signature = "Approval(address,address,uint256)"
            transfer_event_filter = self.w3.eth.filter({
                "fromBlock": block_number,
                "toBlock": block_number,
                "topics": [self.w3.keccak(text=erc20_transfer_event_signature).hex()]
            })
            logger.debug(f"Transfer event filter: {transfer_event_filter}")  # Added logging

            approval_event_filter = self.w3.eth.filter({
                "fromBlock": block_number,
                "toBlock": block_number,
                "topics": [self.w3.keccak(text=erc20_approval_event_signature).hex()]
            })
            logger.debug(f"Approval event filter: {approval_event_filter}")  # Added logging

            
            loop = asyncio.get_running_loop()
            transfer_events = await loop.run_in_executor(None, transfer_event_filter.get_new_entries)
            logger.debug(f"Transfer events: {transfer_events}")  # Added logging

            approval_events = await loop.run_in_executor(None, approval_event_filter.get_new_entries)
            logger.debug(f"Approval events: {approval_events}")  # Added logging

            # Combine the events for further processing
            erc20_events = transfer_events + approval_events

            for event in erc20_events:
                contract_address = event['address']
                contract_address = Web3.to_checksum_address(contract_address)  # Normalize the address

            # Extracting the wallet address from the topics and normalizing it
                wallet_address = event['topics'][1].hex()
                wallet_address = Web3.to_checksum_address(wallet_address[-40:])  # Extract last 40 characters and normalize

                timestamp = self.w3.eth.get_block(event['blockNumber'])['timestamp']

                # Check if the wallet is fresh
                transaction_count = self.w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address), block_identifier=block_number)
                if transaction_count <= 15:
                    # This is a fresh wallet
                    existing_wallet_entry = await self.wallets_collection.find_one({'wallet_address': wallet_address, 'contract_address': contract_address})  # Added await
                    if existing_wallet_entry is None:
                        # Check if the contract was created within the last 2 hours
                        creation_timestamp = await self.get_contract_creation_timestamp(contract_address)  # Added await here
                        if creation_timestamp is None:
                            logger.warning(f"Could not determine creation timestamp for contract: {contract_address}")
                            continue  # Skip this contract if the creation timestamp could not be determined

                        two_hours_ago = time.time() - 7200  # Corrected from 86400 to 7200 for 2 hours
                        if creation_timestamp < two_hours_ago:
                            logger.warning(f"Contract {contract_address} was created more than 2 hours ago. Skipping.")
                            continue  # Skip this contract if it was created more than 2 hours ago

                        # Save fresh wallet
                        await self.wallets_collection.insert_one({
                            'wallet_address': wallet_address,
                            'contract_address': contract_address,
                            'timestamps': [timestamp]
                        })
                        # Check if the alert condition is met for this contract
                        fresh_wallet_count = await self.wallets_collection.count_documents({'contract_address': contract_address})
                        if fresh_wallet_count >= 7:
                            # Check if this contract address has not been alerted before
                            if contract_address not in self.alerted_contracts:
                                await self.alert(contract_address)
                                self.alerted_contracts.add(contract_address)  # Mark this contract as alerted
                    await self.purge_old_entries()  # Added await

        except Exception as e:
            logger.error(f"Error processing block {block_number}: {e}")




    async def get_contract_creation_timestamp(self, contract_address):
        try:
            etherscan_api_url = f"https://api.etherscan.io/api?module=account&action=txlist&address={contract_address}&sort=asc&apikey={ETHERSCAN_API_KEY_BOT2}"
            response_data = await request_infura(etherscan_api_url)
            async with aiohttp.ClientSession() as session:
                async with session.get(etherscan_api_url) as response:
                    response.raise_for_status()
                    response_data = await response.json()
                    if response_data.get('status') == '1':
                        transactions = response_data.get('result', [])
                        if transactions:
                            creation_tx = transactions[0]
                            creation_block_number = int(creation_tx.get('blockNumber', 0))
                            creation_block = self.w3.eth.get_block(creation_block_number)  # Removed await here
                            return creation_block['timestamp']
                        else:
                            logger.warning(f"No transactions found for contract: {contract_address}")
                    else:
                        logger.warning(f"Failed Etherscan API request for contract: {contract_address}")
        except Exception as e:
            logger.error(f"Error fetching contract creation timestamp: {e}")
        return None









    async def get_contract_info(self, contract_address):
            contract_address = Web3.to_checksum_address(contract_address)
            abi = await get_abi(contract_address)
            if abi is None:  # Handle None vatlue for abi
                return None
            contract = self.w3.eth.contract(address=contract_address, abi=abi)
            return contract

        
    async def is_erc20_contract(self, contract_address):
        try:
            if contract_address in ERC20_CACHE:
                return ERC20_CACHE[contract_address]

            if not Web3.is_address(contract_address):
                logger.debug(f"Invalid address: {contract_address}")  # Change to debug level
                ERC20_CACHE[contract_address] = False  # Cache the result
                return False

            try:
                contract_info = await self.get_contract_info(contract_address)
                if contract_info is None:
                    logger.debug(f"Could not get contract info for address {contract_address}")  # Change to debug level
                    ERC20_CACHE[contract_address] = False  # Cache the result
                    return False

                required_functions = {'totalSupply', 'balanceOf', 'transfer', 'transferFrom', 'approve', 'allowance'}
                contract_functions = {function['name'] for function in contract_info.functions.abi if 'name' in function}
                is_erc20 = required_functions.issubset(contract_functions)
                ERC20_CACHE[contract_address] = is_erc20  # Cache the result
                return is_erc20
            except Exception as e:
                logger.debug(f"Error checking if contract is ERC-20: {e}")  # Change to debug level
                ERC20_CACHE[contract_address] = False  # Cache the result
                return False  # Assume it's not an ERC-20 contract if there's an error
        except Exception as e:
            logging.warning(f"Exception while checking if {contract_address} is an ERC-20 contract: {e}")
            return False

        
    async def purge_old_entries(self):  # Added async
            self.one_hour_ago = time.time() - 3600  # Update one_hour_ago each time the method is called
            old_entries = await self.wallets_collection.find({'timestamps': {'$lt': self.one_hour_ago}}).to_list(None)  # use await and self.wallets_collection
            for entry in old_entries:
                await self.wallets_collection.delete_one({'_id': entry['_id']})  # use self.wallets_collection

    async def alert(self, contract_address):
        logger.info(f"Sending alert for contract {contract_address}")
        try:
            name = await get_token_name(contract_address)
            message_text = f"Detected 10+ fresh wallets aping {name}. [Etherscan](https://etherscan.io/token/{contract_address}) | [Dexscreener](https://dexscreener.com/ethereum/{contract_address})"
            await telegram_bot.send_message(chat_id=TELEGRAM_CHAT_ID_BOT2, text=message_text, parse_mode='Markdown')  # if send_message is async
        except Exception as e:
            logger.error(f"Error sending Telegram alert: {e}")

# Utility function to get method identifier
def get_method_identifier(method_signature):
    return Web3.keccak(text=method_signature)[:4].hex()

async def get_abi(contract_address):
    if contract_address in ABI_CACHE:
        logger.info(f"Using cached ABI for {contract_address}")
        return ABI_CACHE[contract_address]

    etherscan_api_url = f"https://api.etherscan.io/api?module=contract&action=getabi&address={contract_address}&apikey={ETHERSCAN_API_KEY_BOT2}"

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(etherscan_api_url) as response:
                response.raise_for_status()  # This will raise an HTTPError for bad responses (4xx and 5xx)
                response_data = await response.json()
                logger.info(f'Successful API call to get_abi for {contract_address}')  # Added logging statement
        except aiohttp.ClientError as e:
            logger.error(f"Network error: {e}")  # Changed logger.debug to logger.error for better visibility
            return None  # Return None instead of raising an exception

    if response_data.get('status') != '1' or response_data.get('message') != 'OK':
        logger.error(f"Etherscan error for contract {contract_address}: {response_data.get('message', 'Unknown error')}")  # Changed logger.debug to logger.error for better visibility
        return None  # Return None instead of raising an exception

    abi = response_data['result']
    if isinstance(abi, str):
        try:
            abi = json.loads(abi)
        except json.JSONDecodeError:
            logger.error(f"Invalid ABI format for contract {contract_address}")
            return None  # Return None instead of raising an exception

    if not validate_abi(abi):
        logger.error(f"Invalid ABI for {contract_address}")
        return None  # Return None instead of raising an exception

    ABI_CACHE[contract_address] = abi  # Store valid ABI in the cache
    return abi

def validate_abi(abi):
    # Ensure ABI is a list as expected
    if not isinstance(abi, list):
        logger.error("ABI is not a list")
        return False
    
    # A simple check to ensure each item in the ABI has a 'type' field
    for item in abi:
        if 'type' not in item:
            logger.error(f"Missing 'type' field in ABI item: {json.dumps(item, indent=2)}")  # Log the problematic item
            return False

    return True

def calculate_functions_checksum(abi):
    abi_str = json.dumps(abi, sort_keys=True)  # Convert JSON to string and sort keys for consistent hashing
    return hashlib.sha256(abi_str.encode()).hexdigest()  # Hash the string representation of the ABI


async def get_token_name(contract_address):
    async with aiohttp.ClientSession() as session:
        try:
            etherscan_api_url = f"https://api.etherscan.io/api?module=token&action=tokeninfo&contractaddress={contract_address}&apikey={os.getenv('ETHERSCAN_API_KEY')}"
            async with session.get(etherscan_api_url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['status'] == '1':
                        return data['result']['tokenName']
                    else:
                        logger.warning(f"Failed to retrieve token name: {data.get('message', 'Unknown error')}")
                else:
                    logger.warning(f"Failed to retrieve token name. HTTP status: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching token name: {e}")
        return "Unknown Token"  # Default if the name can't be retrieved




async def main():
    monitor = BlockchainMonitor()
    await monitor.initialize()
    await monitor.start() 

if __name__ == "__main__":
    asyncio.run(main())