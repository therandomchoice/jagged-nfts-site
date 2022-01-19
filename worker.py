import requests
import pandas as pd
import redis
import logging
import dotenv
import pickle
import sys
import os

dotenv.load_dotenv()
etherscan_api_url = 'https://api.etherscan.io/api'
etherscan_api_key = os.getenv('ETHERSCAN_API_KEY')
redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
jagged_address = '0x8888888888E9997E64793849389a8Faf5E8e547C'
fnd_address = '0xcDA72070E455bb31C7690a170224Ce43623d0B6f'
private_selector = '0x6775d96a'     # buyFromPrivateSale(address,uint256,uint256,uint8,bytes32,bytes32)
bid_selector = '0x9979ef45'         # placeBid(uint256)

logging.basicConfig(stream=sys.stdout, format='%(levelname)s %(message)s', level=logging.INFO)
redis_conn = redis.from_url(redis_url)



def load_transactions(from_block):
    params = dict(
        module='account',
        action='txlist',
        address=jagged_address,
        startblock=from_block,
        endblock='latest',
        apikey=etherscan_api_key)

    response = requests.get(etherscan_api_url, params)

    if response.status_code == 200:
        res = pd.DataFrame(response.json()['result'])
        logging.info(f'success tx loading from block {from_block}, {len(res)} tx loaded')
        return res
    else:
        logging.error(f'error on tx loading, code={response.status_code}')



def load_internal_transactions(from_block):
    params = dict(
        module='account',
        action='txlistinternal',
        address=jagged_address,
        startblock=from_block,
        endblock='latest',
        apikey=etherscan_api_key)

    response = requests.get(etherscan_api_url, params)

    if response.status_code == 200:
        res = pd.DataFrame(response.json()['result'])
        logging.info(f'success itx loading from block {from_block}, {len(res)} itx loaded')
        return res
    else:
        logging.error(f'error on itx loading, code={response.status_code}')



def load_ethusd_rate():
    params = dict(
        module='stats',
        action='ethprice',
        apikey=etherscan_api_key)

    response = requests.get('https://api.etherscan.io/api', params)

    if response.status_code == 200:
        ethusd = float(response.json()['result']['ethusd'])
        redis_conn.set('ethusd', ethusd)
        return ethusd

    return float(redis_conn.get('ethusd'))



def update_summary(df, ethusd):
    from_block = df.blockNumber.max()
    tx = load_transactions(from_block)
    itx = load_internal_transactions(from_block)

    usecols = ['blockNumber', 'timeStamp', 'value', 'hash']
    if tx is not None:
        tx = tx[tx.to == fnd_address.lower()][usecols + ['input']].copy()
        tx.blockNumber = tx.blockNumber.astype(int)
        tx.timeStamp = pd.to_datetime(tx.timeStamp, unit='s')
        tx.value = [int(v) / 1e18 for v in tx.value]
        tx['method'] = tx['input'].apply(lambda s: s[:10])

        private = tx[tx.method == private_selector][usecols].copy()
        private['method'] = 'private_auction'

        bid = tx[tx.method == bid_selector].copy()
        bid['auction'] = bid['input'].apply(lambda s: int(s[10:74], 16))
        bid = bid[usecols + ['auction']].copy()
        bid['method'] = 'bid'

        df = df.append(bid, ignore_index=True).append(private, ignore_index=True)

    if itx is not None:
        itx = itx[itx['from'] == fnd_address.lower()][usecols].copy()
        itx.blockNumber = itx.blockNumber.astype(int)
        itx.timeStamp = pd.to_datetime(itx.timeStamp, unit='s')
        itx.value = [-int(v) / 1e18 for v in itx.value]
        itx['method'] = 'outbid'

        df = df.append(itx, ignore_index=True)

    df = df.drop_duplicates().sort_values('blockNumber')
    df['usd'] = df.value * ethusd
    return df



try:
    df = pd.DataFrame(pickle.loads(redis_conn.get('summary')))
except:
    df = pd.read_csv('preloaded.csv', parse_dates=['timeStamp'])
    logging.info('load preloaded.csv')
ethusd = load_ethusd_rate()
df = update_summary(df, ethusd)
redis_conn.set('summary', pickle.dumps(df.to_dict()))
logging.info(f'summary updated, {len(df)} records')
