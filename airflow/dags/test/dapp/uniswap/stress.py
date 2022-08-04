# test high number of addresses in eth.get_logs filter
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context, BranchPythonOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from web3 import Web3
from web3.datastructures import AttributeDict
from hexbytes import HexBytes
import os
import pandas as pd
from dap.utils import eth_ip, allocatable_threads
from dap.events.etl import BATCH_LENGTH

# sample data: first 2 Uniswap V3 and single occurences of events in the same block
contracts = [
    '0x1F98431c8aD98523631AE4a59f267346ea31F984',  # Factory
    '0xC36442b4a4522E871399CD717aBDD847Ab11FE88'  # NonfungiblePositionManager
]
epoch, first_block = 412, 12369739  # block of first events from both contracts
# load topics with all there is in subgraph
signatures = [
    'PoolCreated(address,address,uint24,int24,address)',
    'IncreaseLiquidity(uint256,uint128,uint256,uint256)',
    'DecreaseLiquidity(uint256,uint128,uint256,uint256)',
    'Collect(indexed uint256,address,uint256,uint256)',
    'Transfer(indexed address,indexed address,indexed uint256)',
    'Initialize(uint160,int24)',
    'Swap(indexed address,indexed address,int256,int256,uint160,uint128,int24)',
    'Mint(address,indexed address,indexed int24,indexed int24,uint128,uint256,uint256)',
    'Burn(indexed address,indexed int24,indexed int24,uint128,uint256,uint256)',
    'Flash(indexed address,indexed address,uint256,uint256,uint256,uint256)'
]
topics = [[Web3.keccak(text=sig).hex() for sig in signatures]]
filter = {
    'address': contracts,
    'fromBlock': first_block - 999,
    'toBlock': first_block,
    'topics': topics
}
# expected from eth.get_logs
events = [
    AttributeDict({
        'address': '0x1F98431c8aD98523631AE4a59f267346ea31F984', 
        'topics': [
            HexBytes('0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'), 
            HexBytes('0x0000000000000000000000001f9840a85d5af5bf1d1762f925bdaddc4201f984'), 
            HexBytes('0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'), 
            HexBytes('0x0000000000000000000000000000000000000000000000000000000000000bb8')
        ], 
        'data': ('0x000000000000000000000000000000000000000000000000000000000000003c00000'
            '00000000000000000001d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801'), 
        'blockNumber': 12369739, 
        'transactionHash': 
            HexBytes('0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf'), 
        'transactionIndex': 33, 
        'blockHash': 
            HexBytes('0xe8228e3e736a42c7357d2ce6882a1662c588ce608897dd53c3053bcbefb4309a'), 
        'logIndex': 24, 
        'removed': False
    }), AttributeDict({
        'address': '0xC36442b4a4522E871399CD717aBDD847Ab11FE88', 
        'topics': [
            HexBytes('0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f'), 
            HexBytes('0x0000000000000000000000000000000000000000000000000000000000000001')
        ], 
        'data': ('0x00000000000000000000000000000000000000000000000005543a1a83a9ad5800000'
            '00000000000000000000000000000000000000000000de0b6b3a763ffb400000000000000000'
            '0000000000000000000000000000000002ceb7c286cfd24'), 
        'blockNumber': 12369739, 
        'transactionHash': 
            HexBytes('0x37d8f4b1b371fde9e4b1942588d16a1cbf424b7c66e731ec915aca785ca2efcf'), 
        'transactionIndex': 33, 
        'blockHash': 
            HexBytes('0xe8228e3e736a42c7357d2ce6882a1662c588ce608897dd53c3053bcbefb4309a'), 
        'logIndex': 31, 
        'removed': False
    })
]

PARAMS = {
    'eth_client': os.environ['ETHEREUM_CLIENT']
}

def ws():
    eth_client = get_current_context()['dag_run'].conf['eth_client']
    return Web3(Web3.WebsocketProvider(
        f"ws://{eth_ip(eth_client)}:8546", websocket_timeout=60))

def logs(w3, filter):
    return w3.eth.get_logs(filter)

@task()
def validate_sample():
    assert(logs(ws(), filter) == events)

def router(client):
    return {
        1: 'large', 3: 'xlarge', 7: '2xlarge'
    }[allocatable_threads(client)]
@task()
def large():
    pass
@task()
def xlarge():
    pass
@task()
def _2xlarge():
    pass

@task()
def filter_10k_contracts():
    # Uniswap V2 Router, Tether and USDC come on top of the list
    gas_guzzlers = HiveServer2Hook().get_pandas_df(f"""
        WITH tx AS (
            SELECT explode(transactions) AS tx 
            FROM parquet.`s3://{os.environ['DATA_BUCKET']}/blocks/epoch={epoch}`
            WHERE number BETWEEN {first_block} - 999 AND {first_block}
        )
        SELECT tx.to FROM tx WHERE tx.to IS NOT NULL 
        GROUP BY 1 ORDER BY SUM(tx.gas) DESC
        LIMIT 10000
    """)
    contract_noise = gas_guzzlers['to'].to_list()
    assert(len(contract_noise) == 10000)
    many_contracts =  contract_noise + contracts
    loaded_filter = dict(filter, address=many_contracts)
    actual = logs(ws(), loaded_filter)
    print(f'returned events: {len(actual)}')
    assert(actual == events)

@dag(
    schedule_interval=None,
    start_date=days_ago(2),
    params=PARAMS,
    tags=['test', 'uni']
)
def uni_Stress():
    l, xl, _2xl = large(), xlarge(), _2xlarge()
    branch = BranchPythonOperator(
        task_id='router',
        python_callable=router,
        op_args=['{{ params.eth_client }}']
    )
    validate_sample() >> branch
    branch >> l >> [filter_10k_contracts()]
    branch >> xl
    branch >> _2xl

test_dag = uni_Stress()

