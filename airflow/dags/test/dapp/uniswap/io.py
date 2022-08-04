from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.param import Param
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
import os
import pandas as pd
from web3 import Web3
from dap.utils import eth_ip
from dap.constants import EPOCH_LENGTH
from test.dap.events.constants import FACTORY_SRC
import dap.events.etl as etl
from dapp.uniswap.factories import pool_created

PARAMS = {
    'date': Param(default='2021-09-30', 
        type='string', pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'),
    'epoch': Param(444, type="integer", minimum=412),
    # specific check applied to benchmark_in: output swaps matching input
    'benchmark_in': f"{os.environ['DATA_BUCKET']}/uniswap/shards=0",
    'challenger_in': f"{os.environ['TEST_BUCKET']}/uniswap/shards=0"
    # reconciliation between benchmark_out and challenger_out to be added ->
    # on all-time (trend) aggregate for coverage:
    # reconciliation('{{ params.date }}', '{{ params.benchmark_out }}', 
    #     '{{ params.challenger_out }}', period='date')
}

@task()
def reconciliation(epoch, benchmark, challenger, period='epoch'):
    hive = HiveServer2Hook()
    def count(set, epoch):
        return hive.get_pandas_df(f"""
            SELECT count(*) FROM parquet.`s3://{set}` WHERE {period} = {epoch}
        """).squeeze()
    benchmark_rows = count(benchmark, epoch)
    print(f'{benchmark_rows} rows in {benchmark}')
    challenger_rows = count(challenger, epoch)
    print(f'{challenger_rows} rows in {challenger}')
    assert(challenger_rows == benchmark_rows)
    distinct_records = hive.get_pandas_df(f"""
        SELECT count(DISTINCT *) FROM (
            SELECT * FROM parquet.`s3://{benchmark}` WHERE epoch = {epoch}
            UNION
            SELECT * FROM parquet.`s3://{challenger}` WHERE epoch = {epoch}
        )
    """).squeeze()
    # if a distinct union of 2 sets gives the same row count as these sets:
    # they are identical
    assert(benchmark_rows == distinct_records)

# verify that all swaps in raw data are also in output table on a given day
@task()
def swap_in_out(benchmark, date):
    io_count = HiveServer2Hook().get_pandas_df(f"""
        WITH output AS (
            SELECT sum(swapCount) AS swaps
            FROM uniswap.day_swaps WHERE date = '{date}'
        ), blocks AS (
            SELECT min(number) AS first_block, max(number) AS last_block,
                   min(epoch) AS start_epoch, max(epoch) AS end_epoch
            FROM parquet.`s3://{os.environ['DATA_BUCKET']}/blocks`
            WHERE date = '{date}'
        )
        SELECT count(*) AS input_swaps, (SELECT swaps FROM output) AS output_swaps
        FROM parquet.`s3://{benchmark}/contract=Pool` 
        WHERE epoch BETWEEN  -- filter partition 
            (SELECT start_epoch FROM blocks) AND (SELECT end_epoch FROM blocks)
        AND blockNumber BETWEEN (SELECT first_block FROM blocks) AND (SELECT last_block FROM blocks)
        AND event = 'Swap'
    """)
    # literal string aligns headers with data on a new line
    print(f"""
{io_count.to_string()}""")
    pd.testing.assert_series_equal(io_count.input_swaps, io_count.output_swaps, check_names=False)

# analytical check: a pool created by the factory should be initialized by its own contract
# this test indicates whether events of pools created within the processed epoch are included
# a failure is most probably a factory handler issue (can go unnoticed without this check)
CLIENT = os.environ['ETHEREUM_CLIENT']
@task(pool=CLIENT, pool_slots=2)
def new_pool_events(epoch, sets):
    new_pools = etl.lookahead(epoch, FACTORY_SRC, 'PoolCreated', {
        'eth_client': CLIENT, 'dapp': 'uniswap'
    })
    batch = etl.process_batch(new_pools)
    w3 = Web3(Web3.WebsocketProvider(f'ws://{eth_ip(CLIENT)}:8546', websocket_timeout=60))
    for set in sets:
        bucket = set.split('/')[0]
        contracts = [p['address'] for p in pool_created(bucket, 'nopath', epoch, batch)]
        filter = {
            'address': contracts, 
            'fromBlock': epoch * EPOCH_LENGTH, 
            'toBlock': (epoch + 1) * EPOCH_LENGTH - 1,
            'topics': [[Web3.keccak(text='Initialize(uint160,int24)').hex()]]
        }
        logs = w3.eth.get_logs(filter)
        len_logs = len(logs) 
        print(f'{len_logs} out of the {len(contracts)} created pools have been initialized')
        actual = HiveServer2Hook().get_pandas_df(f"""
            SELECT count(*) FROM parquet.`s3://{set}/contract=Pool`
            WHERE epoch = {epoch} and event = 'Initialize'
        """).squeeze()
        print(f'found {actual} Initialize events in {set} epoch {epoch}')
        assert(actual == len_logs)

@dag(
    params=PARAMS,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['uni', 'check'],
    render_template_as_native_obj=True
)
def uni_io():
    reconciliation('{{ params.epoch }}', 
        '{{ params.benchmark_in }}', '{{ params.challenger_in }}')
    swap_in_out('{{ params.benchmark_in }}', '{{ params.date }}')
    new_pool_events('{{ params.epoch }}', 
        ['{{ params.benchmark_in }}', '{{ params.challenger_in }}'])

test_dag = uni_io()

