from dapp.uniswap.factories import FACTORIES, pool_created
from test.dapp.uniswap.constants import TEMPLATE_DATA
from test.dap.events.constants import processed_FACTORY_EVENTS

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from pprint import pprint

PARAMS = {
    'epoch': 412,
    'bucket': os.environ['TEST_BUCKET'],
    'path': 'test/uni/factories'
}

@task()
def historical_events(path):
    conf = get_current_context()['dag_run'].conf
    # handler must merge current with previous epoch events
    epoch = conf['epoch'] - 1
    pq.write_to_dataset(
        pa.Table.from_pandas(pd.DataFrame(processed_FACTORY_EVENTS)),
        f"{conf['bucket']}/{conf['path']}/{path}/epoch={epoch}",
        filesystem=s3fs.S3FileSystem(), compression='SNAPPY',
        partition_filename_cb=lambda _: f'0.parquet.snappy'
    )
    return path

@task()
def test_handler(path):
    conf = get_current_context()['dag_run'].conf
    if path:
        actual = pool_created(f"{conf['path']}/{path}", conf['epoch'], 
            processed_FACTORY_EVENTS, bucket=conf['bucket'])
        expected = TEMPLATE_DATA + TEMPLATE_DATA
    else:
        actual = pool_created('thispathdoesnotexist', conf['epoch'], 
            processed_FACTORY_EVENTS, bucket=conf['bucket'])
        expected = TEMPLATE_DATA
    assert(actual == expected)

@dag(
    params=PARAMS,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['uni', 'test']
)
def uni_Factory_ok():
    test_handler('')  # no partition to lookup on first epoch
    test_handler(historical_events('uniswap/contract=Factory'))

test_dag = uni_Factory_ok()

