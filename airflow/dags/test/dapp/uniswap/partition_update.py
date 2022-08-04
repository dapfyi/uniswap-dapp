from dapp.uniswap.partition_update import print_date, update_partitions

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from datetime import timedelta
import pandas as pd

NEW_TABLE = 'uniswap_test.new_table_partitioning'
OLD_TABLE = 'uniswap_test.existing_table_partitioning'
PARAMS = {'tables': [NEW_TABLE, OLD_TABLE]}
DAYS_AHEAD=3

@task()
def create_db():
    HiveServer2Hook().run('CREATE DATABASE IF NOT EXISTS uniswap_test')

@task()
def new_table_setup():
    hive = HiveServer2Hook()
    hive.run(f"DROP TABLE IF EXISTS {NEW_TABLE}")
    hive.run(f"CREATE TABLE {NEW_TABLE} (column INT, date STRING) PARTITIONED BY (date)")

@task()
def old_table_setup():
    two_days_ago = print_date(
        get_current_context()['data_interval_end'] - timedelta(days=2))
    hive = HiveServer2Hook()
    hive.run(f"DROP TABLE IF EXISTS {OLD_TABLE}")
    hive.run(f"CREATE TABLE {OLD_TABLE} (column INT, date STRING) PARTITIONED BY (date)")
    hive.run(f"ALTER TABLE {OLD_TABLE} ADD PARTITION (date='{two_days_ago}')")

def date_bounds():
    execution_date = get_current_context()['data_interval_end']
    return execution_date, execution_date + timedelta(days=DAYS_AHEAD)

@task()
def new_table_check():
    start, end = date_bounds()
    dates = pd.date_range(start=start, end=end)
    expected = ['date=' + print_date(t) for t in dates]
    partitions = HiveServer2Hook().get_pandas_df(f'SHOW PARTITIONS {NEW_TABLE}')
    actual = partitions.partition.tolist()
    print(f'ACTUAL: {actual}')
    print(f'EXPECTED: {expected}')
    assert(actual == expected)

@task()
def old_table_check():
    start, end = date_bounds()
    dates = pd.date_range(start=(start - timedelta(days=2)), end=end)
    expected = ['date=' + print_date(t) for t in dates]
    partitions = HiveServer2Hook().get_pandas_df(f'SHOW PARTITIONS {OLD_TABLE}')
    actual = partitions.partition.tolist()
    print(f'ACTUAL: {actual}')
    print(f'EXPECTED: {expected}')
    assert(actual == expected)

@dag(
    params=PARAMS,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['uni', 'test'],
    render_template_as_native_obj=True
)
def uni_Partition_ok():
    new_setup, old_setup = new_table_setup(), old_table_setup()
    new_check, old_check = new_table_check(), old_table_check()
    update = update_partitions('{{ params.tables }}')
    create_db() >> [new_setup, old_setup] >> update >> [new_check, old_check]

test_dag = uni_Partition_ok()

