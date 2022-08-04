from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from datetime import datetime, timedelta
import pandas as pd
import math

PARAMS = {
    'tables': [
        'uniswap.day_swaps'
    ]
}
DATE_PATTERN = '%Y-%m-%d'

def print_date(date):
    return datetime.strftime(date, DATE_PATTERN)

@task()
def update_partitions(tables):
    # inputs to pd.date_range() must have the same timezone: set to None
    exec_date = get_current_context()['data_interval_end'].replace(tzinfo=None)
    last_part = exec_date + timedelta(days=3)  # add partitions 3 days ahead
    print(f'DaP ~ last partition will be {print_date(last_part)}')

    hive = HiveServer2Hook()
    for table in tables:
        partitions = hive.get_pandas_df(f'SHOW PARTITIONS {table}')
        max_part = partitions.partition.str.split('=').str[-1].max()
        # float NaN indicates that there is no partition, max_part is a string otherwise
        if str(max_part) == 'nan':
            next_part = exec_date
        else:
            next_part = datetime.strptime(max_part, DATE_PATTERN) + timedelta(days=1)
        print(f'DaP ~ next {table} partition is {print_date(next_part)}')
        for day in pd.date_range(start=next_part, end=last_part):
            date = print_date(day)
            hive.run(f"ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (date='{date}')")

@dag(
    default_args={
        'retries': 1
    },
    params=PARAMS,
    schedule_interval='21 4 * * *',
    catchup=False,
    start_date=days_ago(1),
    tags=['uni', 'job'],
    render_template_as_native_obj=True
)
def uni_Partition():
    update_partitions('{{ params.tables }}') 

update_dag = uni_Partition()

