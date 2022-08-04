import os
import pyarrow.parquet as pq
import s3fs
import pandas as pd
import json
import logging

logger = logging.getLogger('airflow.task')

def pool_created(bucket, path, epoch, batch):

    try:
        factory = pq.ParquetDataset(
            f'{bucket}/{path}',
            filters=[
                ('epoch', '<', epoch), 
                ('event', '=', 'PoolCreated')
            ],
            filesystem=s3fs.S3FileSystem(),
            use_legacy_dataset=False
        ).read().to_pandas()
        # drop epoch partition to merge with batch
        old_pools = factory.drop('epoch', axis=1)
        del factory
        logger.info(f"""sample of old pools: 
{old_pools.head().to_string()}""")
    except FileNotFoundError:
        logger.info(f'FileNotFoundError at {bucket}/{path}')
        logger.warning(f'assuming no factory record before epoch {epoch}')
        old_pools = pd.DataFrame({})

    new_pools = pd.DataFrame(batch)
    logger.info(f"""sample of new pools: 
{new_pools.head().to_string()}""")

    pools = pd.concat([old_pools, new_pools])
    # replace the factory contract address with pool address
    pools['address'] = pools.args.apply(lambda a: json.loads(a)['pool'])
    # remove latest pools where address isn't known, yet
    # pools = pools[pools.address.notna()]
    pools.rename(columns={'blockNumber': 'startBlock'}, inplace=True)
    pools['name'] = 'Pool'  # template name in subgraph.yaml

    return pools[['address', 'startBlock', 'name']].to_dict('records')

FACTORIES = {
    # data source name in subgraph.yaml
    'Factory': {
        # used in epoch lookahead to filter factory event in contract:
        # e.g. `contract.events[event].createFilter(...)` where event = 'PoolCreated'
        'event': 'PoolCreated',
        'handler': pool_created
    }
}

