# uniswap-dapp
*Web3 data pipeline deployable on DaP*

This repo is a proof of concept of a DaP pipeline (dapp) and the only one installed on DaP by default. It provides an open supply chain of historical pricing and fees from v3 Uniswap.

**Caution**: this is a raw, actual and transactional data feed; outside analytics, most use cases prefer smoothed and averaged prices.

> See Pipeline link in DaP `doc` folder to configure and install a dapp.

## Datasets

Resulting datasets are partitioned by date and exposed by:
- direct s3 paths configured in Airflow dag parameters;
- the Hive metastore and Spark Thrift Server providing a SQL interface.

Database | Table Name | Resolution | Format
--- | --- | --- | ---
uniswap | rated_swaps | enriched and converted transactions: a row per swap | delta
uniswap | day_swaps | daily aggregate by pool w/ time-series | parquet

Raw contract events are also persisted in the data lake following extraction. All events listed in the official Uniswap [subgraph](https://github.com/Uniswap/v3-subgraph/blob/main/subgraph.yaml) are stored in s3 paths partitioned by contract name.

