- epoch reload of historical swaps: `./submit.sh -c reload uniswap 444`\
or `SINK_BUCKET=my-test-bucket ./submit.sh ...` to redirect output to a testing bucket

- daily pool statistics: `./submit.sh -c agg uniswap 2021-09-30`

- Hive table initialization: `./submit.sh -c init uniswap script metadata`

