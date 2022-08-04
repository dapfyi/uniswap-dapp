import spark.sql

try {

    println("DaP ~ Uniswap Database")
    sql("CREATE DATABASE IF NOT EXISTS uniswap")
    
    println("DaP ~ rated_swaps Table")
    sql(s"""
        CREATE EXTERNAL TABLE IF NOT EXISTS uniswap.rated_swaps 
        USING DELTA
        LOCATION 's3a://${sys.env("DELTA_BUCKET")}/uniswap/rswaps'
    """)
    
    println("DaP ~ day_swaps Table")
    /* generate column expression:
      val agg = spark.read.parquet(s"""s3://${sys.env("DELTA_BUCKET")}/uniswap/agg""")
      println(agg.schema.toDDL)
    */
    sql(s"""CREATE EXTERNAL TABLE IF NOT EXISTS uniswap.day_swaps(
        `address` STRING,
        `pool` STRING,
        `pair` STRING,
        `swapCount` BIGINT,
        `firstPrice0` DOUBLE,
        `firstPrice1` DOUBLE,
        `lastPrice0` DOUBLE,
        `lastPrice1` DOUBLE,
        `firstPriceLogId` DECIMAL(38,18),
        `lastPriceLogId` DECIMAL(38,18),
        `minPrice0` DOUBLE,
        `minPrice1` DOUBLE,
        `maxPrice0` DOUBLE,
        `maxPrice1` DOUBLE,
        `liquidityWeightedPrice0` DOUBLE,
        `liquidityWeightedPrice1` DOUBLE,
        `ttAbsPriceDeltaPct0` DOUBLE,
        `ttAbsPriceDeltaPct1` DOUBLE,
        `skewness0` DOUBLE,
        `skewness1` DOUBLE,
        `kurtosis0` DOUBLE,
        `kurtosis1` DOUBLE,
        `trend0` DOUBLE,
        `trend1` DOUBLE,
        `volume0` DOUBLE,
        `volume1` DOUBLE,
        `netAmount0` DOUBLE,
        `netAmount1` DOUBLE,
        `volumeBase` DOUBLE,
        `volumeUsd` DOUBLE,
        `avgVolumeBase` DOUBLE,
        `avgVolumeUsd` DOUBLE,
        `medVolumeBase` DOUBLE,
        `medVolumeUsd` DOUBLE,
        `avgTicks` DOUBLE,
        `medTicks` DOUBLE,
        `avgTickDepth0` DOUBLE,
        `avgTickDepth1` DOUBLE,
        `avgGas` DOUBLE,
        `medGas` DOUBLE,
        `tipQ1` DOUBLE,
        `tipQ2` DOUBLE,
        `tipQ3` DOUBLE,
        `avgTotalFeePct` DOUBLE,
        `medTotalFeePct` DOUBLE,
        `avgTotalFeesUsd` DOUBLE,
        `medTotalFeesUsd` DOUBLE,
        `price0Series` ARRAY<STRUCT<
            `logId`: DECIMAL(38,18),
            `time`: TIMESTAMP,
            `price`: DOUBLE,
            `priceDeltaPct`: DOUBLE,
            `netAmount`: DOUBLE,
            `volume`: DOUBLE>>,
        `price1Series` ARRAY<STRUCT<
            `logId`: DECIMAL(38,18),
            `time`: TIMESTAMP,
            `price`: DOUBLE,
            `priceDeltaPct`: DOUBLE,
            `netAmount`: DOUBLE,
            `volume`: DOUBLE>>,
        `costSeries` ARRAY<STRUCT<
            `logId`: DECIMAL(38,18),
            `time`: TIMESTAMP,
            `volumeUsd`: DOUBLE,
            `gas`: BIGINT,
            `gasXTipUsd`: DOUBLE,
            `gasXBaseFeeUsd`: DOUBLE,
            `LPFeeUsd`: DOUBLE,
            `tipGwei`: DOUBLE,
            `baseFeeGwei`: DOUBLE,
            `totalFeePct`: DOUBLE>>,
        `token0` STRUCT<`symbol`: STRING, `decimals`: DECIMAL(38,0), `address`: STRING>,
        `token1` STRUCT<`symbol`: STRING, `decimals`: DECIMAL(38,0), `address`: STRING>,
        `fee` BIGINT,
        `isBasePool` BOOLEAN,
        `priceDelta0` DOUBLE,
        `priceDelta1` DOUBLE,
        `priceDeltaPct0` DOUBLE,
        `priceDeltaPct1` DOUBLE,
        `priceRange0` DOUBLE,
        `priceRange1` DOUBLE,
        `volatility0` DOUBLE,
        `volatility1` DOUBLE,
        `volatility` DOUBLE,
        `priceImpactPctFor1kUsd` DOUBLE)
        PARTITIONED BY (`date` STRING)
        STORED AS parquet
        LOCATION 's3a://${sys.env("DELTA_BUCKET")}/uniswap/agg'
    """)

    println("DaP ~ MSCK Repair")
    sql("MSCK REPAIR TABLE uniswap.day_swaps")

    System.exit(0)

} catch {

    case error: Throwable =>
        println(s"Abnormal termination: $error")
        System.exit(1)

}

