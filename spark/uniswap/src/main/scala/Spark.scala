package fyi.dap.uniswap

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

trait Spark extends Serializable {

    // max recent blocks to derive last price quotes and exchange rates
    val BlockExpiry = 10000
    // used in Source to calculate oldest block to include in previous epoch
    val EpochLength = 30000
    val ETH_ADDRESS  = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    val USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    val baseToken = ETH_ADDRESS
    val usdToken  = USDC_ADDRESS

    val logger = Logger.getRootLogger

    val settings = Seq(
        // use the S3A file system for s3 urls
        ("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),

        /* RocksDB disk pressure tends to get out of hand in stateful unit tests:
               set through Spark submit instead.

        ("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"),
        */

        /* see note in https://spark.apache.org/docs/3.2.0/ss-migration-guide.html
          assert statements in uniswap stateful operators already forbid late records */
        ("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false"))

    val conf = new SparkConf().setAll(settings)
    lazy val spark = SparkSession.builder.
        appName("uniswap").
        config(conf).
        getOrCreate

    val DataBucket = sys.env("DATA_BUCKET")
    lazy val DeltaBucket = spark.conf.get("spark.driver.dap.sinkBucket")
    lazy val aggDate = spark.conf.get("spark.driver.dap.date")
    lazy val epoch = spark.conf.get("spark.driver.dap.epoch").toInt
    lazy val isLiveStreaming = epoch == -1

}

