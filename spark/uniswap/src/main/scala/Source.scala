package fyi.dap.uniswap

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import fyi.dap.sparkubi.functions._

object Source extends Spark {
    import spark.implicits._
    
    def pools = {
        
        val pools = spark.read.parquet(s"s3://$DataBucket/uniswap/shards=0").
            where('contract==="Factory").
            where('event==="PoolCreated")

        val tokens = spark.read.parquet(s"s3://$DataBucket/tokens/erc20").
            withColumn("token", struct('symbol, 'decimals, 'address)).
            select('token)

        val argsSchema = schema_of_json(pools.select('args).first.getString(0))
        pools.withColumn("map", from_json('args, argsSchema)).
            join(tokens.withColumnRenamed("token", "token0"), 
                'map("token0")==='token0("address"), "left").
            join(tokens.withColumnRenamed("token", "token1"), 
                'map("token1")==='token1("address"), "left").
            withColumn("pool", concat_ws("_", 'token0("symbol"), 
                'token1("symbol"), ('map("fee")/100).cast("int"))).
            select(
                'map("pool").as("address"), 
                'pool, 
                'map("fee").as("fee"), 
                'map("tickSpacing").as("tickSpacing"), 
                'token0, 
                'token1)
    
    }
    
    def transactions(epoch: Int, blockExpiry: Long) = {

        def blocks(epoch: Tuple2[Int, Int], blockExpiry: Long) = {
            val (partition, index) = epoch
            
            // Treat each epoch as a standalone dataset to reconcile schemas pre/post EIP-1559.
            // I.e. where('epoch===partition) would throw spark.sql.AnalysisException: 
            // cannot resolve 'baseFeePerGas'. 
            val df = spark.read.parquet(s"s3://$DataBucket/blocks/epoch=$partition").
                withColumnRenamed("number", "blockNumber")
                
            val epochStart = partition * EpochLength
            val lookback = if(index == 0) 
                df.where('blockNumber >= epochStart - blockExpiry) else df

            val LondonUpgradeBlock = 12965000
            if(LondonUpgradeBlock > epochStart)
                // placeholder to bypass spark.sql.AnalysisException before London upgrade
                // side-effect: baseFeePerGas will only be available next epoch after fork
                lookback.withColumn("baseFeePerGas", lit(null))
            else lookback
        }

        def expand(blocks: DataFrame) = blocks.
            select(col("*"), explode('transactions).as("transaction")).
            select(    
                'blockNumber,
                'timestamp,
                'transaction("hash").as("transactionHash"),
                'transaction("gas").as("gas"),
                concat('transaction("gasPrice"), lit("e18")).as("gasPrice"),
                concat('baseFeePerGas, lit("e18")).as("baseFeePerGas")).
            withColumn("gasFees", ubim('gas.cast("string"), 'gasPrice)).
            withColumn("tip", ubis('gasPrice, 'baseFeePerGas))
        
        /* bypass Spark partition filter detecting inconsistency pre/post EIP-1559:
           load epochs separately, normalize baseFee when unfolding transactions and combine */
        (epoch - 1 to epoch).zipWithIndex.map(t => 
            expand(blocks(t, blockExpiry))).
        reduce(_ union _)
    }

    def swapArgsJsonSchema(decimalType: String = "string") = s"""
        {"type":"struct","fields":[
            {"name":"amount0","type":"$decimalType","nullable":true},
            {"name":"amount1","type":"$decimalType","nullable":true},
            {"name":"liquidity","type":"$decimalType","nullable":true},
            {"name":"recipient","type":"string","nullable":true},
            {"name":"sender","type":"string","nullable":true},
            {"name":"sqrtPriceX96","type":"$decimalType","nullable":true},
            {"name":"tick","type":"long","nullable":true}
        ]},"nullable":true}]}
    """

    def deserialize_json(jsonCol: Column, jsonSchema: String) = {
        val schema = DataType.fromJson(jsonSchema).asInstanceOf[StructType]
        (from_json(jsonCol, schema), schema)
    }

    def swaps(epoch: Int, blockExpiry: Long) = {

        val (argsMap, argsSchema) = deserialize_json('args, swapArgsJsonSchema())

        spark.read.parquet(s"s3://$DataBucket/uniswap/shards=0").
            where('contract==="Pool").where('epoch.between(epoch - 1, epoch)).
            where('blockNumber >= (epoch - 1) * EpochLength - blockExpiry).
            where('event==="Swap").
            withColumn("map", argsMap).
            select("*", argsSchema.fieldNames.map("map."+_):_*).
            drop("args", "map")

    }

}

