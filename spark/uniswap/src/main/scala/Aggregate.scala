package fyi.dap.uniswap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import fyi.dap.sparkubi.functions._
import Compute.{PAIR, ComputeExtension}

object Aggregate extends App with Spark {
    import spark.implicits._

    type C = org.apache.spark.sql.Column
    type SC = Seq[C]

    def liquidityWeightedPrice = (args: SC) =>
        when(ubi_sign(ubi_sum('liquidity))=!=0, {
            val tickDepth = args(0).cast("string")
            val price = args(1)
            val tickDepthSum = ubi_sum(tickDepth)
            when(ubi_sign(tickDepthSum)=!=0, ubid(
                ubi_sum(ubim(tickDepth, price)), tickDepthSum))
        }).otherwise("0")
   
    /* trend is extrapolated from calculated population correlation coefficient:
    built-in corr function fails on ArithmeticException: divide by zero */
    def trend = (arg: C) => {
        val price = ubi_resolve(arg).cast("double")
        val denominator = stddev(price) * stddev('timestamp)
        when(denominator=!=0, covar_pop(price, 'timestamp) / denominator)
    }
 
    def pairAgg(name: String, arg: String, fn: C => C) =
        PAIR.map(t => fn(col(arg+t)).as(name+t))

    def pairAgg(name: String, args: Seq[String], fn: SC => C) =
        PAIR.map(t => fn(args.map(arg => col(arg+t))).as(name+t))

    def quartile(n: Int, column: String) = {
        require((1 to 3).contains(n), "quartile must be 1, 2 or 3")
        val percentile = n match {
            case 1 => 0.25
            case 2 => 0.5
            case 3 => 0.75
        }
        expr(s"percentile($column, $percentile)")
    }    

    def doubleExpr(column: String) = s"CAST(ubi_resolve($column) AS double)"
    
    def priceSummary = 
        pairAgg("firstPrice", "price", (arg: C) => expr(s"min_by($arg, logId)")) ++
        pairAgg("lastPrice", "price", (arg: C) => expr(s"max_by($arg, logId)")) ++
        Seq(min('logId).as("firstPriceLogId"), max('logId).as("lastPriceLogId")) ++
        pairAgg("minPrice", "price", (arg: C) => ubi_min(arg)) ++
        pairAgg("maxPrice", "price", (arg: C) => ubi_max(arg)) ++
        pairAgg("liquidityWeightedPrice", 
            Seq("tickDepth", "price"), liquidityWeightedPrice) ++
        pairAgg("ttAbsPriceDeltaPct", "priceDeltaPct", (arg: C) => sum(abs(arg))) ++
        pairAgg("skewness", "priceDeltaPct", (arg: C) => skewness(arg)) ++
        pairAgg("kurtosis", "priceDeltaPct", (arg: C) => kurtosis(arg)) ++
        pairAgg("trend", "price", trend)
    
    def volume =
        pairAgg("volume", "volume", (arg: C) => ubi_sum(arg)) ++
        pairAgg("netAmount", "netAmount", (arg: C) => ubi_sum(arg)) ++
        Seq(ubi_sum($"baseAmounts.volume").as("volumeBase"),
            ubi_sum($"usdAmounts.volume").as("volumeUsd"),
            ubi_avg($"baseAmounts.volume").as("avgVolumeBase"),
            ubi_avg($"usdAmounts.volume").as("avgVolumeUsd"),
            quartile(2, doubleExpr("baseAmounts.volume")).as("medVolumeBase"),
            quartile(2, doubleExpr("usdAmounts.volume")).as("medVolumeUsd"))
            
    def ticks =
        Seq(avg(abs('tickDelta)).as("avgTicks"),
            quartile(2, "abs(tickDelta)").as("medTicks")) ++
        pairAgg("avgTickDepth", "tickDepth", (arg: C) => avg(arg))
        
    def gas = 
        Seq(avg('gas).as("avgGas"),
            quartile(2, "gas").as("medGas"),
            quartile(1, "resolvedTip").as("tipQ1"),
            quartile(2, "resolvedTip").as("tipQ2"),
            quartile(3, "resolvedTip").as("tipQ3"))
    
    def fees =
        Seq(avg('totalFeePct).as("avgTotalFeePct"),
            quartile(2, "totalFeePct").as("medTotalFeePct"),
            ubi_avg('totalFeesUsd).as("avgTotalFeesUsd"),
            quartile(2, doubleExpr("totalFeesUsd")).as("medTotalFeesUsd"))
            
    def price0Series =
        Seq(sort_array(collect_list(struct('logId, 'time, 
            'price0.as("price"), 'priceDeltaPct0.as("priceDeltaPct"),
            'netAmount0.as("netAmount"), 'volume0.as("volume"),
        ))).as("price0Series"))

    def price1Series =
        Seq(sort_array(collect_list(struct('logId, 'time, 
            'price1.as("price"), 'priceDeltaPct1.as("priceDeltaPct"), 
            'netAmount1.as("netAmount"), 'volume1.as("volume")
        ))).as("price1Series"))
            
    def costSeries =
        Seq(sort_array(collect_list(struct('logId, 
            'time,
            'usdAmounts("volume").as("volumeUsd"),
            'gas,
            ubim('gas.cast("string"), $"usdAmounts.tip").as("gasXTipUsd"),
            ubim('gas.cast("string"), $"usdAmounts.baseFeePerGas").as("gasXBaseFeeUsd"),
            'usdAmounts("fee").as("LPFeeUsd"),
            ('resolvedTip * pow(lit(10), 9)).as("tipGwei"), 
            (ubi_resolve('baseFeePerGas).cast("double") * pow(lit(10), 9)).as("baseFeeGwei"),
            'totalFeePct
        ))).as("costSeries"))

    def crossReferences =
        Seq(first('token0).as("token0"), 
            first('token1).as("token1"),
            first('fee).as("fee"),
            first('isBasePool).as("isBasePool"))

    def resolve(stats: DataFrame): DataFrame = Seq(
        "firstPrice0", "firstPrice1", 
        "lastPrice0", "lastPrice1",
        "minPrice0", "minPrice1",
        "maxPrice0", "maxPrice1",
        "liquidityWeightedPrice0", "liquidityWeightedPrice1",
        "netAmount0", "netAmount1",
        "volume0", "volume1",
        "volumeBase", "volumeUsd",
        "avgVolumeBase", "avgVolumeUsd",
        "avgTotalFeesUsd",
        "price0Series", "price1Series",
        "costSeries",
        "priceDelta0", "priceDelta1",
        "priceRange0", "priceRange1"
    ).foldLeft(stats) ((df, c) => c match {

        case name if name.endsWith("Series") =>

            // extract schema of nested struct records in array
            val fields = (df.select(col(c)(0)).schema.fields match { 
                case Array(StructField(_, st: StructType, _, _)) => st 
            }).fields 

            df.withColumn(c, transform(col(c), s => 
                struct(fields.map(f => 
                    if(f.dataType == StringType) 
                        ubi_resolve(s(f.name)).cast("double").as(f.name) 
                    else s(f.name).as(f.name)
                ): _*)))

        case _ => df.withColumn(c, ubi_resolve(col(c)).cast("double"))

    })
            
    def sink(agg: DataFrame) = agg.
        write.
        format("parquet").
        mode("overwrite").
        save(s"s3://$DeltaBucket/uniswap/agg/date=$aggDate")

    val poolStats = spark.read.format("delta").
        load(s"s3://$DeltaBucket/uniswap/rswaps").
        where('date === aggDate).
        withColumn("resolvedTip", ubi_resolve('tip).cast("double")).
        withColumn("time", 'timestamp.cast("timestamp")).
        withColumn("pair", concat_ws("_", $"token0.symbol", $"token1.symbol")).
        groupBy('address, 'pool, 'pair).
        agg(count("*").as("swapCount"),
            priceSummary ++
            volume ++
            ticks ++
            gas ++
            fees ++
            price0Series ++
            price1Series ++
            costSeries ++
            crossReferences
        : _*).higherLevelAggregates
    
    sink(resolve(poolStats))

}

