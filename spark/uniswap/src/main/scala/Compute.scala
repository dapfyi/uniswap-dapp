package fyi.dap.uniswap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import fyi.dap.sparkubi.functions._

object Compute extends Spark {
    import spark.implicits._

    type C = org.apache.spark.sql.Column
    type SC = Seq[C]
    
    val PAIR = Seq("0", "1")
   
    implicit class ComputeExtension(val df: DataFrame) extends AnyVal {

        def logId = df.withColumn("logId", 'blockNumber + concat(lit("0."), 
            lpad('transactionIndex, 5, "0"), lpad('logIndex, 5, "0")).cast("decimal(25,10)"))
    
        def pairCol(name: String, arg: String, fn: C => C) = PAIR.foldLeft(df) (
            (df, t) => df.withColumn(name+t, fn(col(arg+t)))
        )
        
        def pairCol(name: String, args: Seq[String], fn: SC => C) = PAIR.foldLeft(df) (
            (df, t) => df.withColumn(name+t, fn(args.map(arg => col(arg+t))))
        )
        
        def switchSuffix(c: C) = {
            val name = c.toString
            col(name.take(name.size - 1) + PAIR.filter(_ != name.takeRight(1))(0))
        }
    
        def swapPrice = df.
            withColumn("price1", regexp_replace(ubim('sqrtPriceX96, 'sqrtPriceX96), 
                lit("0b0/"), concat('token1("decimals") - 'token0("decimals"), lit("b192/")))).
            withColumn("price0", ubid(lit("1"), 'price1)).
            withColumn("resolvedSqrtPrice", ubi_resolve(ubid('sqrtPriceX96, 
                /* 2^96 = */ lit("79228162514264337593543950336"))).cast("double"))
        
        def swapVolume = df.
            pairCol("amount", Seq("amount", "token"), 
                (args: SC) => concat_ws("e", args(0), args(1)("decimals"))).
            pairCol("fee", "amount", (arg: C) => when(ubi_sign(arg) > 0, 
                ubim(arg, concat('fee, lit("e6")))).otherwise("0")).
            pairCol("netAmount", Seq("amount", "fee"), 
                (args: SC) => ubis(args(0), args(1))).
            pairCol("volume", "netAmount", (arg: C) => ubi_abs(arg))

        /* tickLiquidity could lose price accuracy through built-in and optimized pow function:
        underlying java.lang.StrictMath operates on Doubles. Rely only on aggregated results. */
        def tickLiquidity = {
   
            def tickToSqrtPrice(tick: C) = pow(1.0001, tick / 2)
         
            val lowerTick = floor('tick / 'tickSpacing) * 'tickSpacing
            val upperTick = lowerTick + 'tickSpacing
           
            // lower and upper prices in square roots 
            val lowerPrice = tickToSqrtPrice(lowerTick)
            val upperPrice = tickToSqrtPrice(upperTick)
            val priceRange = upperPrice - lowerPrice
            
            val sqrtPrice = 'resolvedSqrtPrice
            val liquidity = 'liquidity.cast("double")
            val unscaledAmounts = df.

                // tokens 0 and 1 in current tick
                withColumn("tickAmount0", liquidity * (upperPrice - sqrtPrice) / (sqrtPrice * upperPrice)).
                withColumn("tickAmount1", liquidity * (sqrtPrice - lowerPrice)).

                // total tick liquidity in a single "0" or "1" token amount
                withColumn("tickDepth0", liquidity * priceRange / (lowerPrice * upperPrice)).
                withColumn("tickDepth1", liquidity * priceRange)
            
            Seq("tickAmount", "tickDepth").foldLeft(unscaledAmounts) ((df, amount) => 
                df.pairCol(amount, Seq(amount, "token"), 
                    (args: SC) => args(0) / pow(10, args(1)("decimals"))))
                
        }
        
        def totalFees = df.
            withColumn("totalFees", ubia('gasFees, $"baseAmounts.fee")).
            withColumn("totalFeePct", when(ubi_sign($"baseAmounts.volume")=!=0, 
                ubi_resolve(ubid('totalFees, $"baseAmounts.volume")).cast("double") * 100))
                
        def higherLevelAggregates = df.
            pairCol("priceDelta", Seq("lastPrice", "firstPrice"), 
                (args: SC) => ubis(args(0), args(1))).
            pairCol("priceDeltaPct", Seq("priceDelta", "lastPrice"), 
                (args: SC) => ubi_resolve(ubid(args(0), args(1))).cast("double") * 100).
            pairCol("priceRange", Seq("maxPrice", "minPrice"), 
                (args: SC) => ubis(args(0), args(1))).
            pairCol("volatility", Seq("priceRange", "firstPrice"), (args: SC) => 
                when(ubi_sign(args(0))=!=0, 
                    ubi_resolve(ubid(args(0), args(1))).cast("double") * 100).
                otherwise(0.0)).
            withColumn("volatility", ('volatility0 + 'volatility1) / 2).
            /* Note price impact isn't linear due to underlying factors volume and liquidity.
            Value is to be solely interpreted as a descriptive aggregate of a pool rather than a swap. */
            withColumn("priceImpactPctFor1kUsd", {
                val volume = ubi_resolve('volumeUsd).cast("decimal(38,0)")
                ('ttAbsPriceDeltaPct0 + 'ttAbsPriceDeltaPct1) / 2 / volume * 1000
            })
    
    }

}

