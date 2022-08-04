package fyi.dap.uniswap

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import Compute._
import fyi.dap.sparkubi.functions._

object Convert extends Spark {
    import spark.implicits._
    
    def convert(amount: Column, base: Column): Column = expr(s"""
        CASE 
        WHEN $base.price is null AND isBasePool = true
        THEN CASE
             WHEN is1 = true
             THEN ubiM($amount, price1)
             ELSE ubiD($amount, price1) 
             END
        WHEN $base.is1 = true 
        THEN ubiM($amount, $base.price) 
        ELSE ubiD($amount, $base.price) 
        END
    """)
    
    def convert(amount: Column): Column = 
        convert(amount, col("base" + amount.toString.takeRight(1)))

    implicit class ConvertExtension(val df: DataFrame) extends AnyVal {

        def exchangeRatePrep = df.
            withColumn("isBasePool", 
                lit(baseToken).isin(Seq('token0("address"), 'token1("address")):_*)).
            withColumn("is1", when('isBasePool, 
                'token1("address")===baseToken)).
            withColumn("pairedToken", when('isBasePool, 
                when('is1, 'token0).otherwise('token1))).
            pairCol("pairedToken", "token", 
                (arg: Column) => when('isBasePool, 'pairedToken).otherwise(arg))

        @deprecated("time window functions do not scale in batch processes", "0.0")
        def baseQuotes = {

            val rollingWindow = Window.orderBy('logId).
                rowsBetween(Window.unboundedPreceding, Window.currentRow)

            // select pool with most recent quote to pick token conversion rate
            def base(window: WindowSpec, order: String) = struct(
                expr(s"max_by(address, $order)").over(window).as("poolAddress"),
                expr(s"max_by(price1, $order)").over(window).as("price"),
                expr(s"max_by(is1, $order)").over(window).as("is1"),
                max(col(order)).over(window).as(order)
            )

            def baseQuote(pairedToken: Column) = 
                base(rollingWindow.partitionBy(pairedToken), "logId")
            def usdBaseQuote = base(rollingWindow, "usdQuoteLogId")

            df.
                pairCol("base", "pairedToken", baseQuote(_: Column)).
                withColumn("usdQuoteLogId", 
                    when(lit(usdToken).isin(Seq('token0("address"), 'token1("address")):_*),
                        'logId)).
                withColumn("usdBase", usdBaseQuote)

        }

        def baseAmounts = {

            val noBaseFee = ubia(convert('fee0), convert('fee1))

            df.
                withColumn("baseAmounts",
                    when('token0("address")===baseToken, map(
                        lit("volume"), 'volume0,
                        lit("volumeBis"), convert('volume1),
                        lit("fee"), ubia('fee0, convert('fee1)))).
                    when('token1("address")===baseToken, map(
                        lit("volume"), 'volume1,
                        lit("volumeBis"), convert('volume0),
                        lit("fee"), ubia(convert('fee0), 'fee1))).
                    // when amount can be computed from either token, pick most recent quote
                    when('base0("logId") >= 'base1("logId") || 'base1.isNull, map(
                        lit("volume"), convert('volume0),
                        lit("volumeBis"), convert('volume1),
                        lit("fee"), noBaseFee)).
                    when('base0("logId") < 'base1("logId") || 'base0.isNull, map(
                        lit("volume"), convert('volume1),
                        lit("volumeBis"), convert('volume0),
                        lit("fee"), noBaseFee)))

        }

        def usdAmounts = df.
            // invert ubiD and ubiM: usd is a paired token in eth base (is1 on eth)
            withColumn("usdAmounts", expr(s"""transform_values(
                map_concat(baseAmounts, map(
                    'gasFees', gasFees, 
                    'tip', tip, 
                    'baseFeePerGas', baseFeePerGas)), 
                (k, v) -> CASE
                    WHEN isBasePool AND 
                        (token0.address = '$usdToken' OR token1.address = '$usdToken')
                    THEN CASE
                         WHEN is1 = true
                         THEN ubiD(v, price1)
                         ELSE ubiM(v, price1) 
                         END
                    WHEN usdBase.is1 = true 
                    THEN ubiD(v, usdBase.price)
                    ELSE ubiM(v, usdBase.price) 
                END)""")).withColumn("totalFeesUsd", 
                    ubia($"usdAmounts.gasFees", $"usdAmounts.fee"))

    }
    
}

