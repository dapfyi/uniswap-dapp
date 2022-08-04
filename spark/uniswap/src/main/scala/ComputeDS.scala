package fyi.dap.uniswap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import fyi.dap.sparkubi.Arithmetic.{multiply, divide, subtract}
import fyi.dap.sparkubi.Unary.{resolve, sign, abs}
import fyi.dap.sparkubi.Typecast._
import fyi.dap.sparkubi.Fraction
import Compute._

object ComputeDS extends Spark {
    import spark.implicits._

      val Zero: Fraction = "0"
      val One: Fraction = "1"

    /* no value class due to compiler notice on pimp-my-lib pattern, i.e.
      "Implementation restriction: nested class is not allowed in value class.
       This restriction is planned to be removed in subsequent releases.
       [error]: as[SwapPriceVolumeRow]" */
    implicit class ComputeDSExtension(df: DataFrame) {

        def swapPriceVolumeDS = {

            val ds = df.select($"*", 
                lit(null).as("price1"), 
                lit(null).as("price0"),
                lit(null).as("resolvedSqrtPrice"),
                lit(null).as("fee0"), 
                lit(null).as("fee1"),
                lit(null).as("netAmount0"), 
                lit(null).as("netAmount1"),
                lit(null).as("volume0"), 
                lit(null).as("volume1")
            ).as[SwapPriceVolumeRow]

            ds.map { r => 

                val sqrtPriceX96: Fraction = r.sqrtPriceX96
                val decimals = r.token1.decimals - r.token0.decimals
                val price1 = multiply(sqrtPriceX96, sqrtPriceX96).
                    replace("0b0/", s"${decimals}b192/")
                val price0 = divide(One, price1)
                val resolvedSqrtPrice = resolve(divide(sqrtPriceX96, 
                /* 2^96 = */ "79228162514264337593543950336")).toDouble

                val fee: Fraction = r.fee.toString + "e6"
                val amount0: Fraction = r.amount0
                val amount1: Fraction = r.amount1
                val fee0: Fraction = 
                    if(sign(amount0) > 0) multiply(amount0, fee) else Zero
                val fee1: Fraction = 
                    if(sign(amount1) > 0) multiply(amount1, fee) else Zero

                val netAmount0 = subtract(amount0, fee0)
                val netAmount1 = subtract(amount1, fee1)

                r.copy(
                    price1 = price1, 
                    price0 = price0,
                    resolvedSqrtPrice = resolvedSqrtPrice,
                    fee0 = fee0,
                    fee1 = fee1,
                    netAmount0 = netAmount0,
                    netAmount1 = netAmount1,
                    volume0 = abs(netAmount0),
                    volume1 = abs(netAmount1)
                )

            }.toDF

        }

    }

}

