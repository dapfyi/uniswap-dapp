import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import fyi.dap.uniswap.{Token, Base}
import fyi.dap.uniswap.Convert._
import fyi.dap.sparkubi.functions._

class ConvertSuite extends FunSuite with TestingBase {

    test("base pool swap converts paired token fee with its own price") {
        import spark.implicits._

        val convertedAmount = Seq((true, true, "1", "2")).
            toDF("isBasePool", "is1", "fee0", "price1").
            withColumn("base0", struct(
                lit(null).as("price"),
                lit(null).as("is1"))).
            select(convert('fee0)).
            first.getString(0)

        assert(convertedAmount == "2e0b0/1e0b0")
    }

    test("correct operation (* or /) picked by both ETH and USD conversions") {
        import spark.implicits._

        val df = Seq(
            (  // data transaction hash: 1AF3EEA5DC4218AAD011383F7BF63C8C105BD121A15DF89F1AEE6FDBB24F06C3
                Token(symbol = "WBTC", decimals = 8L, address = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"),
                Token(symbol = "USDT", decimals = 6L, address = "0xdAC17F958D2ee523a2206206994597C13D831ec7"),
                "1e2b0/2645809970288966871220430322152061218317819773293375026117264e0b192",  // price0
                "2645809970288966871220430322152061218317819773293375026117264e-2b192/1e0b0",  // price1
                "6099198000e14b0/1e0b0", "0",  // fee0 & 1 
                "2026966802000e14b0/1e0b0", "854403634e6b0/1e0b0",  // volume0 & 1
                Base(poolAddress = "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0", 
                    price = "907334895169304099592376346996265340613152495870046552845371787198225e10b192/1e0b0", 
                    is1 = true, logId = BigDecimal("13320818.000860013300000000")),
                Base(poolAddress = "0x11b815efB8f581194ae79006d24E0d814B7697F6", 
                    price = "18322227182269616037841953581097896229670063757649e-12b192/1e0b0", 
                    is1 = false, logId = BigDecimal("13320829.001810022500000000")),
                Base(poolAddress =" 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
                    price = "2148930618079446334171198021437466595445357402245217269710901634724e12b192/1e0b0", 
                    is1 = true, logId = BigDecimal("13320830.000340003800000000")),
                "18608586517127222e18b0/1e0b0", "1825346000e18b0/1e0b0", "104818396254e18", false, null
            )
        ).toDF("token0", "token1", "price0", "price1", "fee0", "fee1", "volume0", "volume1", 
            "base0", "base1", "usdBase", "gasFees", "tip", "baseFeePerGas", "isbasePool", "is1")

        val result = df.
            baseAmounts.
            usdAmounts.
            // compare converted volume from USDT to ETH to USDC with original USDT amount
            select(ubi_resolve(ubid($"usdAmounts.volume", 'volume1)).cast("double").as("value"))
        val one = Seq((1: java.lang.Double)).toDF  // benchmark assuming  USDT = USDC

        /*
           is the combined divergence of eth and usd conversions < 0.1% ? 
           - assuming 0.001 the allowed market differential between USDT and USDC -
        */
        assertDataFrameApproximateEquals(result, one, 0.001)

    }

}

