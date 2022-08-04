import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.MemoryStream
import fyi.dap.uniswap.{ExchangeRateRow, Token, Base}
import fyi.dap.uniswap.StatefulStream._

class StatefulSuite extends FunSuite with TestingBase {

    test("most recent and non-expired base pool quote gives its exchange rate " + 
        "to a subsequent swap with a matching paired token") { 

        import spark.implicits._

        val noneToken = Token("", 0L, "")
        val nullBase = Base(null, null, null, null)
        val initRow = nullRow.copy(token0 = noneToken, token1 = noneToken, 
            pairedToken0 = noneToken, pairedToken1 = noneToken,
            base0 = nullBase, base1 = nullBase, usdBase = nullBase)

        val OutputFields = Seq($"address", $"isBasePool", $"logId".cast("int").as("logId"),
            $"token0.symbol".as("token0"), $"token1.symbol".as("token1"), 
            $"pairedToken0.symbol".as("pairedToken0"), $"pairedToken1.symbol".as("pairedToken1"),
            $"base0", $"base1", $"usdBase")

        val stream = MemoryStream[ExchangeRateRow]

        val tokenX = Token("X", 0L, "x")
        val tokenY = Token("Y", 0L, "y")
        val base = Token("ETH", 18L, baseToken)
        val usdBase = Token("USDC", 6L, usdToken)
        // input rows
        val first = initRow.copy(
            logId = 0, address = "a", price1 = "1", is1 = false, isBasePool = true,
            token0 = base, token1 = tokenX, pairedToken0 = tokenX, pairedToken1 = tokenX)
        val second = initRow.copy(
            logId = 1, address = "d", price1 = "0.5", is1 = false, isBasePool = true,
            token0 = base, token1 = usdBase, pairedToken0 = usdBase, pairedToken1 = usdBase)
        val third = initRow.copy(
            logId = 2, address = "b", price1 = "2", is1 = null, isBasePool = false,
            token0 = tokenX, token1 = tokenY, pairedToken0 = tokenX, pairedToken1 = tokenY)
        val fourth = first.copy(logId = 3, price1 = "1.5")
        val fifth = initRow.copy(
            logId = 4, address = "c", price1 = "2", is1 = null, isBasePool = false,
            token0 = tokenY, token1 = tokenX, pairedToken0 = tokenY, pairedToken1 = tokenX)
        val sixth = third.copy(logId = 5, price1 = "2")
        val seventh = sixth.copy(logId = BlockExpiry + 6)

        stream.addData(first, second, third, fourth, fifth, sixth, seventh)

        // first, second and fourth are base pool quotes: base0 and 1 should be null
        val expected = Seq(
            first, 
            second,
            // exchange rate for token1 tokenY (base1) is unknown and should remain null
            third.copy(base0 = Base(first), usdBase = Base(second)),
            fourth.copy(usdBase = Base(second)),
            // token1 is now tokenX (known exchange rate quote for base)
            fifth.copy(base1 = Base(fourth), usdBase = Base(second)),
            // second swap in X/Y pool should also pick up the updated rate from X to base
            sixth.copy(base0 = Base(fourth), usdBase = Base(second)),
            // all base quotes are now expired and should remain null (see logId)
            seventh
        ).
            toDS.select(OutputFields: _*)

        val streamingQuery = stream.toDS.
            baseExchangeRate.
            usdExchangeRate.
            writeStream.queryName("exchangeRateTest").format("memory").
            start
 
        try {

            streamingQuery.processAllAvailable
            val result = spark.table("exchangeRateTest").
                select(OutputFields: _*).
                sort('logId)

            println("Result")
            result.show(false)
            assertDataFrameEquals(result, expected)

        } finally {
            streamingQuery.stop
        }

    }

    test("swap delta") {
        import spark.implicits._

        val OutputFields = Seq($"price0", $"price1", $"lastPrice0", $"lastPrice1", 
            $"priceDelta0", $"priceDelta1", $"priceDeltaPct0", $"priceDeltaPct1", 
            $"tick", $"tickDelta", $"address", $"logId".cast("int").as("logId"))

        val stream = MemoryStream[ExchangeRateRow]

        val firstRecordA = nullRow.copy(
            address = "a", logId = 0, price0 = "1", price1 = "1", tick = 0L)
        val firstRecordB = nullRow.copy(
            address = "b", logId = 1, price0 = "1", price1 = "1", tick = 5L)
        val secondRecordA = nullRow.copy(
            address = "a", logId = 2, price0 = "0.5", price1 = "2", tick = 1L)
        val secondRecordB = nullRow.copy(
            address = "b", logId = 3, price0 = "0.5", price1 = "2", tick = 2L)
        val thirdRecordA = nullRow.copy(
            address = "a", logId = 4, price0 = "1", price1 = "1", tick = 0L)

        stream.addData(
            firstRecordA,
            firstRecordB,
            secondRecordA,
            secondRecordB,
            thirdRecordA)
      
        val expected = Seq(
            firstRecordA,
            firstRecordB,
            secondRecordA.copy(
                lastPrice0 = "1", lastPrice1 = "1",
                priceDelta0 = "-5e1b0/1e0b0", priceDelta1 = "1e0b0/1e0b0", 
                priceDeltaPct0 = -50.0, priceDeltaPct1 = 100.0,
                tickDelta = 1L),
            secondRecordB.copy(
                lastPrice0 = "1", lastPrice1 = "1",
                priceDelta0 = "-5e1b0/1e0b0", priceDelta1 = "1e0b0/1e0b0", 
                priceDeltaPct0 = -50.0, priceDeltaPct1 = 100.0,
                tickDelta = -3L),
            thirdRecordA.copy(
                lastPrice0 = "0.5", lastPrice1 = "2",
                priceDelta0 = "5e1b0/1e0b0", priceDelta1 = "-1e0b0/1e0b0", 
                priceDeltaPct0 = 100.0, priceDeltaPct1 = -50.0,
                tickDelta = -1L)
        ).
            toDS.select(OutputFields: _*).
            sort('address, 'logId)

        val streamingQuery = stream.toDS.
            swapDelta.
            writeStream.queryName("deltaTest").format("memory").
            start
 
        try {

            streamingQuery.processAllAvailable
            val result = spark.table("deltaTest").
                select(OutputFields: _*).
                sort('address, 'logId)
    
            println("Result")
            result.show(false)
            assertDataFrameEquals(result, expected)

        } finally {
            streamingQuery.stop
        }

    }

    val nullRow = ExchangeRateRow(
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null, null, null, null,
        null, null, null
    )

}

