import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import fyi.dap.uniswap.Aggregate.liquidityWeightedPrice
import fyi.dap.sparkubi.functions._

class AggregateSuite extends FunSuite with TestingBase {

    test("liquidity weighted price") {
        import spark.implicits._
        val input = Seq($"tickDepth", $"price")
        val output = Seq((1, "2", "1"), (4, "1", "1")).
            toDF("tickDepth", "price", "liquidity").
            agg(ubi_resolve(liquidityWeightedPrice(input))).
            first.getString(0)
        assert(output == "1.2")  // (1*2 + 4*1) / (1 + 4)
    }

}

