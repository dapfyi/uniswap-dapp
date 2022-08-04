import org.scalatest.FunSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fyi.dap.uniswap.Source.{deserialize_json, swapArgsJsonSchema}

class SourceSuite extends FunSuite with TestingBase {

    test("lossless json deserialization") {
        import spark.implicits._

        def jsonDeserializationTest(df: DataFrame) = {
            val (argsMap, _) = deserialize_json('value, swapArgsJsonSchema())
            val deserialized = df.
                withColumn("value", argsMap).
                withColumn("value", to_json('value))
            assertDataFrameEquals(deserialized, df)
        }

        // order keys in alphabetical order and remove all spaces to match json output
        val data = Seq((
            """{"amount0":"94027150752759122855288085421985",""" + 
            """"amount1":"-94027150752759122855288085421986",""" + 
            """"liquidity":"133781798900345601581411106757",""" +
            """"recipient":"0x0000000000007F150Bd6f54c40A34d7C3d5e9f56",""" + 
            """"sender":"0x0000000000007F150Bd6f54c40A34d7C3d5e9f56",""" +
            """"sqrtPriceX96":"1461446703485210103287273052203988822378723970341",""" + 
            """"tick":887271}"""))

        jsonDeserializationTest(data.toDF("value"))
    }

}

