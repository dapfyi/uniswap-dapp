import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import fyi.dap.uniswap.Compute.settings
import org.scalatest.Suite

trait TestingBase extends DataFrameSuiteBase { self: Suite =>

    override def conf: SparkConf = {
        new SparkConf().
            setMaster("local[2]").
            setAppName("test").
            set("spark.ui.enabled", "false").
            set("spark.app.id", appID).
            set("spark.driver.host", "localhost").
            set("spark.sql.extensions", "fyi.dap.sparkubi.Extensions").
            setAll(settings)
    }

}

