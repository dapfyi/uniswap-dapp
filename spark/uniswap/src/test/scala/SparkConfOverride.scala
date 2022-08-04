import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import fyi.dap.uniswap.Compute.settings
import org.scalatest.FunSuite

trait SparkConfOverride extends FunSuite with DataFrameSuiteBase {

    val overridenSettings = settings.map(t => t._1 match { 
        case "spark.sql.ansi.enabled"                         => (t._1, "true")
        case "spark.sql.storeAssignmentPolicy"                => (t._1, "strict") 
        case _ => t
    })

    override def conf: SparkConf = {
        new SparkConf().
            setMaster("local[2]").
            setAppName("test").
            set("spark.ui.enabled", "false").
            set("spark.app.id", appID).
            set("spark.driver.host", "localhost").
            setAll(overridenSettings)
    }

    test("conf") {
        val properties = Seq("storeAssignmentPolicy", "enabled")
        spark.conf.getAll.filter(properties contains _._1.split("\\.").last).foreach {
            case (key, value) => println(key + " -> " + value)
        }
    }

}

