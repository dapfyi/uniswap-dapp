/* code from https://github.com/holdenk/spark-testing-base/.../DataFrameSuiteBase.scala 
  - with scalatest dependency removed -
  script is meant to be loaded in the spark-shell of a production-like environment */

// :load dap/spark/uniswap/src/integration/SparkTestingBase.scala

import java.sql.Timestamp

import scala.reflect.ClassTag
import scala.math.abs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object SparkTestingBase extends Serializable {

    val maxUnequalRowsToShow = 10
    
    def assertEmpty[U](arr: Array[U])(implicit CT: ClassTag[U]): Unit =
        assert(arr.isEmpty)
    
    def approxEquals(r1: Row, r2: Row, tol: Double): Boolean = {
        if (r1.length != r2.length) {
            return false
        } else {
            (0 until r1.length).foreach(idx => {
                if (r1.isNullAt(idx) != r2.isNullAt(idx)) {
                    return false
                }
    
                if (!r1.isNullAt(idx)) {
                    val o1 = r1.get(idx)
                    val o2 = r2.get(idx)
                    o1 match {
                        case b1: Array[Byte] =>
                            if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
                                return false
                            }
    
                        case f1: Float =>
                            if (java.lang.Float.isNaN(f1) !=
                                java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
                                return false
                            }
                            if (abs(f1 - o2.asInstanceOf[Float]) > tol) {
                                return false
                            }
    
                        case d1: Double =>
                            if (java.lang.Double.isNaN(d1) !=
                                java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
                                return false
                            }
                            if (abs(d1 - o2.asInstanceOf[Double]) > tol) {
                                return false
                            }
    
                        case d1: java.math.BigDecimal =>
                            if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
                                return false
                            }
    
                        case t1: Timestamp =>
                            if (abs(t1.getTime - o2.asInstanceOf[Timestamp].getTime) > tol) {
                                return false
                            }
    
                        case _ =>
                            if (o1 != o2) return false
                    }
                }
            })
        }
        true
    }
    
    
    def zipWithIndex[U](rdd: RDD[U]) = {
        rdd.zipWithIndex.map{ case (row, idx) => (idx, row) }
    }
    
    /**
      * Compares if two [[DataFrame]]s are equal, checks that the schemas are the same.
      * When comparing inexact fields uses tol.
      *
      * @param tol max acceptable tolerance, should be less than 1.
      */
    def assertDataFrameApproximateEquals(
        expected: DataFrame, result: DataFrame, tol: Double): Unit = {
    
        assert(expected.schema == result.schema, "Schema not Equal")
    
        try {
            expected.rdd.cache
            result.rdd.cache
            assert(expected.rdd.count == result.rdd.count, "Length not Equal")
    
            val expectedIndexValue = zipWithIndex(expected.rdd)
            val resultIndexValue = zipWithIndex(result.rdd)
    
            val unequalRDD = expectedIndexValue.join(resultIndexValue).
                filter{case (idx, (r1, r2)) =>
                !(r1.equals(r2) || approxEquals(r1, r2, tol))}
    
            assertEmpty(unequalRDD.take(maxUnequalRowsToShow))
        } finally {
            expected.rdd.unpersist()
            result.rdd.unpersist()
        }
    }
    
    /**
     * Compares if two [[DataFrame]]s are equal, checks the schema and then if that
     * matches checks if the rows are equal.
     */
    def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
        assertDataFrameApproximateEquals(expected, result, 0.0)
    }

}

