package fyi.dap.uniswap

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{lit, struct, to_date}
import org.apache.spark.sql.types.{StructType, StructField}
import io.delta.tables._

object StatefulStream extends Spark {
    import spark.implicits._

    val unnamedBase = struct(
        lit(null).as("poolAddress"),
        lit(null).as("price"),
        lit(null).as("is1"),
        lit(null).as("logId"))

    implicit class StatefulStreamExtension(df: DataFrame) {

        def toExchangeRateRow = df.
            select($"*",
                lit(null).as("blockNumberTs"),
                lit(null).as("lastPrice0"),
                lit(null).as("lastPrice1"),
                lit(null).as("priceDelta0"),
                lit(null).as("priceDelta1"),
                lit(null).as("priceDeltaPct0"),
                lit(null).as("priceDeltaPct1"),
                lit(null).as("tickDelta"),
                unnamedBase.as("base0"),
                unnamedBase.as("base1"),
                unnamedBase.as("usdBase")).
            as[ExchangeRateRow].
            map(r => r.copy(blockNumberTs = 
                new Timestamp(r.blockNumber * 1000)))

        def ratedSwapUpsert = df.
            withColumn("date", to_date('timestamp.cast("timestamp"))).
            writeStream.
            foreachBatch(upsert _).
            start

        private def upsert(microBatch: DataFrame, batchId: Long) = {

            val rswaps = DeltaTable.createIfNotExists(spark).
                location(s"s3://$DeltaBucket/uniswap/rswaps").
                partitionedBy("date").
                // table schema must be defined to specify partitioning
                addColumns(microBatch.schema).
                execute

            rswaps.as("s").
                merge(microBatch.as("b"), 
                    "s.date = b.date AND s.logId = b.logId").
                whenMatched.updateAll.
                whenNotMatched.insertAll.
                execute

        }

    }

    implicit class StatefulStreamExtensionDS(
        val ds: Dataset[ExchangeRateRow]) extends AnyVal {

        def watermark = ds.withWatermark(
            "blockNumberTs", s"$BlockExpiry seconds")

        def swapDelta = SwapDelta.flatMap(ds)
        def baseExchangeRate = ExchangeRate.flatMap(ds)
        def usdExchangeRate = ExchangeRate.usdFlatMap(ds)
    }

}

