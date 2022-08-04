package fyi.dap.uniswap

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}

trait GroupStateMapping {

    val outputMode = OutputMode.Append
    val groupStateTimeout = GroupStateTimeout.EventTimeTimeout

    def sort(values: Iterator[ExchangeRateRow]) = { 
        val seq = values.toSeq
        (seq.sortWith(_.logId < _.logId).toIterator, seq.size)
    }

}

