package fyi.dap.uniswap

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.GroupState
import StatefulStream.StatefulStreamExtensionDS

object ExchangeRate extends Spark with GroupStateMapping {
    import spark.implicits._

    def flatMap(ds: Dataset[ExchangeRateRow]) = ds.
        watermark.
        groupByKey(_.pairedToken0).
        flatMapGroupsWithState(outputMode, groupStateTimeout)(
            mapping(baseToken, "base0")).
        watermark.
        groupByKey(_.pairedToken1).
        flatMapGroupsWithState(outputMode, groupStateTimeout)(
            mapping(baseToken, "base1"))

    def usdFlatMap(ds: Dataset[ExchangeRateRow]) = {
        val DummyToken = Token("", 0L, "")
        ds.
            watermark.
            // apply current rate from ETH to USD: cannot parallelize time, hence constant key
            groupByKey(_ => DummyToken).
            flatMapGroupsWithState(outputMode, groupStateTimeout)(
                mapping(usdToken, "usdBase"))
    }

    def mapping(token: String, base: String):
        (Token, Iterator[ExchangeRateRow], GroupState[Base]) => Iterator[ExchangeRateRow] = { 

        (key: Token, values: Iterator[ExchangeRateRow], state: GroupState[Base]) => {

            def isBase(value: ExchangeRateRow) = value.isBasePool &&
                Seq(value.token0.address, value.token1.address).contains(token)
    
            if (state.hasTimedOut) {

                state.remove
                Log.info(s"removed timed out ${key.symbol} state in $base mapping")
                // function is called with no value when a group state times out
                values  // return expected type with empty iterator

            } else {

                val (sortedValues, size) = sort(values)
                val buffer = ArrayBuffer[ExchangeRateRow]()
    
                var baseNotFound = true
                var lastState = if (state.exists) { 
                    val lastState = state.get
                    if (sortedValues.buffered.head.logId - lastState.logId > BlockExpiry) {
                        state.remove
                        null
                    } else {
                        baseNotFound = false
                        lastState
                    }
                } else null
 
                while (sortedValues.hasNext && baseNotFound) {
                    val value = sortedValues.next
                    buffer += value
                    if (isBase(value)) {
                        baseNotFound = false
                        lastState = Base(value)
                    }
                }
    
                if (lastState != null) {
                    while (sortedValues.hasNext) {
                        val value = sortedValues.next
                        assert(value.logId > lastState.logId, 
                            s"${value.logId} found after ${lastState.logId} state in $base map")
                        val exchangeRate = isBase(value)
    
                        if (exchangeRate || value.logId - lastState.logId > BlockExpiry) {

                            buffer += value

                        } else {

                            buffer += (base match {
                                case "base0" => value.copy(base0 = lastState)
                                case "base1" => value.copy(base1 = lastState)
                                case "usdBase" => value.copy(usdBase = lastState)
                            })

                        }
    
                        if (exchangeRate) {
                            lastState = Base(value)
                        }
                    }
                    state.update(lastState)
                    state.setTimeoutTimestamp((buffer.last.blockNumber + BlockExpiry) * 1000)
                }
    
                assert(buffer.size == size, 
                    s"input ($size) and output (${buffer.size}) sizes not equal in $base map")
                buffer.toIterator
   
            }
 
        }

    }

}

