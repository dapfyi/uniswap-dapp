package fyi.dap.uniswap

import java.lang.NullPointerException

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.GroupState
import StatefulStream.StatefulStreamExtensionDS
import fyi.dap.sparkubi.Arithmetic.{subtract, divide}
import fyi.dap.sparkubi.Unary.resolve
import fyi.dap.sparkubi.Typecast._

object SwapDelta extends Spark with GroupStateMapping {
    import spark.implicits._

    def flatMap(ds: Dataset[ExchangeRateRow]) = ds.
        watermark.
        groupByKey(_.address).
        flatMapGroupsWithState(outputMode, groupStateTimeout)(mapping _)

    case class SwapDeltaState(
        logId: BigDecimal, 
        price0: String, 
        price1: String, 
        tick: Long
    )

    def mapping(key: String, values: Iterator[ExchangeRateRow], 
        state: GroupState[SwapDeltaState]): Iterator[ExchangeRateRow] = {

        if (state.hasTimedOut) {

            state.remove
            Log.info(s"removed timed out $key state in SwapDelta mapping")
            values

        }  else {

            val (sortedValues, size) = sort(values)
            val buffer = ArrayBuffer[ExchangeRateRow]()
    
            var lastState = if (state.exists) state.get else {
                val value = sortedValues.next
                buffer += value
                SwapDeltaState(value.logId, value.price0, value.price1, value.tick)
            }
    
            while (sortedValues.hasNext) {
                val value = sortedValues.next
                assert(value.logId > lastState.logId, 
                    s"${value.logId} $key event found after ${lastState.logId} state in delta map")
    
                if (value.logId - lastState.logId > BlockExpiry) {
                    buffer += value
                } else {
                    try {
                        val priceDelta0 = subtract(value.price0, lastState.price0)
                        val priceDelta1 = subtract(value.price1, lastState.price1)
                        buffer += value.copy(
                            lastPrice0 = lastState.price0,
                            lastPrice1 = lastState.price1,
                            priceDelta0 = priceDelta0,
                            priceDelta1 = priceDelta1,
                            priceDeltaPct0 = resolve(
                                divide(priceDelta0, lastState.price0)).toDouble * 100,
                            priceDeltaPct1 = resolve(
                                divide(priceDelta1, lastState.price1)).toDouble * 100,
                            tickDelta = value.tick - lastState.tick
                        )
                    } catch {
                        case e: NullPointerException =>
                            Log.error(s"priceDelta subtraction NPE in ${value.pool} pool: last "
                                + s"$lastState, price0 ${value.price0}, price1 ${value.price1}")
                            Log.error(s"offending priceDelta NPE record $value")
                            throw e
                    }
                }
    
                lastState = SwapDeltaState(value.logId, value.price0, value.price1, value.tick)
            }
    
            state.update(lastState)
            state.setTimeoutTimestamp((buffer.last.blockNumber + BlockExpiry) * 1000)
            assert(buffer.size == size, 
                s"input ($size) and output (${buffer.size}) sizes not equal in delta map")
            buffer.toIterator

        }

    }

}

