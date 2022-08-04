package fyi.dap.uniswap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import Compute._
import Convert._
import StatefulStream._

object Swaps extends App with Spark {
    import spark.implicits._

    val pools = Source.pools
    
    // sourced transactions and swaps include last BlockExpiry blocks from previous epoch
    val transactions = Source.transactions(epoch, BlockExpiry)

    // high-throughput batch process on joined pool events and block transactions 
    val swaps = Source.swaps(epoch, BlockExpiry).
        join(transactions, Seq("blockNumber", "transactionHash"), "left").
        join(pools, Seq("address"), "left").
        logId.
        swapPrice.
        swapVolume.
        tickLiquidity.
        exchangeRatePrep

    // order swaps in kafka buffer to sync dependent time-series of token prices
    BufferIO.newTopic
    BufferIO.sink(swaps)
    val streamingSwaps = BufferIO.read(swaps.schema).
        toExchangeRateRow.
        swapDelta

    // apply live exchange rates in streaming process
    val ratedSwaps = streamingSwaps.
        baseExchangeRate.
        usdExchangeRate.
        repartition('address).
        // filter out previous epoch to proceed efficiently with time-independent calculations
        where('epoch===epoch).
        toDF.
        baseAmounts.
        totalFees.
        usdAmounts.
        ratedSwapUpsert

    if(!isLiveStreaming) {
        ratedSwaps.processAllAvailable
        ratedSwaps.stop
        BufferIO.deleteTopic _
    }

}

