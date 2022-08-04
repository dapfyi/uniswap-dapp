// `:load dap/spark/uniswap/src/integration/buffer.scala` to run in shell

import fyi.dap.uniswap.BufferIO
import SparkTestingBase._

def bufferTest = {
    println("")
    println("Test ~ BufferIO")
    BufferIO.newTopic
    try {
        // cast to java Integer to make schema nullable and match output
        val data: Seq[(String, Integer)] = Seq(
            ("a", 3), ("a", 2), ("a", 1), 
            ("b", 0), ("b", 4), ("b", 9), 
            ("c", 7), ("c", 8), ("c", 6), ("c", 5))
        val df = data.toDF("address", "logId")
        BufferIO.sink(df)

        BufferIO.read.writeStream.queryName("bufferTest").format("memory").
            start.processAllAvailable

        val result = spark.sql("select CAST(value AS string) AS json from bufferTest").
            withColumn("json", from_json('json, df.schema)).
            withColumn("address", 'json("address")).
            withColumn("logId", 'json("logId")).drop("json")

        assertDataFrameEquals(result.sort('address, 'logId), df.sort('address, 'logId))
        println("Passed ~ BufferIO")
    } finally {
        spark.streams.active(0).stop
    }
}

def deleteTopicTest = {
    println("")
    println("Test ~ deleteTopic")
    val admin = BufferIO.adminClient
    val firstTopic = admin.listTopics.names.get.iterator.next
    BufferIO.deleteTopic(firstTopic)
    assert(!admin.listTopics.names.get.contains(firstTopic))
    println("Passed ~ deleteTopic")
}

bufferTest
deleteTopicTest

