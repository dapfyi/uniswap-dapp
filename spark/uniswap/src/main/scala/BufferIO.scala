package fyi.dap.uniswap

import java.util.{Properties, Collections}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import scala.util.Random

object BufferIO extends Spark {
    import spark.implicits._

    val BufferAddress = "uniswap-kafka.spark.svc.cluster.local:9092"

    var topic = "uniswap-buffer"

    def newTopic = {
        topic = Random.alphanumeric.take(20).mkString
    }

    def sink(df: DataFrame) = df.
        select('address, to_json(struct("*"))).toDF("key", "value").
        sort('logId).
        write.format("kafka").option("topic", topic).
        option("kafka.bootstrap.servers", BufferAddress).
        save

    def read: DataFrame = spark.readStream.
        format("kafka").
        option("subscribe", topic).
        option("kafka.bootstrap.servers", BufferAddress).
        option("startingOffsets", "earliest").
        load

    def read(schema: StructType): DataFrame = read.
        select('value.cast("string").as("json")).
        withColumn("deserialized", from_json('json, schema)).
        select(schema.fieldNames.map(name => col("deserialized." + name)): _*)

    def adminClient = {
        val props = new Properties
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BufferAddress)
        Admin.create(props)
    }

    def deleteTopic(topic: String = topic) = {
        val admin = adminClient
        try {
            admin.deleteTopics(Collections.singleton(topic))
        } finally {
            admin.close
        }
    }

}

