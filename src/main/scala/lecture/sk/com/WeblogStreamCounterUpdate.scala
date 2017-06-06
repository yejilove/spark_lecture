package lecture.sk.com

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object WeblogStreamCounterUpdate {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("StreamingTest")
      .setMaster("local[2]")
      .set("spark.ui.port", "4040")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "sandbox:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "sk",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("weblog")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    val weblog = stream.map(record => record.value)

    val itemName = sc.textFile("item.txt")
      .map(line => line.split(','))
      .map(fields => (fields(0), fields(1)))

    val purchaseCnt = weblog.transform(rdd => {
      rdd.filter(line => line.contains("action=purchase"))
        .map(line => line.split("&")(1))
        .map(line => (line.split("=")(1), 1))
        .reduceByKey((v1, v2) => v1 + v2)
    })

    val purchaseTotal = purchaseCnt.updateStateByKey(updateCount)

    purchaseTotal.foreachRDD(rdd => {
      val result = itemName.join(rdd).values
      result.saveAsTextFile("/tmp/result")
    })

    ssc.checkpoint("/tmp/checkpoint")

    ssc.start()

    ssc.awaitTermination()
  }

  def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
    val newCount = newCounts.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(newCount + previousCount)
  }
}