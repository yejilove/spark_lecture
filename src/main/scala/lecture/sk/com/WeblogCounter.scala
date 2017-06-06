package lecture.sk.com

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WeblogCounter {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("StreamingTest")

    val sc = new SparkContext(conf)

    val itemName = sc.textFile("item.txt")
      .map(line => line.split(','))
      .map(fields => (fields(0), fields(1)))

    val purchaseCnt = sc.textFile("web.log")
      .filter(line => line.contains("action=purchase"))
      .map(line => line.split("&")(1))
      .map(line => (line.split("=")(1), 1))
      .reduceByKey((v1, v2) => v1 + v2)

    val result = itemName.join(purchaseCnt).values
    result.saveAsTextFile("/tmp/result")
  }
}