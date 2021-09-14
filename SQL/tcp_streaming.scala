import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
object tcp_streaming {
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("tcp_streaming")

    val ssc = new StreamingContext(conf, Seconds(5))

    val textStream = ssc.socketTextStream("master",9999)
    val words = textStream.flatMap(_.split(" "))
    val wordPairs = words.map((_,1)).reduceByKey(_+_)
    wordPairs.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
