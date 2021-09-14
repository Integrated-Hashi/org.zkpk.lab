import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
object hdfs_streaming {
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("hdfs_streaming")

    val ssc = new StreamingContext(conf, Seconds(5))
    val filePath = args(0)
    val fileStream = ssc.textFileStream(filePath)
    val dStream = fileStream.flatMap(_.split(" ")).map((_,1))
    val wc = dStream.reduceByKey(_+_)
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
