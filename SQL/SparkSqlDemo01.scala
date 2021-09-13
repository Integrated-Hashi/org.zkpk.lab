import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
class SparkSqlDemo01 {}
object SparkSqlDemo01 {
  case class Person(id:String, name:String, age:Int)
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlDemo1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val lines = sc.textFile(args(0))
    val personsRDD: RDD[Person] =
      lines.map(line => line.split(" ")).map(arr => Person(arr(0), arr(1), arr(2).toInt))
    import sqlContext.implicits._
    val df = personsRDD.toDF()
    df.createOrReplaceTempView("Person")

    val teenagers = sqlContext.sql("SELECT name FROM person WHERE age >= 13 AND age <= 22")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    teenagers.write.format("parquet").save(args(1))
  }
}
