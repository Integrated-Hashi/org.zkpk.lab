import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
class SparkSqlDemo02 {}
object SparkSqlDemo02 {
  case class Person(id:String, name:String, age:Int)
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("SparkSqlDemo1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    val lines = sc.textFile(args(0))
    val personsRDD: RDD[Person] =
      lines.map(line => line.split(" ")).map(arr => Person(arr(0), arr(1), arr(2).toInt))
    import sqlContext.implicits._
    val df = personsRDD.toDF()
    df.createOrReplaceTempView("Person")
    val nameAgeDF = df.select("name","age")
    nameAgeDF.show()
    val liliDataSet: Dataset[Row] =
      df.select("name","age").filter(row => row.getAs[String]("name") == "lili")
      liliDataSet.show()
    val groupedDF = df.select("age").groupBy("age")
    val groupedDF1 = df.select("age").groupBy(df("age"))
    groupedDF.count().show()
    groupedDF1.count().show()
    val agePlusOne: DataFrame = df.select(df("name"),df("age")+1)
    agePlusOne.show()
    val alias = df.select(df("name").as("n"),df("age").as("a"))
    alias.write.format("json").save(args(1))


    val teenagers = sqlContext.sql("SELECT name FROM person WHERE age >= 13 AND age <= 22")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    teenagers.write.format("parquet").save(args(1))
  }
}

