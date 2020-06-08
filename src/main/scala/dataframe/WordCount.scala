package dataframe


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val hikayeDs=spark.read.textFile("/home/oracle/opensource/dataset/omerseyfettin.txt")
  //  val hikayeDf=  hikayeDs.toDF("value")
  //  hikayeDf.show(10,truncate = false)
    val kelimeler=hikayeDs.flatMap( x=> x.split(" "))
    println(kelimeler.count())
   // kelimeler.show(5)
    import org.apache.spark.sql.functions.count
    kelimeler.groupBy("value").agg(count("value")
      .as("kelimeSayisi")).orderBy($"kelimeSayisi".desc).show(10)
  }
}
