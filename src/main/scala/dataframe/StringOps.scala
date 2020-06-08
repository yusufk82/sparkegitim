package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object StringOps {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val df=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/dirtydata.csv")
    //df.show()
    val df2=df.select("meslek","sehir").withColumn("mesleksehir",concat(col("meslek"),lit("-"),col("sehir")))


  }

}
