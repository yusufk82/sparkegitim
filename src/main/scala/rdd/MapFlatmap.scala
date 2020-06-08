package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MapFlatmap {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    val rddFromTextFile=sc.textFile("/home/oracle/opensource/dataset/nmcrl2016/dataset")
    val filteredListe=rddFromTextFile.filter(x=>x.contains("000000058"));
    filteredListe.foreach(println)
    val upperMap=rddFromTextFile.map(line=>line.toLowerCase()).foreach(println)
    val upperFlatMap=rddFromTextFile.flatMap(line=>line.split(",")).map(line=>line.toLowerCase()).foreach(println)
  }

}
