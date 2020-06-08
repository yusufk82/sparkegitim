package rdd


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountRdd {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val nmcrlRdd=sc.textFile("/home/oracle/opensource/dataset/nmcrl2016/dataset")
    val nmcrlRddFlatMap=nmcrlRdd.flatMap(line=>line.split(","))
  //  println(nmcrlRddFlatMap.foreach(line=>println(line)))
    val nmcrlRddMap=nmcrlRddFlatMap.map(kelime=>(kelime,1)).reduceByKey((a,b)=>a+b)
   // println(nmcrlRddMap.foreach(line=>println(line)))

    val kelimeSayilari=nmcrlRddMap.map(line=>(line._2,line._1))
  //  kelimeSayilari.foreach(println)
    kelimeSayilari.sortByKey(false).foreach(println)
  }

}
