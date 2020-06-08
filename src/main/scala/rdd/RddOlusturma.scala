package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object RddOlusturma {

  def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)
/*
      val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
        .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()

    val sc=spark.sparkContext

 */
    val conf=new SparkConf().setMaster("local[4]").setAppName("RddOlusturma")
    val sc=new SparkContext(conf)
    val sparkRddList=sc.makeRDD(List(1,2,4,5,7,8,9))

    sparkRddList.take(4).foreach(println)

    val sparkRddListTuple=sc.makeRDD(List((1,2),(4,5,7),(8,9)))
    sparkRddListTuple.foreach(println)

    val sparkRange=sc.range(1000,100000,1000)
    println(sparkRange.foreach(println))

    val rddFromTextFile=sc.textFile("/home/oracle/opensource/dataset/nmcrl2016/dataset")
    println(rddFromTextFile.count())

  }

}
