package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object RddExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext

    val list1=sc.makeRDD(List(3,7,13,15,22,36,7,11,3,25))
    val list2=sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

    val kesisimList=list1.intersection(list2)
    kesisimList.foreach(println)
    val tekilList1=list1.distinct().foreach(println)
    list1.countByValue().foreach(println)
  }

}
