package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object MapTransformation {

  def main(args: Array[String]): Unit = {
    case class CancelledPrice(isCancelled:Boolean,total:Double)

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    val rddFile=sc.textFile("/home/oracle/opensource/dataset/OnlineRetail.csv").filter(!_.contains("InvoiceNo"))
    rddFile.take(4).foreach(println)
    println(rddFile.first())
   val retailTotal= rddFile.map(x=>{
     var isCanceled:Boolean=if(x.split(";")(0).startsWith("C")) true else false
      var total:Double=x.split(";")(3).toDouble*x.split(";")(5).trim().replace(",",".").toDouble

   CancelledPrice(isCanceled,total)
    })
    retailTotal.take(6).foreach(println)

    retailTotal.map(x=>(x.isCancelled,x.total)).reduceByKey((x,y)=>x+y)
      .filter(x=>x._1==true).map(x=>x._2).take(5).foreach(println)

  }

}
