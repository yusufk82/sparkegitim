package rdd


import com.sun.xml.internal.bind.v2.model.core.MaybeElement
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source
object BroadcastOpsVariable {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext

    // satış hacmi en yüksek ürünü bulma

    def loadProducts():Map[Int,String]={
      val source=Source.fromFile("/home/oracle/opensource/dataset/products.csv")
      val lines=source.getLines().filter(x=>(!(x.contains("productCategoryId"))))
      var productIdName:Map[Int,String]=Map()
      for(line <- lines){
          val productId=line.split(",")(0).toInt
        val productName=line.split(",")(2)
        productIdName+=(productId->productName)
        return  productIdName
      }
      return productIdName
    }

    val broadcastProducts=sc.broadcast(loadProducts)

    val orderItemRdd=sc.textFile( "/home/oracle/opensource/dataset/order_items.csv").filter(!_.contains("orderItemName"))
    def makeOrderItemsPairRdd(line:String):(Int,Float)={
      val orderItemName=line.split(",")(0)
      val orderItemName1=line.split(",")(1)
      val orderItemName2=line.split(",")(2).toInt
      val orderItemName3=line.split(",")(3)
      val orderItemName4=line.split(",")(4).toFloat
      val orderItemName5=line.split(",")(5)
      (orderItemName2,orderItemName4)
    }

    val orderItemsPairRdd=orderItemRdd.map(makeOrderItemsPairRdd)
    orderItemsPairRdd.take(5).foreach(println)
    println("reduce by key aşaması")
   val siraliListe= orderItemsPairRdd.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false)
      .map(x=>(x._2,x._1))
        siraliListe.take(5).foreach(println)
    val sortedOrderWithProducts=siraliListe.map(x=>(broadcastProducts.value(x._1),x._2))
    println(broadcastProducts.value(1))
    sortedOrderWithProducts.take(5).foreach(println)
  }
}
