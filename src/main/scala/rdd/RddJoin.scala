package rdd


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object RddJoin {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    val orderItems=sc.textFile("/home/oracle/opensource/dataset/order_items.csv").filter(!_.contains("orderItemName"))
    val productsItems=sc.textFile("/home/oracle/opensource/dataset/products.csv").filter(!_.contains("productId"))

    println("order lar")
    orderItems.take(5).foreach(println)

    println("products")
    productsItems.take(5).foreach(println)

    def orderItemsConvertPairRdd(line:String)={
      val orderItemName=line.split(",")(0)
      val orderItemName1=line.split(",")(1)
      val orderItemName2=line.split(",")(2)
      val orderItemName3=line.split(",")(3)
      val orderItemName4=line.split(",")(4)
      val orderItemName5=line.split(",")(5)
      (orderItemName2,(orderItemName,orderItemName1,orderItemName2,orderItemName3,orderItemName4,orderItemName5))
    }

    val orderItemsPairRdd=orderItems.map(orderItemsConvertPairRdd)
    orderItemsPairRdd.take(5).foreach(println)

    def productItemsConvertPairRdd(line:String)={
      val orderItemName=line.split(",")(0)
      val orderItemName1=line.split(",")(1)
      val orderItemName2=line.split(",")(2)
      val orderItemName3=line.split(",")(3)
      val orderItemName4=line.split(",")(4)
      val orderItemName5=line.split(",")(5)
      (orderItemName,(orderItemName,orderItemName1,orderItemName2,orderItemName3,orderItemName4,orderItemName5))
    }

    val productItemsPairRdd=productsItems.map(productItemsConvertPairRdd)
    productItemsPairRdd.take(5).foreach(println)

    val orderProductJoinRdd=orderItemsPairRdd.join(productItemsPairRdd)
    orderProductJoinRdd.take(5).foreach(println)
  }

}
