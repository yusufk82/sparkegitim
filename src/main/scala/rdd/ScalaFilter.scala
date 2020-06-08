package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ScalaFilter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    val rddFile=sc.textFile("/home/oracle/opensource/dataset/OnlineRetail.csv")
   // rddFile.take(10).foreach(println)
   // println(rddFile.count())
    val retailRdd=rddFile.mapPartitionsWithIndex(
      (idx,iter)=>if(idx==0)iter.drop(1) else iter
    )

   // retailRdd.take(19).foreach(println)

    println("\n \n")
    println("birim miktarı 30 dan büyük olanlar")
    retailRdd.filter(x=>x.split(";")(3).toInt>30).take(10).foreach(println)

    println("ürün tanımında kahve geçenler ve birim fiyatı 20 den büyük olanlar")
    retailRdd.filter(x=> x.split(";")(2).contains("COFFEE") &&
      x.split(";")(5).trim().replace(",",".").toFloat>20.0f).take(20).foreach(println)

 println("yukarıdaki örneği fonksiyonla yapmak")

    retailRdd.filter(x=>coffePrice(x)).take(10).foreach(println)
  }

  def coffePrice(line:String):Boolean={
    var sonuc=true

    var description:String=line.split(";")(2)
    var birimFiyat:Float=line.split(";")(5).trim().replace(",",".").toFloat
     sonuc=description.contains("COFFEE") && birimFiyat>20.0f
sonuc
  }

}
