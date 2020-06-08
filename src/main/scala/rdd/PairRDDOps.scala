package rdd


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PairRDDOps {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    val rddFile=sc.textFile("/home/oracle/opensource/dataset/SimpleData.csv").filter(!_.contains("sirano"))
    //rddFile.foreach(println)

    def meslekMaasPair(line:String) ={

      val meslek=line.split(",")(3)
      val maas=line.split(",")(5).toDouble
      (meslek,maas)
    }

   val meslekMaasPairRdd=rddFile.map(meslekMaasPair)
    meslekMaasPairRdd.foreach(println)


    val meslegeGoreMaas=meslekMaasPairRdd.mapValues(x=>(x,1))
    meslegeGoreMaas.foreach(println)

    println("-----------------reduce by key-------------")
    val meslekMaasReduceByKey=meslegeGoreMaas.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    meslekMaasReduceByKey.foreach(println)
println("---------------ortalama maaş ----------------")
    val meslekOrtalamaMaas=meslekMaasReduceByKey.mapValues(x=>x._1/x._2).foreach(println)
  }

}
