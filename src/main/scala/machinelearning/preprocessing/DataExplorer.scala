package machinelearning.preprocessing


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions=>F}


object DataExplorer {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val trainDf=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/adult.data")

    val testDf=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/adult.test")

  //  trainDf.show(6)
    println("test")
    //testDf.show(5)

    val adultWholeData=trainDf.union(testDf)

    adultWholeData.show(6)

    println(adultWholeData.printSchema())

    adultWholeData.describe(  "age", "fnlwgt","education_num","capital_gain","capital_loss","hours_per_week").show()
    adultWholeData.groupBy($"workclass").agg(F.count($"*").as("sayi")).show()
    adultWholeData.groupBy($"education").agg(F.count($"*").as("sayi")).show()
    adultWholeData.groupBy($"marital_status").agg(F.count($"*").as("sayi")).show()
    adultWholeData.groupBy($"relationship").agg(F.count($"*").as("sayi")).show()
    adultWholeData.groupBy($"native_country").agg(F.count($"*").as("sayi")).show()


  }

}
