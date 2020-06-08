package machinelearning.preprocessing


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.ml.feature.{StringIndexer}
object PreProcessOps {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val df=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/SimpleData.csv")

    df.show(5)
    val df1=df.withColumn("ekonomikdurum",
      when(col("aylik_gelir").gt(7000),"iyi")
      .otherwise("kötü"))

    df1.show()

    df.groupBy("meslek").agg(count("*").as("sayi")).sort(desc("sayi"),asc("meslek")).show()

   
    val meslekIndexer=new StringIndexer()
      .setInputCol("meslek")
      .setOutputCol("meslekIndex")
      .setHandleInvalid("skip")



   val meslekIndexerModel= meslekIndexer.fit(df1)
    val meslekIndexedDf=meslekIndexerModel.transform(df1)


    val sehirIndexer=new StringIndexer()
        .setInputCol("sehir")
        .setOutputCol("sehirIndex")
        .setHandleInvalid("skip")

    val sehirIndexerModel=sehirIndexer.fit(meslekIndexedDf)
    val sehirIndexerDF=sehirIndexerModel.transform(meslekIndexedDf)

    sehirIndexerDF.show()

  }

}
