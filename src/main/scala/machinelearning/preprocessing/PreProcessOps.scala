package machinelearning.preprocessing


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler,StandardScaler}

import scala.math.pow
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

   ///string indexer kategorik özellikleri sayısal hale getiriliyor////
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

    /// kategorikleri one hot ile sayısal hale getirme///
    val encoder=new OneHotEncoderEstimator()
      .setInputCols(Array[String]("meslekIndex","sehirIndex"))
      .setOutputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded"))
    val encoderModel=encoder.fit(sehirIndexerDF)
    val onehotEncodeDf=encoderModel.transform(sehirIndexerDF)
    onehotEncodeDf.show(false)

    //// vektör haline getirme ///

    val vectorAssembler=new VectorAssembler()
      .setInputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded","yas","aylik_gelir"))
      .setOutputCol("vectoriesFeatures")

    val vectorAssembledDF=vectorAssembler.transform(onehotEncodeDf)
    vectorAssembledDF.show(false)

    /// label ındexer ///

    val labelIndexer=new StringIndexer()
      .setInputCol("ekonomikdurum")
      .setOutputCol("label")

    val labelIndexerModel=labelIndexer.fit(vectorAssembledDF)
    val labelIndexerDF=labelIndexerModel.transform(vectorAssembledDF)
    labelIndexerDF.show(false)


    /// standart scale//
    import scala.math._
    val yasEtkisi=sqrt(pow((35-33),2))
    val maasEtkisi=sqrt(pow((18000-3500),2))
    val oklidMesafesi=sqrt(pow((35-33),2)+pow((18000-3500),2))
    println(s"Toplam etkisi: $oklidMesafesi , yaş etkisi: $yasEtkisi, maaş etkisi: $maasEtkisi")

    val scaler=new StandardScaler()
      .setInputCol("vectoriesFeatures")
      .setOutputCol("features")
    val scalerModel=scaler.fit(labelIndexerDF)
    val scalerDF=scalerModel.transform(labelIndexerDF)

    scalerDF.show(false)

    /// train test ayirma ///
   val Array(trainDF,testDF)= scalerDF.randomSplit(Array(0.8,0.2),142L)
    trainDF.show(false)
    testDF.show(false)

    /// basit bir ml modeli//

    import org.apache.spark.ml.classification.LogisticRegression
    val logisticRegression=new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val lrModel=logisticRegression.fit(trainDF)  // egitildi.
    lrModel.transform(testDF).show()  // egitilen model test edildi.

  }

}
