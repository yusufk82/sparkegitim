package machinelearning.preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler,StandardScaler}
import org.apache.spark.ml.Pipeline

import scala.math.pow

object PipelineOps {
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

    val sehirIndexer=new StringIndexer()
      .setInputCol("sehir")
      .setOutputCol("sehirIndex")
      .setHandleInvalid("skip")

    val encoder=new OneHotEncoderEstimator()
      .setInputCols(Array[String]("meslekIndex","sehirIndex"))
      .setOutputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded"))

    val vectorAssembler=new VectorAssembler()
      .setInputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded","yas","aylik_gelir"))
      .setOutputCol("vectoriesFeatures")

    val labelIndexer=new StringIndexer()
      .setInputCol("ekonomikdurum")
      .setOutputCol("label")

    val scaler=new StandardScaler()
      .setInputCol("vectoriesFeatures")
      .setOutputCol("features")


    val Array(trainDF,testDF)= df1.randomSplit(Array(0.8,0.2),142L)


    import org.apache.spark.ml.classification.LogisticRegression
    val logisticRegression=new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val pipeLineObj=new Pipeline()
      .setStages(Array(meslekIndexer,sehirIndexer,encoder,vectorAssembler,labelIndexer,scaler,logisticRegression))

    val pipelineModel=pipeLineObj.fit(trainDF)
    pipelineModel.transform(testDF).show(false)

  }
}
