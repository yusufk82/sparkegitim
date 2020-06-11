package machinelearning.preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

object MultipleLineerRegresyon {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val df=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/Advertising.csv")
    df.show(false)

    val yeniSutunlar=Array("id","tv","radio","newspaper","label")
    val df1=df.toDF(yeniSutunlar:_*)
    df1.show(false)

    var numerikDegiskenler=Array("tv","radio")
    /// newspaper p değerlerine göre anlanmlı olmadığı için kaldırıldı
    var label=Array("label")
    println(df1.describe().show())

    val vectorAssembler=new VectorAssembler()
      .setInputCols(numerikDegiskenler)
      .setOutputCol("features")

    val lr=new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")


    val pipeline=new Pipeline()
      .setStages(Array(vectorAssembler,lr))

    val Array(trainDf,testDf)=df1.randomSplit(Array(0.8,0.2),137L)
    val pipelineModel=pipeline.fit(trainDf)
    pipelineModel.transform(testDf).show()

    val lrModel=pipelineModel.stages(1).asInstanceOf[LinearRegressionModel ]
    println("Sabit:"+lrModel.intercept)
    println("Katsayılar:"+lrModel.coefficients)
    println("RootMeanSquareError:"+lrModel.summary.rootMeanSquaredError)
    println("R2:"+lrModel.summary.r2)
    println("R2adj:"+lrModel.summary.r2adj)
    println("p degerleri:"+lrModel.summary.pValues.mkString((",")))
    println("t değerleri:"+lrModel.summary.tValues.mkString((",")))
///// model y=2.842686851312404+ 0.0457449146985224*tv+0.18985040697353542*radio

    /// tv 50 radio 10

    val predictDf=Seq((50.0,10.0)).toDF("tv","radio")
    predictDf.show()
    val predictDFVect=vectorAssembler.transform(predictDf)
    lrModel.transform(predictDFVect).show(false)
  }

}
