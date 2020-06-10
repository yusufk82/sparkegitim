package machinelearning.preprocessing


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

object BasitLineerRegresyon {

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

    val df2=df.withColumn("Advertisement",(col("TV")+col("Newspaper")+col("Radio")))
      .withColumnRenamed("Sales","label")
      .drop("TV","Radio","Newspaper")

    df2.show(false)

    df2.describe("Advertisement","label").show()

    val vectorAssembler=new VectorAssembler()
      .setInputCols(Array("Advertisement"))
      .setOutputCol("features")

    val lr=new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    val pipeline=new Pipeline()
      .setStages(Array(vectorAssembler,lr))

    val Array(trainDF,testDf) =df2.randomSplit(Array(0.8,0.2),142L)

    val pipelineModel=pipeline.fit(trainDF)

    pipelineModel.stages.foreach(println(_))

    val resultDF=pipelineModel.transform(testDf)

    resultDF.show(false)

    val lrModel=pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]

    println(s"R kare:${lrModel.summary.r2}")
    println(s"Duzeltilmiş R kare:${lrModel.summary.r2adj}")
  println(s"rmse: ${lrModel.summary.rootMeanSquaredError}")
    println(s"katsayılar: ${lrModel.coefficients}")
    println(s"sabit: ${lrModel.intercept}")
    println(s"p degerleri:${lrModel.summary.pValues.mkString(",")}")
    println(s"t degerleri:${lrModel.summary.tValues.mkString(",")}")

    resultDF.withColumn("residuals",(col("label")-col("prediction"))).show()

/// regresyon denklemi y=4.537119328969264 + 0.04723638038563483*Advertisement

    val predictionDF=Seq(100).toDF("Advertisement")
    val vectorDF=vectorAssembler.transform(predictionDF)
    lrModel.transform(vectorDF).show()

  }

}
