package dataframe


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrameGiris {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext

    import spark.implicits._
    val df=sc.parallelize(List(1,2,3,4,5,6,7,8)).toDF("rakamlar")
    df.printSchema()

    val dfCreateRange=spark.range(10,100,6).toDF("altili")
    dfCreateRange.printSchema()

    val dfFromFile=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/stoklar.csv")
    dfFromFile.printSchema()
    dfFromFile.show(10,false)
    println(dfFromFile.count())

    dfFromFile.select("stk","niin","gelenMiktar").show(10)
    dfFromFile.sort("stk").show(5)
    dfFromFile.sort(dfFromFile.col("stk")).show(10)
    dfFromFile.sort($"STK").explain()

  }
}
