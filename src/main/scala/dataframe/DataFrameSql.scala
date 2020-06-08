package dataframe


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object DataFrameSql {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val dfFromFile=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/stoklar.csv")
    dfFromFile.cache()
    dfFromFile.createOrReplaceTempView("tablo")
    spark.sql("select NIIN,SUM(GelenMiktar) miktar from tablo group by NIIN order by miktar desc"

    ).show(20)

    println("ikinci sorgu")

    spark.sql("select NIIN,SUM(GelenMiktar) gm,sum(kalanmiktar) km from tablo group by NIIN order by gm desc"

    ).show(20)
  }
}
