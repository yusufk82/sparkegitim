package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions=>F}

object DateTimeOps {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val dfFromFile=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/OnlineRetail.csv").select("InvoiceDate").distinct()
  dfFromFile.show()

      val mevcutFormat="dd.MM.yyyy HH:mm"
    val formatTR="dd/MM/yyyy HH:mm:ss"
    val formatEng="MM-dd-yyyy HH:mm:ss"
    val df2=dfFromFile.withColumn("InvoiceDate",F.trim($"InvoiceDate"))
      .withColumn("NormalTarih",F.to_date($"InvoiceDate",mevcutFormat))
        .withColumn("StandartTS",F.to_timestamp($"InvoiceDate",mevcutFormat))
        .withColumn("UnixTS",F.unix_timestamp($"StandartTS"))
        .withColumn("FormatTR",F.date_format($"StandartTS",formatTR))
      .withColumn("FormatENG",F.date_format($"StandartTS",formatEng))
        .withColumn("BirYil",F.date_add($"StandartTS",365))
        .withColumn("year",F.year($"StandartTS"))
        .withColumn("fark",F.datediff($"BirYil",$"StandartTS"))

    df2.show()
  }

}
