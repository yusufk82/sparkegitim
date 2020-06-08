package dataframe


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions=>F}

object WriteToKafka {

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
    dfFromFile.show()

    val df2=dfFromFile.withColumn("key",F.col("NIIN")).drop("ID")
      .withColumn("value",F.concat(
        F.col("AD"),F.lit(","),
        F.col("GELENMIKTAR"),F.lit(","),
        F.col("GIDENMIKTAR"),F.lit(","),
        F.col("STK"),F.lit(","),
        F.col("REZERVEMIKTAR"),F.lit(","),
        F.col("KALANMIKTAR"),F.lit(","),
        F.col("SERBESTMIKTAR"),F.lit(","),
        F.col("ZIMMETTEKIMIKTAR"),F.lit(","),
        F.col("SEVKGIDEN"),F.lit(","),
        F.col("BIRIMFIYAT"),F.lit(","),
        F.col("AMBARREF"),F.lit(","),
        F.col("AMBARBOLUMREF"),F.lit(",")
      )).drop("STK","GELENMIKTAR","GIDENMIKTAR","AD","REZERVEMIKTAR","KALANMIKTAR","SERBESTMIKTAR","ZIMMETTEKIMIKTAR","SEVKGIDEN","BIRIMFIYAT","AMBARREF","AMBARBOLUMREF")
    df2.show()
    df2.write.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","deneme")
      .save()
  }

}
