package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object StokGirisHatalari {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[2]").appName("StokGirisHatalari")
      .config("spark.driver.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val dfStoklar=spark.read.format("csv").option("sep",";").option("header",true).option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/stoklar.csv")
    val dfGirisler=spark.read.format("csv").option("sep",";").option("header",true).option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/girisler.csv")
    val dfDevirler=spark.read.format("csv").option("sep",";").option("header",true).option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/devirler.csv")

    val filteredGirisler=dfGirisler.filter("STK==917 AND  MALZEMEGIRISDURUM==2")
        .groupBy("NIIN").agg(sum($"MIKTAR").alias("MIKTAR"))
     val filteredStoklar=dfStoklar.filter("STK==917").groupBy("NIIN").agg(sum($"GELENMIKTAR").alias("GELENMIKTAR"),sum($"GIDENMIKTAR").alias("GIDENMIKTAR"),
       sum($"KALANMIKTAR").alias("KALANMIKTAR"),sum($"REZERVEMIKTAR"),sum($"SERBESTMIKTAR"),sum($"ZIMMETTEKIMIKTAR"),sum($"TAKILIMIKTAR"),sum($"SEVKGIDEN"))
    val filteredDevirler=dfDevirler.filter("STK==917").groupBy("NIIN").agg(sum($"GELECEKYILADEVIRMIKTAR").alias("GELECEKYILADEVIRMIKTAR"))
      //.withColumn("GELECEKYILADEVIRMIKTAR",regexp_replace($"GELECEKYILADEVIRMIKTAR",".",","))


    //filteredGirisler.show(5)
    //filteredStoklar.show(5)
    //filteredDevirler.show(5)
    val joinDf=filteredStoklar.join(filteredDevirler,"NIIN").join(filteredGirisler,"NIIN")
      .withColumn("HESAPLANAN",(($"GELECEKYILADEVIRMIKTAR"+$"MIKTAR")-$"GIDENMIKTAR")).sort("NIIN")
    //joinDf.show(10)
    joinDf.coalesce(1)
      .write.mode("Overwrite")
      .option("sep",",")
      .option("header",true)
      .csv("/home/oracle/opensource/dataset/rapor/917stoklar")
  }
}
