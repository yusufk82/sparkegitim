package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object StokAnaliz {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val df=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/stoklar.csv")

    val dfGirisler=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/girisler.csv")

    println(dfGirisler.count())
    val dfCikislar=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/cikislar.csv")

    val dfDevirler=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/devirler.csv")

    import spark.implicits._
   //df.createOrReplaceTempView("stoks")
  // spark.sql("select NIIN,SUM(GELENMIKTAR),SUM(gidenmiktar),sum(kalanmiktar) from stoks where STK='913' group by NIIN ")
    //  .show(88)

    val dfFilteredStoklar= df.filter("STK==917").groupBy("NIIN").agg(sum("GELENMIKTAR").alias("GELENMIKTAR")
      ,sum("GIDENMIKTAR").alias( "GIDENMIKTAR"),sum("KALANMIKTAR").alias("KALANMIKTAR"),sum("REZERVEMIKTAR").alias("REZERVEMIKTAR"),sum("SEVKGIDEN").alias("SEVKGIDEN"),sum("SERBESTMIKTAR").alias("SERBESTMIKTAR"),sum("ZIMMETTEKIMIKTAR").alias("ZIMMETTEKIMIKTAR"))
    dfFilteredStoklar.filter("NIIN=='270016078'").show()

    val dfFilteredDevirler= dfDevirler.filter("STK==917").
      withColumn("GELECEKYILADEVIRMIKTAR",regexp_replace($"GELECEKYILADEVIRMIKTAR",".",",")).groupBy("NIIN").agg(sum("GELECEKYILADEVIRMIKTAR").alias("DEVIRMIKTAR"))
      dfFilteredDevirler.filter("NIIN=='270435039'").show()
    //  dfFilteredDevirler.show(10 )

    val dfFilterdGirisler=dfGirisler.filter("STK==917 AND MALZEMEGIRISDURUM==2").groupBy("NIIN").agg(sum("MIKTAR").alias("GIRISMIKTAR"))
    dfFilterdGirisler.filter("NIIN=='270016078'").show()

    val dfFilteredCikislar=dfCikislar.filter("STK==917").filter("CIKISDURUM==2")
      .filter("KABULDURUM==1 OR (KABULDURUM==0 AND ALICIBIRIMTIP!=1)").groupBy("NIIN").agg(sum("MIKTAR").alias("CIKISMIKTAR"))
 println(dfFilteredCikislar.count())

    val stokDevirJoin= dfFilteredStoklar.join(dfFilteredDevirler, "NIIN")
    val girisJoin=stokDevirJoin.join(dfFilterdGirisler,"NIIN")
    val cikisJoin=girisJoin.join(dfFilteredCikislar,"NIIN")

    val filteredCikis=cikisJoin.select($"NIIN",$"DEVIRMIKTAR",$"GIRISMIKTAR",$"CIKISMIKTAR",$"REZERVEMIKTAR",$"KALANMIKTAR",(($"DEVIRMIKTAR"+$"GIRISMIKTAR")-$"CIKISMIKTAR").alias("SONUC"),
      ((($"DEVIRMIKTAR"+$"GIRISMIKTAR")-$"CIKISMIKTAR")-$"KALANMIKTAR").alias("FARK"))

    val sonucFilteredListe=filteredCikis.filter("SONUC!=KALANMIKTAR")
    //.withColumn("KOMUT",concat(lit("UPDATE STOKAMBAR SET GELENMIKTAR=GELENMIKTAR"),col("FARK"),lit(",SERBESTMIKTAR=SERBESTMIKTAR"),col("FARK"),lit(",KALANMIKTAR=KALANMIKTAR"),col("FARK"),lit(" WHERE RID=")))
   // sonucFilteredListe.show(truncate = false)
    sonucFilteredListe.coalesce(1)
      .write.mode("Overwrite")
      .option("sep",",")
      .option("header",true)
      .csv( "/home/oracle/opensource/dataset/rapor/917hatalar")

    // df.show(20)
  }

}
