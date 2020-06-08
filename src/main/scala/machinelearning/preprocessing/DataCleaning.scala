package machinelearning.preprocessing


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions=>F}

object DataCleaning {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val trainDf=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/adult.data")

    val testDf=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/adult.test")

    val adultWholeData=trainDf.union(testDf)
    adultWholeData.show(6)

    ///////////sutunlarda boşluk kontrolü boşluk kontrolü string alanlarda yapılır//////////
    val adulWholeDataDFI=adultWholeData
      .withColumn("workClass",trim(col("workClass")))
      .withColumn("education",trim(col("education")))
      .withColumn("marital_status",trim(col("marital_status")))
      .withColumn("occupation",trim(col("occupation")))
      .withColumn("relationship",trim(col("relationship")))
      .withColumn("race",trim(col("race")))
      .withColumn("sex",trim(col("sex")))
      .withColumn("native_country",trim(col("native_country")))
      .withColumn("output",trim(col("output")))


    //////////////output temizliği/////////////

    val adultWholeDF2=adulWholeDataDFI
      .withColumn("output",regexp_replace(col("output"),"<=50K.","<=50K"))
      .withColumn("output",regexp_replace(col("output"),">50K.",">50K"))

    adultWholeDF2.groupBy($"output")
      .agg(count($"*").as("sayi"))
      .show(false)

    var forSayacKontrol=1
    for (sutun<-adultWholeDF2.columns){
      if(adultWholeDF2.filter(col(sutun).contains("?")).count()>0){
        println(forSayacKontrol+"."+sutun+" içinde ? var")
      }else
        {
          println(forSayacKontrol+"."+sutun)
        }
      forSayacKontrol+=1
    }

    adultWholeDF2.select("workClass","occupation","native_country","output")
      .filter(col("workClass").contains("?") || col("occupation").contains("?")
      || col("native_country").contains("?") || col("output").contains("?")).groupBy("workClass","occupation","native_country","output")
      .count().orderBy(col("count").desc).show(50)

    var forSayacNull=1
    for (sutun<-adultWholeDF2.columns){
      if(adultWholeDF2.filter(col(sutun).isNull).count()>0){
        println(forSayacNull+"."+sutun+" içinde null var")
      }else
      {
        println(forSayacNull+"."+sutun)
      }
      forSayacNull+=1
    }

    val adultWholeDF3=adultWholeDF2
      .filter(!(col("workClass").contains("?") || col("occupation").contains("?")
        || col("native_country").contains("?")))

    println("temizlik öncesi" + adultWholeDF2.count())
    println("temizlik sonrası" + adultWholeDF3.count())

    val adultWholeDF4=adultWholeDF3
      .filter(!(col("workClass").contains("never-worked") || col ("workClass").contains("without-pay")
        || col("occupation").contains("Armed-Forces") || col("native_country").contains("Holand-Netherlands")))

    println("temizlik öncesi" + adultWholeDF3.count())
    println("temizlik sonrası" + adultWholeDF4.count())

    val adultWholeDF5=adultWholeDF4
      .withColumn("education_merged",when(col("education").isin("1st-4th","5th-6th","7th-8th"),"Elementary School")
      .when(col("education").isin("9th","10th","11th","12th"),"High School")
      .when(col("education").isin("Masters","Doctorate"),"Postgraduate")
      .when(col("education").isin("Bachelors","Some-college"),"Undergraduate")
      .otherwise(col("education")))

    println("birleştirilmiş eğitim")
    adultWholeDF5.groupBy($"education_merged")
      .agg(count($"*").as("sayi"))
      .show(false)

   // adultWholeDF5.show(false)

    val nitelikSiralama = Array[String]("workclass", "education", "education_merged", "marital_status", "occupation", "relationship", "race",
      "sex", "native_country", "age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week","output")

    val adultWholeData6 =adultWholeDF5.select(nitelikSiralama.head,nitelikSiralama.tail:_*)

    adultWholeData6.coalesce(1)
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header",true)
      .csv("/home/oracle/opensource/dataset/adultdatacleaningfinal")



  }


}
