package dataframe

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConcernConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config._

object WriteToMongoDb {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
      .getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val df=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/rapor/stoklar.csv")
    df.show()

    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
    MongoSpark.save(df,writeConfig)
  }

}
