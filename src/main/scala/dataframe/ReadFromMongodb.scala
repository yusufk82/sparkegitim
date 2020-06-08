package dataframe
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConcernConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.config._
object ReadFromMongodb {
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

    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    val df=customRdd.toDF()
    df.show()
    println(customRdd.count)
    println(customRdd.first.toJson)

  }

}
