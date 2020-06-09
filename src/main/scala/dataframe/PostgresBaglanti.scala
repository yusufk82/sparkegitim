package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions=>F}

object PostgresBaglanti {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val df=spark.read.format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://localhost:5432/postgres")
      .option("dbtable","table")
      .option("user","postgres")
      .option("password","********")
      .load()
  df.show()
  }

}
