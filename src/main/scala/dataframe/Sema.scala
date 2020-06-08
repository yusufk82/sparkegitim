package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sema {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder().master("local[4]").appName("RddOlusturma")
      .config("spark.driver.memory","2g").config("spark.executor.memory","4g").getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val df=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/OnlineRetail.csv")

    df.show(10)
    df.printSchema()

    import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,FloatType,DoubleType,DateType}
    val retailManuelSchema=new StructType(
      Array(
        new StructField("InvoiceNo",StringType,nullable = true),
        new StructField("StockCode",StringType,nullable = true),
        new StructField("Description",StringType,nullable = true),
        new StructField("Quantity",IntegerType,nullable = true),
        new StructField("InvoiceDate",StringType,nullable = true),
        new StructField("UnitPrice",FloatType,nullable = true),
        new StructField("CustomerID",IntegerType,nullable = true),
        new StructField("Country",StringType,nullable = true)
      )
    )

    val df2=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .schema(retailManuelSchema)
      .load("/home/oracle/opensource/dataset/OnlineRetail.csv")
    df2.show(10)

    val df3=spark.read.format("csv").option("sep",";")
      .option("header",true)
      .option("inferSchema",true)
      .load("/home/oracle/opensource/dataset/OnlineRetail.csv")
      .withColumn("UnitPrice",regexp_replace($"UnitPrice",",","."))
      df3.show()

    df3.coalesce(1)
      .write.mode("Overwrite")
      .option("sep",",")
      .option("header",true)
      .csv( "/home/oracle/opensource/dataset/duzenlenenveri")

    val df4=spark.read.format("csv").option("sep",",")
      .option("header",true)
      .option("inferSchema",true)
      .schema(retailManuelSchema)
      .load("/home/oracle/opensource/dataset/duzenlenenveri")
  }

}
