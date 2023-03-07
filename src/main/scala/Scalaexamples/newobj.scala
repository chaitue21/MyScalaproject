package Scalaexamples

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object newobj {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("newobj").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
   // import spark.sql
    val data = "C:\\Users\\Dev12\\Downloads\\drive-dataset\\10000Records.csv"
    val df = spark.read.format(source = "csv").option("header", "true").option("inferSchema", "true").load(data)
    df.show(5)
    spark.stop()
  }
}