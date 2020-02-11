package com.df.aadhar.card.analysis.using.datasets2

import java.lang.String
import java.text.SimpleDateFormat

import scala.xml.XML

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//
//val df = Seq(("a", 10), ("a", 10), ("a", 20)).toDF("col1", "col2")
//
//val windowSpec = Window.partitionBy("col1").orderBy("col2")
//
//df
//  .withColumn("rank", rank().over(windowSpec))
//  .withColumn("dense_rank", dense_rank().over(windowSpec))
//  .withColumn("row_number", row_number().over(windowSpec)).show
//
//+----+----+----+----------+----------+
//|col1|col2|rank|dense_rank|row_number|
//+----+----+----+----------+----------+
//|   a|  10|   1|         1|         1|
//|   a|  10|   1|         1|         2|
//|   a|  20|   3|         2|         3|
//+----+----+----+----------+----------+
object AadhardDS_KPI1 {

  case class DataDict(
    Date1:            String,
    Registrar:        String,
    private_Agency:   String,
    State:            String,
    District:         String,
    Sub_District:     String,
    Pin_Code:         String,
    Gender:           String,
    Age:              String,
    Aadhar_Generated: Int,
    Rejected:         String,
    Mobile_No:        String,
    Email_Id:         String)

  def main(args: Array[String]) = {
    //		System.setProperty("hadoop.home.dir", "D:\\Anish_Training_Data\\setups\\hadoop_setups\\CDH5\\hadoop-2.5.0-cdh5.3.2")
    //			System.setProperty("spark.sql.warehouse.dir", "file:/D:/Anish_Training_Data/setups/Spark_Setups/spark-2.0.0-bin-hadoop2.6/spark-warehouse")

    System.setProperty("spark.sql.warehouse.dir", "file:///D:/Bigdata/spark-warehouse");

    System.setProperty("hadoop.home.dir", "D://Bigdata//");

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    val format2 = new SimpleDateFormat("yyyy-MM");

    val spark = SparkSession
      .builder
      .appName("Aadhar Card Analysis using DataSet KPI -1")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.expressions._
    import org.apache.spark.sql.functions._

    val data = spark.read.text("D://Bigdata//spark//aadhaar_data.csv").map {
      line => line.toString().split(",")
    }.map {
      tokens =>
        DataDict(tokens(0), tokens(1), tokens(2), tokens(3), tokens(4), tokens(5), tokens(6), tokens(7), tokens(8), tokens(9).toInt, tokens(10), tokens(11), tokens(12))
    }

    //1. View/result of the top 25 rows from each individual store

    //    val data2=data.gr
    val data2 = data.groupBy("private_Agency").agg(sum(data("Aadhar_Generated")).alias("sum"))
      .select(col("private_Agency").alias("privAgen"), col("sum"))

    //    val datajoined = data.join(data2, data("private_Agency") === data2("privAgen")).dropDuplicates()
    //    datajoined.printSchema()

    //    val windowSpec = Window.partitionBy(datajoined.col("private_Agency")).orderBy(datajoined.col("sum").desc)
    val windowSpec = Window.partitionBy(data2.col("privAgen")).orderBy(data2.col("sum").desc)
    ////
    val data3 = data2.withColumn("rowid", row_number().over(windowSpec))
      .filter(col("rowid") <= 25)
    //
    data3.rdd.repartition(1).saveAsTextFile("D://outputAdar-DS-KPI-1")

    spark.stop()

  }
}
