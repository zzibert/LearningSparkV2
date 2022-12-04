package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{min => Fmin, avg => Favg, max => Fmax, sum => Fsum}

object FireDepartment {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SparkSQLExampleApp")
      .getOrCreate()

    val csvFile = "departuredelays.csv"

    // Read and create temporary view
    // infer schema (note that for larger files you may want to specify the schema)
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    // create temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """SELECT distance, origin, destination
        |FROM us_delay_flights_tbl WHERE distance > 1000
        |ORDER BY distance DESC
        |""".stripMargin)

    df.select("distance", "origin", "destination")
      .where(col("distance") > 1000)
      .orderBy(col("distance").desc)

    spark.sql(
      """SELECT date, delay, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE delay > 120 AND origin='SFO' AND destination='ORD'
        |ORDER BY delay DESC
        |""".stripMargin
    )

    df.select("date", "delay", "origin", "destination")
      .where(col("delay") > 120 && col("origin") === "SFO" && col("destination") === "ORD")
      .orderBy(col("delay").desc)

    spark.sql(
      """SELECT delay, origin, destination,
        |CASE
        | WHEN delay > 360 THEN 'Very Long Delays'
        | WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        | WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        | WHEN delay < 60 THEN 'Tolerable Delays'
        | ELSE 'Early'
        |END AS Flight_Delays
        |FROM us_delay_flights_tbl
        |ORDER BY origin, delay DESC
        |""".stripMargin
    )

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    // Create a managed table
//    spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, " +
//      "origin STRING, destination STRING)")

    // Create an unmanaged table
    spark.sql(
      """
        |CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
        |distance INT, origin STRING, destination STRING)
        |USING csv OPTIONS
        |(PATH 'departuredelays.csv')
        |""".stripMargin)






  }
}
