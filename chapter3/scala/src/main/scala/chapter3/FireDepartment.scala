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
        |""".stripMargin).show(10)




  }
}
