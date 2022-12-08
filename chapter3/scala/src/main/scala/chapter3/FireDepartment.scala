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

//    val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
//    val usFlightsDF2 = spark.table("us_delay_flights_tbl")
//
//    val file = "2010-summary.parquet"
//
////    val df = spark.read.format("parquet").load(file)
//    val df2 = spark.read.load(file)
//
//    // use csv
//    val df3 = spark.read.format("csv")
//      .option("inferSchema", "true")
//      .option("header", "true")
//      .option("mode", "PERMISSIVE")
//      .load("csv/*")
//
//    // use json
//    val df4 = spark.read.format("json")
//      .load("json/*")
//
//    df.write
//      .mode("overwrite")
//      .saveAsTable("us_delay_flights_tbl")

    // User defined functions
    val cubed = (s: Long) => {
      s * s * s
    }

    // Register UDF
    spark.udf.register("cubed", cubed)

    // Create temporary view
    spark.range(1,9).createOrReplaceTempView("udf_test")

    // Query the cubed UDF
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test")

    import spark.implicits._

    // Create DataFrame with two rows of two arrays (tempc1, tempc2)
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val tC = Seq(t1, t2).toDF("celsius")

    tC.createOrReplaceTempView("tC")

    // Calculate Fahrenheit from Celsius for an array of temperatures
    spark.sql(
      """
        |SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
        |FROM tC
        |""".stripMargin)

    spark.sql(
      """
        |SELECT celsius, filter(celsius, t -> t > 38) as high
        |FROM tC
        |""".stripMargin)


    spark.sql(
      """
        |SELECT celsius, exists(celsius, t -> t > 38) as threshold
        |FROM tC
        |""".stripMargin
    )

    val reduce = (a: Array[Int], acc: Int, f1: (Array[Int], Int) => Int, f2: Int => Int) => {
      val result = a.fold(0)(_ + _)

      f2(result)
    }

    spark.udf.register("reduce", reduce)

    // Calculate average temperature and convert to F
    spark.sql("""
       SELECT celsius,
       aggregate(
          celsius,
          0,
          (t, acc) -> t + acc,
          acc -> (acc div size(celsius) * 9 div 5) + 32
        ) as avgFahrenheit
      FROM tC
     """)

    // Set file paths
    val delaysPath = "departuredelays.csv"
    val airportsPath = "airport-codes-na.txt"

    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)

    airports.createOrReplaceTempView("airports_na")

    // Obtain departure Delays data set
    val delays = spark.read
      .option("header", "true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay AS INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))

    delays.createOrReplaceTempView("departureDelays")

    // create temporary small table
    val foo = delays.filter(
      expr(
        """
          |origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0
          |""".stripMargin))

    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM foo")

    // Union two tables
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")

    bar.filter(expr(
      """origin == 'SEA' AND destination = 'SFO'
        |AND date LIKE '01010%' AND delay > 0
        |""".stripMargin))

    foo.join(
      airports.as('air),
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination")

    // add new column
    import org.apache.spark.sql.functions.expr
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )

    foo2.show()
















  }
}
