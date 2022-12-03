package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FireDepartment {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Example-3_7")
      .getOrCreate()

    val fireDF = spark
      .read
      .option("samplingRatio", 0.001)
      .option("header", true)
      .csv("sf-fire-calls.csv")


    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")

    // Distinct call Types
    val callTypesDF = fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")

    val newFireDF = fireDF
      .withColumnRenamed("Delay", "ResponseDelayedInMins")
      .select("ResponseDelayedInMins")
      .where(col("ResponseDelayedInMins") > 5)

    val fireTsDF = fireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    fireTsDF
      .select(year(col("IncidentDate")))
      .distinct
      .orderBy(year(col("IncidentDate")))


    // Most common types of fire calls
    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count
      .orderBy(desc("count"))
      .show()



  }
}
