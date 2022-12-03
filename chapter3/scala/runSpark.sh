#!/bin/sh

sbt clean package
cp target/scala-2.12/main-scala-chapter3_2.12-1.0.jar jars/
spark-submit --class main.scala.chapter3.FireDepartment jars/main-scala-chapter3_2.12-1.0.jar
