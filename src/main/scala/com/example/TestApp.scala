package com.example

import org.apache.spark.sql.SparkSession

object TestApp extends App {
  val spark: SparkSession = SparkSession.builder().master("local[4]").getOrCreate()

  val task1 = new Task1(spark)
  val task2 = new Task2(spark)
  val task3 = new Task3(spark)

}
