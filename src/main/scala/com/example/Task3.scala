package com.example

import org.apache.spark.sql.functions.{array_max, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Task3(val spark: SparkSession) {

  import spark.implicits._

  val values = List(
    ("1", List(3, 5, 10)),
    ("2", List(2, 20, 3)),
    ("3", List(55, 1, 3)))

  val df: DataFrame = values.toDF("id", "array")
  df.show()

  df.withColumn("max", array_max($"array"))
    .orderBy(desc("max"))
    .drop("max")
    .show()
}
