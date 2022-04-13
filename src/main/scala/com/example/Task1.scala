package com.example

import org.apache.spark.sql.functions.{asc, avg, count, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Task1(val spark: SparkSession) {

  import spark.implicits._

  //  TODO 1 Прочитать csv файл: book.csv
  val booksPath = "src/main/resources/books.csv"
  val df: DataFrame = spark.read.option("header", "true").csv(booksPath)

  //  TODO 2 Вывести схему для dataframe полученного из п.1
  df.printSchema()

  //  TODO 3 Вывести количество записей
  println("row count: " + df.count())

  //  TODO 4 Вывести информацию по книгам у которых рейтинг выше 4.50
  df.filter($"average_rating" > 4.50).show(10)

  //  TODO 5 Вывести средний рейтинг для всех книг
  println("rating avg: " + df.select(avg($"average_rating")).collect()(0)(0))

  //  TODO 6 Вывести агрегированную инфорацию по количеству книг в диапазонах:
  df.filter(($"average_rating").cast("float").isNotNull)
    .select(
      when($"average_rating" < 1, "0 - 1")
      .when($"average_rating" < 2, "1 - 2")
      .when($"average_rating" < 3, "2 - 3")
      .when($"average_rating" < 4, "3 - 4")
      .when($"average_rating" < 5, "4 - 5")
      .when($"average_rating" === 5, "4 - 5")
      .otherwise("undefined")
      .alias("rating"))
    .groupBy($"rating")
    .count()
    .orderBy(asc("rating"))
    .show()
}
