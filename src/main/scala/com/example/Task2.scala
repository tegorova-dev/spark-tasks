package com.example

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

class Task2(val spark: SparkSession) {

  //  TODO 1 Прочитать файлы
  val uDataPath = "src/main/resources/u.data"
  val uItemPath = "src/main/resources/u.item"

  val movieSchema: StructType = StructType(
    ("movie_id | movie_title | release_date | video_release_date |" +
      "IMDb_URL | unknown | Action | Adventure | Animation |" +
      "Children's | Comedy | Crime | Documentary | Drama | Fantasy |" +
      "Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |" +
      "Thriller | War | Western |")
      .replace(" ", "")
      .split(Array('|'))
      .map(StructField(_, StringType)))

  val ratingSchema: StructType = StructType(
    "user_id | item_id | rating | timestamp"
      .replace(" ", "")
      .split(Array('|'))
      .map(StructField(_, LongType)))

  val movie: DataFrame = spark.read.schema(movieSchema).option("sep", "|").csv(uItemPath)
  val rating: DataFrame = spark.read.schema(ratingSchema).option("sep", "\t").csv(uDataPath)


  //  TODO 2 Создать выходной файл в формате json
  val filmId = 32

}
