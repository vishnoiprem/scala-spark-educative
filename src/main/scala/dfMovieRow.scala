



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object dfMovieRow  {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()

    val movies = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/data/BollywoodMovieDetail.csv")
    print(movies.printSchema())
    val row = Row("Upcoming New Movie", 2021, "Comedy")
    import  spark.implicits._

    print(row(0))
    print("\n")

    print(row(1))
    print("\n")

    print(row(2))
    print("\n")

    val rows = Seq(("Tom Cruise Movie", 2021, "Comedy"), ("Rajinikanth Movie", 2021, "Action"))
    val newMovies = rows.toDF("Movie Name", "Release Year", "Genre")
    newMovies.show()
    print("\n")

//    # Projections and filters


    (movies.select("title")
      .where(col("hitFlop") > 8)
      .show())


    print("\n----1112--1-")



    (movies.select("title")
      .filter($"genre".contains("Romance"))
      .count())
    print("\n")

    (movies.select("title")
      .filter($"genre".contains("Romance"))
      .where($"releaseYear" > 2010)
      .count())
    print("\n")

    (movies.select("releaseYear")
      .distinct()
      .sort($"releaseYear".desc)
      .show())


  }
}

