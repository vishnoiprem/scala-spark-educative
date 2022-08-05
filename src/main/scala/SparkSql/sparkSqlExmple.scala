package SparkSql




// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkSqlExmple  {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()
    import spark.implicits._

    val movies = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/data/BollywoodMovieDetail.csv")
      print(movies.columns)

    spark.sql("CREATE DATABASE if not exists spark_course")
    spark.sql("USE spark_course")
    spark.sql("SHOW TABLES").show(5, false)

    spark.sql("CREATE TABLE if not exists movieShortDetail(imdbID String, title String)")

    spark.sql("SHOW TABLES").show(5, false)
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
//    movies.write.option("mode","overwrite").saveAsTable("movieShortDetailUsingDataFrame")
    spark.sql("CREATE TABLE if not exists movieShortDetailUnmanaged (imdbID STRING, title STRING) USING csv OPTIONS (PATH 'src/main/resources/data/BollywoodMovieDetail.csv')")
    movies.write.option("path","src/main/resources/data/shortMovieDetail.csv").option("mode","overwrite").saveAsTable("movieShortDetailUsingDataFrameUnmanaged")


    spark.catalog.listTables().show(5, false)
    spark.catalog.listColumns("movieshortdetail").show(10, false)
    spark.catalog.listDatabases().show(10, false)
    movies.write.saveAsTable("movies")
    spark.sql("CREATE OR REPLACE TEMP VIEW high_rated_movies AS SELECT title FROM movies WHERE hitFlop > 7")
    spark.catalog.listTables().show(3)
    spark.catalog.listTables().show(5,false)
    spark.sql("CREATE OR REPLACE GLOBAL TEMP VIEW high_rated_movies_global AS SELECT title FROM movies WHERE hitFlop > 7")
    spark.sql("SELECT * FROM global_temp.high_rated_movies_global").show(3, false)
    spark.catalog.listDatabases().show(3)

    spark.catalog.listTables().show(3)

    spark.catalog.listColumns("movies").show(3)






  }
}