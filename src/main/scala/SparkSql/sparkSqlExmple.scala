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

    // Print the result
    movies.createOrReplaceTempView("tmp_movie_Vw")
    // Find all movies released after 2010 and labelled as average, above average or below average

    val sql_select ="""select * FROM tmp_movie_Vw where releaseYear>2010"""
    spark.sql(sql_select).show(3)

//    spark.sql("SELECT title FROM tmp_movie_Vw WHERE releaseYear > 2010 ORDER BY title desc").show(3)
    val sql= """
                SELECT title,
                hitFlop,
                CASE
                  WHEN hitFlop < 5 THEN 'below average'
                  WHEN hitFlop = 5 THEN 'average'
                  WHEN hitFlop > 5 THEN 'above average' END AS MovieRating
                FROM tmp_movie_Vw WHERE releaseYear > 2010 ORDER BY hitFlop desc, title desc
                """
//
    val df = spark.sql(sql)
    df.show(10)
//
//    # Equivalent query using DatFrame API
    (movies.where($"releaseYear" > 2010)
      .withColumn("MovieRating",
        when($"hitFlop" === 5,"average" )
          .when($"hitFlop">5,"above average")
          .otherwise("below average"))
      .select($"title", $"MovieRating")
      .sort(desc("title"))
      .show(10))



  }
}