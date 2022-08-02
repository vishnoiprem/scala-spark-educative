package dataset



// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object readTextDf  {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()
    import spark.implicits._

    val movies = spark.read.format("csv")
      .option("header","true")
      .option("sep"," ")
      .option("inferSchema","true")
      .load("src/main/resources/data/BollywoodReview.txt")
    print(movies.columns)

    // Print the result
    movies.createOrReplaceTempView("tmp_movie_Vw")
    // Find all movies released after 2010 and labelled as average, above average or below average

    val sql_select ="""select id FROM tmp_movie_Vw"""
    spark.sql(sql_select).show(3)

    //


  }
}