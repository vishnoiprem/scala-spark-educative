// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CountSequelMovies  {

  def main(args: Array[String]): Unit = {

    // Create the SparkSession object. We get it by default in the Spark
    // shell but we need to create one ourselves in the Scala program.
    val spark = SparkSession.builder.appName("SequelMoviesCount").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    // Load the file in CSV format.
    val movies = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/data/BollywoodMovieDetail.csv")

    // Query to count the rows with
    // column sequel set to 1. Note that the column
    // sequel is interpreted as a String and not an
    // integer. We also name the aggregated column
    // "Number of Sequels Produced" using the alias
    // function.
    val numSequels = movies.select("sequel")
      .where(col("sequel")==="1")
      .agg(count("sequel"))
      .alias("Number of Sequels Produced")

    // Print the result
    numSequels.show()

  }
}