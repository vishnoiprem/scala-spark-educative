package dataset

case class BMovieDetail(imdbID: String,
                        title: String,
                        releaseYear: Int,
                        releaseDate: String,
                        genre: String,
                        writers: String,
                        actors: String,
                        directors: String,
                        sequel: String,
                        rating: Int)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, Row, SparkSession}


object dataframeCase  {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder.appName("SequelMoviesCount").config("spark.master", "local").getOrCreate()
    import spark.implicits._


    val caseClassSchema = Encoders.product[BMovieDetail].schema
    val movieDataset = spark.read.option("header",true).schema(caseClassSchema).csv("src/main/resources/data/BollywoodMovieDetail.csv").as[BMovieDetail]


     movieDataset.filter(row => {row.releaseYear > 2010})
    var row = Row("Upcoming New Movie", 2021, "Comedy")
    print(row.getInt(0))

  }
}