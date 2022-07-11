

// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DfSchemaMovie  {

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

      print(movies.schema)
      print("\n")


    val customSchema = (StructType(Array(StructField("imdbId",StringType,true),
      StructField("title",StringType,true),
      StructField("releaseYear",IntegerType,true),
      StructField("releaseDate",StringType,true),
      StructField("genre",StringType,true),
      StructField("writers",StringType,true),
      StructField("actors",StringType,true),
      StructField("directors",StringType,true),
      StructField("sequel",IntegerType,true),
      StructField("hitFlop",IntegerType,true))))
      print(movies.schema)
      print("\n")
      print(customSchema)
//
//
//
    val movies1 = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","false")
      .schema(customSchema)load("src/main/resources/data/BollywoodMovieDetail.csv"))

    movies1.schema
//
    movies1.show(1, false)
print("\n")
    val movies2 = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("samplingRatio", 0.01)
      .load("src/main/resources/data/BollywoodMovieDetail.csv"))

     print(movies2.schema)
//
//    //    # Specifying schema using DDL
//    val ddl = "imdbId STRING, title STRING, releaseYear STRING, releaseDate STRING, genre STRING, writers STRING, actors STRING, directors STRING, sequel INT, hitFlop INT"
//
//    val movies3 = spark.read.format("csv")
//      .option("header","true")
//      .option("inferSchema","false")
//      .schema(ddl).load("src/main/resources/data/BollywoodMovieDetail.csv")

//    //
//////    # Specifyig DATE types in schema
    val ddl2 = "imdbId STRING, title STRING, releaseYear DATE, releaseDate DATE, genre STRING, writers STRING, actors STRING, directors STRING, sequel INT, hitFlop INT"

    val movies4 = (spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","false")
      .schema(ddl2).load("src/main/resources/data/BollywoodMovieDetail.csv"))

      movies4.show(1, false)
////
//////    # Writing out DataFrames
//    (movies4.write.format("parquet").mode(SaveMode.Overwrite)
//      .save("src/main/resources/data/moviesFile"))



  }
}