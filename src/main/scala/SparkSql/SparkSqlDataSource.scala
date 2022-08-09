package SparkSql

import org.apache.spark.sql.SparkSession


object SparkSqlDataSource {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()
    import spark.implicits._

    val movies = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/data/BollywoodMovieDetail.csv")
    print(movies.columns)

    movies.write.saveAsTable("MovieData")
    val movieTitle = spark.sql("select * FROM MovieData")
    movieTitle.show(10)

    movies.write.format("parquet").mode("overwrite").option("compression", "snappy").save("src/main/resources/data/moviesParquet")

    val parquetFp = "src/main/resources/data/moviesParquet/*"
    val readParqut = spark.read.format("parquet").load(parquetFp)

    readParqut.show(10)
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW movies_view USING parquet OPTIONS ( path "src/main/resources/data/moviesParquet/*")""")

    spark.sql("SELECT title FROM movies_view").show(3)

    movies.write.format("json").save("src/main/resources/data/moviesJson")

    val jsonfilePath = "src/main/resources/data/moviesJson/*"
    val jsonDF = spark.read.format("json").load(jsonfilePath)
    jsonDF.show(3)


    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW movies_view_json USING json OPTIONS(path "src/main/resources/data/moviesJson/*")""")


    movies.write.format("CSV")
      .mode("overwrite")
      .save("src/main/resources/data/moviesCSV")


    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW movies_view_csv USING csv OPTIONS (path "src/main/resources/data/moviesCSV/*", header "true", inferSchema "true", mode "FAILFAST")""")


  }
}