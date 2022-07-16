


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DFMovieColumn  {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()

    val movies = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/data/BollywoodMovieDetail.csv")
     print(movies.columns)


    print(movies.printSchema())
//    //Accessing columns
//    var ratingCol1 = movies.col("hitFlop")
//    movies.select(ratingCol1).show(5)
//
    var ratingCol2= movies.col("hitFlop")
    print(ratingCol2)
    movies.select(ratingCol2).show(10)


//    movies.select("hitFlop").show(5)
//    movies.select(expr("hitFlop")).show(5)

    movies.select("title").show(3)


    (movies.select(expr("hitFlop > 5")).show(3))

    (movies.select(movies.col("hitFlop") > 5) show(3))
//
//    (movies.withColumn("Good Movies to Watch", expr("hitFlop > 5")).show(3))
    movies.withColumn("Good Movies to Watch",expr("hitFlop>5")).show(10)
    movies.withColumn("testdata for 2001",expr("releaseYear>2001")).show(2)
//    (movies.withColumn("movie rating condition", expr("hitFlop">5)).show(10))
    (movies.withColumn("Good Movies to Watch", expr("hitFlop > 5")).select("Good Movies to Watch").show(3))
    var ratingCol4 = movies.col("hitFlop")

//
//    (movies.sort(ratingCol4).show(3))
    movies.sort(ratingCol4.desc).show(100)

//    (movies.sort($"hitFlop".desc).show(3))


  }
}

