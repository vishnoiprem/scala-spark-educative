



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DfOperation  {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()
    import spark.implicits._ // << add this

    val movies = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/data/BollywoodMovieDetail.csv")
    print(movies.columns)
    print(movies.printSchema())
   movies.show(3)



    //Changing column names
    val moviesNewColDF = movies.withColumnRenamed("hitFlop","Rating")
  print(moviesNewColDF)
    print(moviesNewColDF.printSchema())
    val new_rename_colum = movies.withColumnRenamed(existingName ="actors","actor_name" )
    print(new_rename_colum.printSchema())



    // Changing column types
//    val newDF = movies.withColumn("launchDate", to_date($"releaseDate", "d MMM yyyy"))
//      .drop("releaseDate")

    val NewDF=movies.withColumn("launchDate",to_date($"releaseDate", "d MMM yyyy"))

    print(NewDF.printSchema())
    NewDF.show(10)
//
//    val newDF1 = movies.withColumn("launchDate", to_date($"releaseDate", "d MMM yyyy"))
//
    (NewDF.select("releaseDate","launchDate")
      .where($"launchDate".isNull)
      .show(5,false))
//
    println((NewDF.select("releaseDate","launchDate")
      .where($"launchDate".isNull)
      .count()))
    (NewDF.select(year($"launchDate"))
      .distinct()
      .orderBy(year($"launchDate"))
      .show())

//    // Aggregations
    (NewDF.select("releaseYear")
      .groupBy("releaseYear")
      .count()
      .orderBy("releaseYear")
      .show)

//    (movies.select(max($"hitFlop"))
//      .show)
//
//
//    (movies.select(min($"hitFlop"))
//      .show)
//
//    (movies.select(sum($"hitFlop"))
//      .show)
//
//    (movies.select(avg($"hitFlop"))
//      .show)
//
    (NewDF.select("releaseYear","hitFlop")
      .groupBy("releaseYear")
      .avg("hitFlop")
      .orderBy("releaseYear")
      .show)


  }
}

