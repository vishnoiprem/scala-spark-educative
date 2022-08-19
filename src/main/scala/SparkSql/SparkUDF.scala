package SparkSql



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkUDF  {

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
    movies.createOrReplaceTempView("tmp_movie_Vw")

//    val twoDigtYear=(year:Int)=>{ ((year/10)%10).toString + (year%10).toString }
//

    val twoDigitFOrmatYear =(year:Int)=>{((year/10)%10).toString + (year%10).toString}



//    spark.udf.register("shortYear",twoDigtYear)

    spark.udf.register("shortYearFormat",twoDigitFOrmatYear)
    spark.sql("select * FROM tmp_movie_Vw").show()
    spark.sql("select releaseYear, shortYearFormat(releaseYear) FROM tmp_movie_Vw group by releaseYear, shortYearFormat(releaseYear)").show(10)


    //    spark.sql("select * FROM tmp_movie_Vw limit 1").show( truncate=false)
//
//    spark.sql("SELECT title, shortYear(releaseYear) FROM tmp_movie_Vw").show(3)
//
//      spark.sql("SELECT upper(title) FROM tmp_movie_Vw").show(3)
//
//    spark.sql("SELECT random()").show(1)
//
//    spark.sql("SELECT now()").show(1, false)
//
//    spark.sql("SELECT current_timestamp()").show(1, false)


  }
}