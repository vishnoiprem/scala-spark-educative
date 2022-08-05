package dataset



// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.regex.Pattern
import org.apache.spark.sql.functions.{col, udf}


object readTextDf  {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples")
      .getOrCreate()
    import spark.implicits._

    val movies = spark.read.format("csv")
      .option("sep","|")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/main/resources/data/BollywoodReview.txt")
    print(movies.columns)
    print(movies.printSchema())
    import spark.implicits._


//    val col_re_list = expr("regexp_extract_all(text, r\"\\b(\\d{5})\\b\" 0)")
//    movies.withColumn("postal", array_join(col_re_list, " ")).show(false)


    // Print the result
    movies.createOrReplaceTempView("tmp_movie_Vw")
    // Find all movies released after 2010 and labelled as average, above average or below average



    import spark.implicits._
    def toExtract(str: String) = {
      val pattern = Pattern.compile("\\d{6}")
      val tmplst = scala.collection.mutable.ListBuffer.empty[String]
      val matcher = pattern.matcher(str)
      while (matcher.find()) {
        tmplst += matcher.group()
      }
      tmplst.mkString(",")
    }
    val Extract1 = udf(toExtract _)
    spark.udf.register("Extract", Extract1)

    val values = List("@always_nidhi @YouTube no i dnt understand bt i loved the music nd their dance awesome all the song of this mve is rocking")
    val sql_select = """select  Extract(account_registration_address)  FROM tmp_movie_Vw"""

    spark.sql(sql_select).show(10)

    //


  }
}