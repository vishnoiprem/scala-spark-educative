package SparkSql


// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkWindow {

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


    val actors = spark.read.format("csv")
      .option("header", "true")
      .option("samplingRatio", 0.001)
      .option("inferSchema", "true")
      .load("src/main/resources/data/BollywoodActorRanking.csv")
    print(actors.columns)


    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW movies USING csv OPTIONS (path "src/main/resources/data/BollywoodMovieDetail.csv", header "true", inferSchema "true", mode "FAILFAST")""")
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW  tempTbl1_vw AS SELECT title, releaseYear, hitFlop, split(actors,"[|]") AS actors FROM movies""")
    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW  tempTbl2_vw AS SELECT title, releaseYear, hitFlop, upper(trim(actor)) AS actor FROM (SELECT  title, releaseYear, hitFlop, explode(actors) AS actor FROM tempTbl1_vw)""")

    spark.sql("select * FROM tempTbl2_vw").show(10,truncate = false)

    val sql_query=""" select title, releaseYear,hitFlop,actor, row_number() over(  PARTITION BY releaseYear ORDER BY hitFLop DESC) FROM tempTbl2_vw """

    spark.sql("select * FROM tempTbl2_vw").show(10,truncate = false)
    spark.sql(sql_query).show(20,truncate = false)

        val actorsDF = actors.withColumn("actorName",trim(upper($"actorName")))
    //    actorsDF.show(100)
        val moviesDF = spark.sql("""SELECT * FROM tempTbl2_vw""")
    //    moviesDF.show(10)
        moviesDF.join(actorsDF, $"actor" === $"actorName").show(5,false)

        val df1 = movies.select("title").where( $"releaseYear" > 2010).limit(2)
        val df2 = movies.select("title").where( $"releaseYear" < 2010).limit(2)
        df1.union(df2).show(false)
    //    spark.sql("SELECT title, hitFlop, releaseYear, dense_rank() OVER (PARTITION BY releaseYear ORDER BY hitFLop DESC) as rank  FROM MOVIES").show(3)
    //    spark.sql("SELECT title, hitFlop, releaseYear, dense_rank() OVER (PARTITION BY releaseYear ORDER BY hitFLop DESC) as rank  FROM MOVIES WHERE releaseYear=2013").show(3)
    //    spark.sql("SELECT * FROM (SELECT title, hitFlop, releaseYear, dense_rank() OVER (PARTITION BY releaseYear ORDER BY hitFLop DESC) as rank  FROM MOVIES) tmp WHERE rank <=2 ORDER BY releaseYEar").show(5)
    //
    //  }
  }
}