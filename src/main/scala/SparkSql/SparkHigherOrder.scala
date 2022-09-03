



package SparkSql



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkHigherOrder  {

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

    spark.sql("select * FROM tmp_movie_Vw ").show(10,truncate = false)

//    spark.sql("select actors , split(actors,\"[|]\") as actors_name  FROM tmp_movie_Vw ").show(10,truncate = false)

    spark.sql("select title, releaseYear ,hitFlop ,actors , split(actors, \"[|]\") as  actor from  tmp_movie_Vw").show(10,truncate = false)
    spark.sql("select title, releaseYear ,hitFlop ,actors , explode(split(actors, \"[|]\")) as  actor from  tmp_movie_Vw").show(10,truncate = false)
//    spark.sql("create or replace temp view  movie_vw_temp1 as select title, releaseYear, hitFlop ,actors , explode(split(actors, \"[|]\")) as  actor from  tmp_movie_Vw")
        spark.sql("create or replace temp view   movie_vw_temp1 as select actors ,title,releaseYear, hitFlop, explode(split(actors,\"[|]\")) as actors_name  FROM tmp_movie_Vw ")


    spark.sql("select  avg(hitFlop) as rate ,  actors_name from  movie_vw_temp1 group by actors_name").show(10,truncate = false)
    spark.sql("select collect_list(title) AS allTitles from movie_vw_temp1").show(10,truncate = false)
    spark.sql("select collect_list(title) AS allTitles from movie_vw_temp1").show(10,truncate = false)


    //    spark.sql("create or replace temp view   vw_movie as select actors ,title,releaseYear, hitFlop, explode(split(actors,\"[|]\")) as actors_name  FROM tmp_movie_Vw ")
//    spark.sql(" select actors ,title,releaseYear, hitFlop, actors_name  FROM vw_movie ").show(10,truncate = false)



//    CREATE OR REPLACE TEMPORARY VIEW movies USING csv OPTIONS (path "/data/BollywoodMovieDetail.csv", header "true", inferSchema "true", mode "FAILFAST");
//
//    SELECT title, releaseYear, hitFlop, split(actors,"[|]") AS actors FROM movies;
//
//    CREATE TABLE tempTbl1 AS SELECT title, releaseYear, hitFlop, split(actors,"[|]") AS actors FROM movies;
//
//    SELECT  title, releaseYear, hitFlop, explode(actors) AS actor FROM tempTbl1 LIMIT 10;
//
//    CREATE TABLE tempTbl2 AS SELECT title, releaseYear, hitFlop, trim(actor) AS actor FROM (SELECT  title, releaseYear, hitFlop, explode(actors) AS actor FROM tempTbl1);
//
//    select * from tempTbl2 limit 10;
//
//    SELECT actor, avg(hitFlop) AS score from tempTbl2 GROUP BY actor ORDER BY score desc LIMIT 5;
//
//    select array_distinct(allTitles) FROM (select collect_list(title) AS allTitles from tempTbl2);
//
//    select transform(actors, actor -> upper(actor)) from tempTbl1 LIMIT 5;


  }
}