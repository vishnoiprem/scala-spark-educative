
// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataframe_avg  {

  def main(args: Array[String]): Unit = {

    // Create the SparkSession object. We get it by default in the Spark
    // shell but we need to create one ourselves in the Scala program.
    val spark = SparkSession.builder.appName("SequelMoviesCount").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    // Load the file in CSV format.
//    val dataDF = spark.createDataFrame(Seq(("Gone with the Wind", 6),
//      ("Gone with the Wind", 8), ("Gone with the Wind", 8))).toDF("movie", "rating")
//    val avgDF = dataDF.groupBy("movie").agg(avg("rating"))
//    avgDF.show()

  val df= spark.createDataFrame(Seq(("item",1),("item",3),("itme 2 ",2))).toDF("name","value")
    df.show()
    val avg_df=df.groupBy("name").agg( count("value"))
    avg_df.show()
    // Print the result

  }
}