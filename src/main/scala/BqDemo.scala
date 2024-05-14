import scala.math.random
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import com.google.cloud.spark.bigquery._

object BqDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Bq Demo")
      // .config("spark.sql.session.timeZone", "America/New_York")
      // .config("spark.master", "local[*]") // local dev
      // .config(
      //   "spark.hadoop.fs.AbstractFileSystem.gs.impl",
      //   "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      // )
      // .config("spark.hadoop.fs.gs.project.id", "cf-data-analytics")
      // .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
      // .config(
      //   "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
      //   "/Users/chasf/Desktop/cf-data-analytics-56659d6eac1c.json"
      // )
      .getOrCreate()

    import spark.implicits._

    val pages = Seq("Google", "Amazon", "Microsoft")

    val df_24 =
      spark.read
        .bigquery("bigquery-public-data.wikipedia.pageviews_2024")
        .filter(to_date($"datehour").between("2024-01-01", "2024-01-31"))
        .filter($"title".isin(pages: _*))
        .filter($"wiki" === "en")
        .groupBy("title")
        .agg(sum("views"))
        .withColumnRenamed("sum(views)", "views_24")

    val df_23 =
      spark.read
        .bigquery("bigquery-public-data.wikipedia.pageviews_2023")
        .filter(to_date($"datehour").between("2023-01-01", "2023-01-31"))
        .filter($"title".isin(pages: _*))
        .filter($"wiki" === "en")
        .groupBy("title")
        .agg(sum("views"))
        .withColumnRenamed("sum(views)", "views_23")

    val df_22 =
      spark.read
        .bigquery("bigquery-public-data.wikipedia.pageviews_2022")
        .filter(to_date($"datehour").between("2022-01-01", "2022-01-31"))
        .filter($"title".isin(pages: _*))
        .filter($"wiki" === "en")
        .groupBy("title")
        .agg(sum("views"))
        .withColumnRenamed("sum(views)", "views_22")

    val df_out = df_24
      .join(df_23, Seq("title"), "inner")
      .join(df_22, Seq("title"), "inner")
      .filter($"title".isin(pages: _*))
      .withColumn(
        "max_views_jan",
        greatest($"views_22", $"views_23", $"views_24")
      )
      .drop(
        "views_22",
        "views_23",
        "views_24"
      )

    df_out.show();

     df_out.write
      .format("bigquery")
      .option("writeMethod", "direct")
      .mode("append")
      .save(
        "cf-data-analytics.dataproc.jan_views_spark"
      )
  }
}
