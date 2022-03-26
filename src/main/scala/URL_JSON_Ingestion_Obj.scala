import org.apache.spark._
import sys.process._
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext.jarOfObject
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.util.parsing.json.JSON
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source._

object URL_JSON_Ingestion_Obj {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ComplexDataProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder()
      .config("fs.s3a.access.key", "")
      .config("fs.s3a.secret.key", "")
      .getOrCreate()

    println("=====Read URL data=====")
    val urldata = fromURL("https://randomuser.me/api/0.8/?results=10").mkString
   print(urldata)
   val rdd = sc.parallelize(List(urldata))
   val df = spark.read.json(rdd)
   df.show()
   df.printSchema()
  println("====Flatten URL JSON data====")
   val flattendata = df.withColumn("results", expr("explode(results)"))
   flattendata.show()
   flattendata.printSchema()

   val flattendf1 = flattendata.select(
     col("nationality"),
     col("results.user.cell"),
     col("results.user.dob"),
     col("results.user.gender"),
     col("results.user.location.*"),
     col("results.user.md5"),
     col("results.user.name.*"),
     col("results.user.password"),
     col("results.user.phone"),
       col("results.user.picture.*"),
     col("results.user.registered"),
     col("results.user.salt"),
     col("results.user.sha1"),
     col("results.user.sha256"),
     col("results.user.username")


   )

   flattendf1.show()
   flattendf1.printSchema()


  }
}