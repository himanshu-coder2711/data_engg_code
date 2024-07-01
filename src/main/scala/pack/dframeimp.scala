package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object dframeimp {
  case class columns(id: String, category: String, product: String, spendby: String)

  def main(args: Array[String]): Unit = {

    println("===Reading DrData===")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.driver.host", "localhost")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

val csvdf=spark.read.format("csv").option("header","true").load("file:////Users/himanshusingh/Desktop/bigdata/usdata.csv")

    csvdf.show()

val orcdf=spark.read.format("orc").load("file:////Users/himanshusingh/Desktop/bigdata/part.orc")

orcdf.show()

val jsondf=spark.read.format("json").load("file:////Users/himanshusingh/Desktop/bigdata/devices.json")

jsondf.show()

val parquetdf=spark.read.format("parquet").load("file:////Users/himanshusingh/Desktop/bigdata/part.parquet")

parquetdf.show()

  }
}
