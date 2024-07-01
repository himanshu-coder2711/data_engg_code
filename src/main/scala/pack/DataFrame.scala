package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataFrame {
  case class columns (id:String,category:String,product:String,spendby:String)

  def main(args: Array[String]): Unit = {

    println("===Reading DrData===")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.driver.host","localhost")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    val data = sc.textFile("file:////Users/himanshusingh/Desktop/bigdata/dr.txt",1)

    println()
    println("=====Raw Data===")
    println()

    data.foreach(println)

    val mapsplit=data.map(x=>x.split(","))
    val schemardd = mapsplit.map(x=>columns(x(0),x(1),x(2),x(3)))
    val colfilter=schemardd.filter(x=>x.product.contains("Gymnastics"))

    println()
    println("===col filter===")
    println()

    colfilter.foreach(println)

    val df = colfilter.toDF()

    println()
    println("==========DF=========")
    println()
    df.show()

    //df.write.parquet("")

    df.createOrReplaceTempView("iplfinal")

    val prodf = spark.sql("select id,product from iplfinal")
    prodf.show()

    println("====All Work Done====")




  }
}