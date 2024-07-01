package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object UsData {

  def main(args: Array[String]): Unit = {

    println("===Reading UsData===")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.driver.host","localhost")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")


    val data = sc.textFile("file:///Users/himanshusingh/Desktop/usdata.csv",1)

    println
    println("===== raw data print=====")
    println


    data.take(10).foreach(println)
  }

}
