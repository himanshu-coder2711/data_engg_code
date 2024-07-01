package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object avroread {

  def main(args:Array[String]):Unit={

    println("====started====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")


    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val avrodf = spark
      .read
      .format("avro")
      .load("file:///Users/himanshusingh/Desktop/bigdata/part.avro")

    avrodf.show()


  }

}
