package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object readandfilter {

  def main(args: Array[String]): Unit = {

    println("===Reading UsData===")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true").set("spark.driver.host","localhost")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")


    val data = sc.textFile("file:///Users/himanshusingh/Desktop/usdata.csv",1)

    println
    println("===== raw data print=====")
    println


    data.foreach(println)

    val leng=data.filter(x=>x.length>200)

    println()
    println("length more than 200")
    println()

    leng.foreach(println)

    val flatdata=leng.flatMap(x=> x.split(","))

    println()
    println("====FlatData======")
    println()

    flatdata.foreach(println)

    val remove=flatdata.map(x=> x.replace("-",""))

    println()
    println("=====removed data=====")
    println()

    remove.foreach(println)

    val concat=remove.map(x=>x+",zeyo")

    println()
    println("=====concat===")
    println()

    concat.foreach(println)
  }

}


