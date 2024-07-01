package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object salscenario {
  def main(args:Array[String]):Unit= {

    println("====started====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.host", "localhost").set("spark.driver.allowMultipleContexts", "true")


    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("DEPT3", 500),
      ("DEPT3", 200),
      ("DEPT1", 1000),
      ("DEPT1", 700),
      ("DEPT1", 700),
      ("DEPT1", 500),
      ("DEPT2", 400),
      ("DEPT2", 200)
    )


    val df: DataFrame = data.toDF("department", "salary")


    df.show()

    val departwindow = Window.partitionBy("department").orderBy(col("salary")desc)
    val denserank = df.withColumn("dense_rank", dense_rank() over departwindow)
    denserank.show()

    //way2
    val denserank1=df.withColumn("dense_rank",expr(""" dense_rank() over(partition by department order by salary desc)"""))
    denserank1.show()

    //final filter
    val finaldf= denserank.filter("dense_rank=2").drop("dense_rank")
    finaldf.show()
  }

}
