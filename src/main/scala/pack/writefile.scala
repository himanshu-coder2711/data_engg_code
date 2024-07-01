package pack

import org.apache.spark._
import org.apache.spark.sql._

object writefile {

  def main(args:Array[String]):Unit={
    println("====ITS started====")


    //System.setProperty("hadoop.home.dir","D:\\hadoop")   // Change path accordingly


    val conf = new SparkConf().setAppName("s3read").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark  = SparkSession.builder.getOrCreate()
    import spark.implicits._


    val data = Seq(
      ("DEPT3", 500),
      ("DEPT3", 200 ),
      ("DEPT1", 1000 ),
      ("DEPT1", 700 ),
      ("DEPT1", 700 ),
      ("DEPT1", 500 ),
      ("DEPT2", 400 ),
      ("DEPT2", 200)
    )


    val df: DataFrame = data.toDF("department", "salary")


    df.show()



    val  procedf = df.filter("salary > 700")


    procedf.show()



    procedf
      .write
      .format("csv")
      .mode("overwrite")
      .save("file:////Users/himanshusingh/Desktop/bigdata")

  }

}
