package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object joins {
  def main(args:Array[String]):Unit={
    println("====started====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")


    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._



    val leftdata = Seq(
      ("1","raj"),
      ("2","ravi"),
      ("3","sai"),
      ("5","rani")
    )


    val leftdf = leftdata.toDF("id", "name")


    leftdf.show()

    //	leftdf.printSchema()


    val rightdata = Seq(
      ("1","mouse"),
      ("3","mobile"),
      ("7","laptop")
    )


    val rightdf = rightdata.toDF("id", "product")



    rightdf.show()

    val rightdf1 = rightdata.toDF("id", "product").withColumnRenamed("id","cid")



    rightdf1.show()
    println()
    println("INNER JOIN")
    println()

    val innerjoin=leftdf.join(rightdf,Seq("id"),"inner")
    innerjoin.show()

    println()
    println("LEFT JOIN")
    println()

    val leftjoin=leftdf.join(rightdf,Seq("id"),"left")
    leftjoin.show()

    println()
    println("RIGHT JOIN")
    println()

    val rightjoin=leftdf.join(rightdf,Seq("id"),"right")
    rightjoin.show()

    println()
    println("FULL JOIN")
    println()

    val fulljoin=leftdf.join(rightdf,Seq("id"),"full")
    fulljoin.show()

    println()
    println("INNER JOIN WITH MISMATCH COLUMN")
    println()

    val innerjoin1=leftdf.join(rightdf1,leftdf("id")===rightdf1("cid"),"inner").drop("cid")
    innerjoin1.show()

    println()
    println("left anti join")
    println()

    val leftantijoin=leftdf.join(rightdf,Seq("id"),"left_anti")
    leftantijoin.show()

    println()
    println("left anti joinwhen we sellect left table and remove from right table")
    println()

    val leftantijoin1=rightdf.join(leftdf,Seq("id"),"left_anti")
    leftantijoin1.show()

    println()
    println("Cross Join")
    println()

    val crossjoin=leftdf.crossJoin(rightdf)
    crossjoin.show()

  }
}
