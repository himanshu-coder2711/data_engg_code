package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object createRead {

  def main(args:Array[String]):Unit={

    println("====started====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      .set("spark.driver.host","localhost").set("spark.driver.allowMultipleContexts", "true")


    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._


    val data = Seq(
      ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
      ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
      ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
      ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
      ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
      ("00000005", "02-14-2011", 200, "Gymnastics", null, "cash")
    )


    val df = data.toDF("id", "tdate", "amount", "category", "product", "spendby")


    df.show()

    val  procdf = df.filter("category='Gymnastics' and spendby='cash'")

    procdf.show()

    println()
    println(" multi column filter ")
    val  mulcol = df.filter("category='Gymnastics' or spendby='cash'")
    mulcol.show()

    println()
    println(" multi value filter ")
    println("="*50)
    val  mulval = df.filter("category='Gymnastics' or category='Exercise'")
    mulval.show()

    println()
    println("Multi value by infilter")
    println("="*50)

    val infilter=df.filter("category in ('Gymnastics','Exercise')")
    infilter.show()

    println("="*10,"Like Filter","="*10)
    println("="*50)
    val likefilter=df.filter("product like '%Gymnastics%'")
    likefilter.show()

    println("="*10,"Null Filter","="*10)
    println("="*50)
    val nullfilter=df.filter("product is null")
    nullfilter.show()

    println("="*10,"Not Null Filter","="*10)
    println("="*50)
    val notnullfilter=df.filter("product is not null")
    notnullfilter.show()

    println("="*10,"Not equals Filter","="*10)
    println("="*50)
    val notequals=df.filter("category !='Gymnastics'")
    notequals.show()

    println("="*10,"Not notLike Filter","="*10)
    println("="*50)
    val notlikefilter=df.filter("product not like '%Gymnastics%'")
    notlikefilter.show()

    println()
    println("Multi value by notinfilter")
    println("="*50)

    val notinfilter=df.filter("category not in ('Gymnastics','Exercise')")
    notinfilter.show()



  }
}


