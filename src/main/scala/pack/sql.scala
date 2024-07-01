package pack

import org.apache.spark.SparkContext  // rdd
import org.apache.spark.sql.SparkSession  // dataframe
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.catalyst.expressions.Upper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.io.Source

object sql {

  def main(args:Array[String]):Unit={
      println("===Hello====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()

    import spark.implicits._


    val data = Seq(
      (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
      (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
      (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
      (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
      (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
      (5, "02-14-2011", 200.0, "Gymnastics", null, "cash"),
      (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
      (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
      (8, "02-14-2011", 200.0, "Gymnastics", null, "cash")
    )

    val df = data.toDF("id","tdate","amount","category","product","spendby")


    val data2 = Seq(
      (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
      (5, "02-14-2011", 200.0, "Gymnastics", null, "cash"),
      (6, "02-14-2011", 200.0, "Winter", null, "cash"),
      (7, "02-14-2011", 200.0, "Winter", null, "cash")
    )

    val df1 = data2.toDF("id","tdate","amount","category","product","spendby")

    val data4 = Seq(
      (1, "raj"),
      (2, "ravi"),
      (3, "sai"),
      (5, "rani")
    )

    val cust = data4.toDF("id","name")

    val data3 = Seq(
      (1, "mouse"),
      (3, "mobile"),
      (7, "laptop")
    )

    val prod = data3.toDF("id","product")

    df.show()
    df1.show()
    cust.show()
    prod.show()

    df.createOrReplaceTempView("df")
    df1.createOrReplaceTempView("df1")
    cust.createOrReplaceTempView("cust")
    prod.createOrReplaceTempView("prod")

    println
    println("====== WORK AREA BELOW=====")
    println

spark.sql("select id,tdate from df").show()

    //filter the dataframe where category=exercise
spark.sql("select *from df where category='Exercise'").show()

    //filter the dataframe where category=exercise and spendby=cash but only from column id, tdate, category and spendby
spark.sql("select id,tdate,category,spendby from df where category='Exercise' and spendby='cash'").show()

    //filter category = exercise and gymnastics both
    spark.sql("select *from df where category in ('Exercise','Gymnastics')").show()
    //filter dataframe where product contains gymnastics
 spark.sql("select *from df where product like('%Gymnastics%')").show()

    //filter dataframe where category not equals to exercise
 spark.sql("select *from df where category !='Exercise'").show()

    //category not equals to exercise and gymnastics
    spark.sql("select *from df where category not in ('Exercise','Gymnastics')").show()

    //filter data where product is null
    spark.sql("select *from df where product is null").show()

    //filter where product is not null
    spark.sql("select *from df where product is not null").show()

    //filter max of id
    spark.sql("select max(id) from df").show()

    //rename max(id)

    spark.sql("select max(id) as idmax from df").show()

    // filter of rows count
    spark.sql("select count(1) from df").show()
    //1 will place one-one for all the rows and count it

    //add extra column as status and assign cash as 1 and credit as 0
    //conditional statement
    spark.sql("select *, case when spendby='cash' then 1 else 0 end as status from df").show()

    //Concat Two Columns as condata with 0-exercise,1-gymnastics
    spark.sql("select id,category,concat(id,'-',category) as condata from df").show()

    //concat multiple column
    spark.sql("select id,category,product,concat(id,'-',category,'-',product) as multicondata from df").show()

    //concat many multi concatination
    spark.sql("select id,category,product, concat_ws('-',id,category,product)as concat from df").show()

    //lowercase the data
    spark.sql("select category,lower(category) as lower from df").show()

    //uppercase the data
    spark.sql("select category,upper(category) as upper from df").show()

    //Ceil operation ceil refers to rounding value to upper side
    spark.sql("select amount,ceil(amount) as ceil from df").show()

    // round of the value, it will round up to nearest integer
    spark.sql("select amount,round(amount) as round from df").show()

    //Replace Null value
    spark.sql("select product,coalesce(product,'NA') as nullrep from df").show()

    //Trim the spaces from front and back of string in a row
    spark.sql("select trim(product) from df").show()

    //distinct - it finds the unique value
    spark.sql("select distinct category from df").show()
    spark.sql("select distinct category,product from df").show()

    //filter start from 1st to 10 character from string
    spark.sql("select substring(product,1,10) as substring from df").show()

    //split with substring
    spark.sql("select product,split(product,' ')[0] as splitted from df").show()

    //union all----> it combines the dataframe into one.
    spark.sql("select * from df union all select * from df1").show()

    //union---> eleminate duplicates from union all
    spark.sql("select *from df union select *from df1").show()

    //find aggregate sum amount of each category
    spark.sql("select category,sum(amount) as sum from df group by category").show()

    //aggregate sum of two columns- total amount spent on category and spendby
    spark.sql("select category,spendby,sum(amount) as sum from df group by category,spendby").show()

    //count of group sum aggregate
    spark.sql("select category,spendby,sum(amount) as sum,count(amount) as cnt from df group by category,spendby").show()

    //find max amount of each category
    spark.sql("select category,max(amount) as max from df group by category").show()

    //order by-bydefault the order is ascending
    spark.sql("select category,max(amount) as max from df group by category order by category").show()

    //order by in descending
    spark.sql("select category,max(amount) as max from df group by category order by category desc").show()

    //Window operations
    //window- it categorises the column
    spark.sql("select category,amount,row_number() over (partition by category order by amount desc) as row_number from df").show()

    //give same rank to the common values
    spark.sql("select category,amount, dense_rank() over(partition by category order by amount desc) as dense_rank from df").show()

    //rank- it gives the row rank as highest rank, eg- if we have two same values and 1 different then it gives ranking as 1,1,3
    spark.sql("select category,amount,rank() over(partition by category order by amount desc) as rank from df").show()

    //Lead-->
    spark.sql("select category,amount,lead(amount) over(partition by category order by amount desc) as lead from df").show()

    //lag--it allows us to access data from preceeding row within the same result set,
    //it is useful for comparing values b/w consecutive rows in ordered dataset
    spark.sql("select category,amount,lag(amount) over (partition by category order by amount desc) as lag from df").show()

    //having--> it is used to count the repetition of duplicate values/rows
    spark.sql("select category,count(category) as cnt from df group by category having count(category)>1").show()

    //joins---inner join--> it combines rows from two tables based on a related column,
    // returning only the rows with matching values in both tables
    spark.sql("select a.*,b.product from cust a join prod b on a.id=b.id").show()

    //left join-->it returns all rows from the left table and the matching rows from the right table,
    // with non-matching rows in the right table resulting in 'NULL' values
    spark.sql("select a.*,b.product from cust a left join prod b on a.id=b.id").show()

    //right join-->> it returns all rows from the right table and the matching rows from the left table,
    //with non-matching rows in the left table resulting in 'NULL' values
    spark.sql("select a.*,b.product from cust a right join prod b on a.id=b.id").show()

    //full join--> It returns all rows when there is a match in either the left or right table,
    // with non-matching rows from both tables resulting in NULL values where there is no match.
    spark.sql("select a.*,b.product from cust a full join prod b on a.id=b.id").show()

    //left anti join-->> returns ids from left which is not having in the right table
    spark.sql("select a.* from cust a left anti join prod b on a.id=b.id").show()

    //date format
    spark.sql("select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-YYYY'),'YYYY-MM-dd') as con_date from df").show()

    //sub query--->>it is a query nested within another query, allowing for the retrieval of data based on the results of another query.
    spark.sql(""" select sum(amount) as total,con_date from(select id,tdate,from_unixtime(unix_timestamp(tdate,'MM-dd-YYYY'),'YYYY-MM-dd') as con_date,amount,category,product,spendby from df) group by con_date""").show()
  }

}
