package pack

object obj1 {
  def main(args: Array[String]): Unit = {
val lis=List(1,2,3,4)
    lis.foreach(println)

    val lss=List("A~B","C~D","E~F")
    println()
    println("===Raw Tilt List===")
    println()
    lss.foreach(println)
    println()
    val flatten=lss.flatMap(x=>x.split("~"))
    println("===Flatten List Element===")
    println(flatten)
    flatten.foreach(println)
    println()
    println("===Reverse List===")
    //val rev=flatten.reverse
    //rev.foreach(println)
    flatten.reverse.foreach(println)



  }

}
