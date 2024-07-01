package pack

object FlattenFilterContains {
  def main(args: Array[String]): Unit = {

    val citylist = List("State-> Telangana,City->Hyderabad",
                        "State->Gujrat,City->GandhiNagar",
                        "State->UttarPradesh,City->Lucknow")
    println()
    println("===Raw List===")
    citylist.foreach(println)

    val flatdata =citylist.flatMap(x=>x.split(","))
    println()
    println("===flatdata list===")
    println()
    flatdata.foreach(println)

    val cityfilter=flatdata.filter(x=>x.contains("City"))
    println()
    println("===City Filter Data===")
    println()
    cityfilter.foreach(println)

    val repcity=cityfilter.map(x=>x.replace("City->",""))
    println()
    println("===replaced City===")
    println()
    repcity.foreach(println)


  }

}
