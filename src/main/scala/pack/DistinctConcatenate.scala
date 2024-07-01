package pack

object DistinctConcatenate {
  def main(args: Array[String]): Unit = {
    val lss=List("hadoop","hive","sqoop","hadoop","hive")
    println()
    println("===Raw Data===")
    println()
    lss.foreach(println)

    val dlss=lss.distinct
    println()
    println("===Distinct List===")
    println()

    dlss.foreach(println)
    val upper =dlss.map(x=>x.toUpperCase())
    println()
    println("===Upper Case List===")
    println()

    upper.foreach(println)

    val finalist = upper.map(x=>"Tech->"+ x + "Trainer->Sai")
    println()
    println("===Final List===")
    println()

    finalist.foreach(println)
  }

}
