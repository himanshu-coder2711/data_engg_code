package pack

object obj {

  def main(args: Array[String]): Unit = {
    println("====Started====")
    println()

    val lisin = List(1,2,3,4,5)
    println("===Raw List===")
    println(lisin)
    println()

    val mullis = lisin.map(x=> x*10)
    println("====Mul List===")
      println(mullis)
    println()

    val flis = lisin.filter(x=>x>2)
    println()
      println("====fil list===")
      println()
      println(flis)

    println()
    println("===less than 2===")
    val fliss=lisin.filter(x=>x<2)
    println(fliss)

    val lp = List(1,2,3,4)
    println("===Raw List===")
    println(lp)
    println()
    val rl=lp.reverse
    println("=======Reversed List=========")
    println(rl)


  }

}
