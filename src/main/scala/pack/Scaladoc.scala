package pack
import java.util.Date
object Scaladoc {
  val lst=List(1,2,3);
  val map=Map(1->"tom",2->"max",3->"john");
  val opt:Option[Int]=Some(55);

def main(args: Array[String]): Unit = {
  println(lst.find(_ > 2).getOrElse(0));
  println(map.get(5).getOrElse("No name found"));

  println(opt.isEmpty);

 }
}
