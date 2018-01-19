package mob
case class Student(name:String)
object TT{
  def matchTest(x:Any):String = {
    x match {
      case Student("name") =>"one"
      case 2 =>"two"
      case Student("") => "other"
    }
  }
  def main(args: Array[String]): Unit = {
    println(matchTest(2.0))
  }
}



