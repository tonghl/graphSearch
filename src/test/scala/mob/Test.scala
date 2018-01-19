package mob

object Test {
  def main(args: Array[String]): Unit = {
    val x = "a|b|c|a"
    println(x.replace("|a",""))
  }
  def distinctNode(input:String,id:String):String={
    return (input.split("|").toSet).addString(new StringBuilder,"|").toString()
  }
}
