import scala.io.Source

class Blackbox {
  private val r1 = scala.util.Random
  r1.setSeed(553)
  def ask(filename: String, num: Int) : Array[String]= {
    val inputFilePath = filename
    val lines = Source.fromFile(inputFilePath).getLines().toArray
    var stream = new Array[String](num)
    for(i<-0 to(num-1)) {
      stream(i) = lines(r1.nextInt(lines.length))
    }
    return stream
  }
}
