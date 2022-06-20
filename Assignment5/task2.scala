import java.io.{File, PrintWriter}
import scala.util.control.Breaks._

object task2 {
  val m = 997
  def main(args: Array[String]): Unit= {
    /*
    val input_file_path = "/Users/gopi/Desktop/Assignment5/users.txt"
    val outputFilePath = "/Users/gopi/Desktop/Assignment5/output_1.csv"
    val streamSize = 300
    val numOfAsks = 50
    */

    val input_file_path = args(0)
    val outputFilePath = args(3)
    val streamSize = args(1).toInt
    val numOfAsks = args(2).toInt

    val bx = new Blackbox()

    var pw = new PrintWriter(new File(outputFilePath))
    pw.write("Time,Ground Truth,Estimation")
    pw.write("\n")
    var gtSum = 0.0
    var pdSum = 0.0
    //println(getZeroCount("10010100"))
    //println("QPT4Ud4H5sJVr68yXhoWFw".hashCode)


    for(i <- 0 until(numOfAsks)) {
      var streamUsers = bx.ask(input_file_path, streamSize)
      var prediction = flajoletMartin(streamUsers)
      gtSum += prediction(0)
      pdSum += prediction(1)
      var string = i.toString + "," + prediction(0).toInt.toString + "," + prediction(1).toInt + "\n"
      pw.write(string)
    }
    pw.flush()
    //println(gtSum)
    //println(pdSum)
    println(pdSum / gtSum)
    pw.close()
  }

  def flajoletMartin(usersStream : Array[String]) : Array[Double] = {
    var userSet = Set[String]()
    var hashValuesAll = Array[Array[String]]()
    var predictions = Array[Double]()
    var groupAvgs = Array[Double]()

    for(user <- usersStream) {
      var hvs = myhashs(user)
      var binaryHvs = Array[String]()
      for(hv <- hvs) {
        binaryHvs :+= hv.toBinaryString
      }
      hashValuesAll :+= binaryHvs
      userSet += user
    }

    for(i <- 0 until(55)) {
      var maxValue = 0
      for(j <- 0 until(usersStream.size)) {
        var zeroCount = getZeroCount(hashValuesAll(j)(i))
        if(zeroCount > maxValue) {
          maxValue = zeroCount
        }
      }
      predictions :+= scala.math.pow(2, maxValue)
    }

    for(i <- 0 until(11)) {
      var sum = 0.0
      for(j<- i*5 until( i*5 + 5)){
        sum += predictions(j)
      }
      groupAvgs :+= sum/5.0
    }
    groupAvgs = groupAvgs.sortBy(a => a)
    var result = Array[Double]()
    result :+= userSet.size.toDouble
    result :+= groupAvgs(6)
    return result
  }

  def myhashs(user : String) : Array[Int] = {
    var userId = user.hashCode
    if(userId < 0) {
      userId = 0 - userId
    }
    var result = Array[Int]()
    var a = 37
    var b = 13
    for(i <- 0 until(55)) {
      var hashValue = ((a * userId * i) + (b)) % m
      result :+= hashValue
    }
    return result
  }

  def getZeroCount(binary : String) : Int={
    var count = 0
    breakable{
      for(i<- binary.size -1 to(0) by -1) {
        if(binary(i) == '0') {
          count += 1
        } else {
          break
        }
      }
    }

    return count
  }
}
