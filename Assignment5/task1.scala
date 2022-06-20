import java.io.{File, PrintWriter}

object task1 {
  val m = 69997
  var previousSet = Set[String]()
  var filterBitArray = Array[Int]()
  private val r2 = scala.util.Random
  for(i<- 0 until(m)) {
    filterBitArray :+= 0
  }
  var randomValues = Array[Array[Int]]()
  for(i <- 0 until 50) {
    var a1 = Array[Int]()
    a1 :+= r2.nextInt(101)
    a1 :+= r2.nextInt(101)
    randomValues :+= a1
  }

  def main(args: Array[String]): Unit= {
    /*
    val input_file_path = "/Users/gopi/Desktop/Assignment5/users.txt"
    val outputFilePath = "/Users/gopi/Desktop/Assignment5/output_1.csv"
    val streamSize = 100
    val numOfAsks = 50*/

    val input_file_path = args(0)
    val outputFilePath = args(3)
    val streamSize = args(1).toInt
    val numOfAsks = args(2).toInt

    val bx = new Blackbox()

    var pw = new PrintWriter(new File(outputFilePath))
    pw.write("Time,FPR")
    pw.write("\n")

    for(i <- 0 until(numOfAsks)) {
      var streamUsers = bx.ask(input_file_path, streamSize)
      var prediction = bloomFilter(streamUsers)
      var string = i.toString + "," + prediction.toString + "," + "\n"
      //println(streamUsers.size)
      pw.write(string)
    }
    pw.flush()
    pw.close()
  }

  def myhashs(user : String) : Array[Int] = {
    var userId = user.hashCode / 1000
    if(userId < 0) {
      userId = 0 - userId
      //println(userId)
    }
    var result = Array[Int]()
    var a = 3
    var b = 7
    for(i <- 0 until(50)) {
      var hashValue = ((a * userId * randomValues(i)(0)) + (b * randomValues(i)(1))) % m
      result :+= hashValue
    }
    return result
  }

  def bloomFilter(streamUsers: Array[String]) : Double ={
    var falsePositive = 0
    var trueNegative = 0
    for(user<-streamUsers) {
      var hashValues = myhashs(user)
      var count = 0
      var zeroPresent = false
      for(hashValue <- hashValues) {
        if(filterBitArray(hashValue) != 1) {
          zeroPresent = true
          filterBitArray.update(hashValue, 1)
        }
      }
      if(!previousSet.contains(user)) {
        if(zeroPresent) {
          trueNegative += 1
        } else{
          falsePositive += 1
        }
      }

      previousSet += user
    }
    return falsePositive.toDouble / (falsePositive.toDouble + trueNegative.toDouble)
  }
}
