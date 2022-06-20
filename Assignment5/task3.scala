import java.io.{File, PrintWriter}

object task3 {
  var n = 0
  var mainList = new Array[String](100)
  private val r2 = scala.util.Random
  r2.setSeed(553)
  var count = 0


  def main(args: Array[String]): Unit={
    /*val input_file_path = "/Users/gopi/Desktop/Assignment5/users.txt"
    val outputFilePath = "/Users/gopi/Desktop/Assignment5/output_1.csv"
    val stream_size = 100
    val num_of_asks = 30
*/
    val input_file_path = args(0)
    val outputFilePath = args(3)
    val stream_size = args(1).toInt
    val num_of_asks = args(2).toInt
    val bx = new Blackbox()

    var pw = new PrintWriter(new File(outputFilePath))
    pw.write("seqnum,0_id,20_id,40_id,60_id,80_id")
    pw.write("\n")

    for(i <-  0 until(num_of_asks)) {
      val stream_users = bx.ask(input_file_path, stream_size)
      var string = reservoirSampling(stream_users, stream_size)
      string = n.toString + "," + string + "\n"
      pw.write(string)
    }
    pw.flush()

  }

  def reservoirSampling(streamUsers: Array[String], streamSize : Int) : String={
    //println(mainList.size)
    for(user <- streamUsers) {
      n = n + 1
      if(count < streamSize) {
        mainList.update(count,user)
        count = count + 1
        //println(count)
      } else {
        var randomNumber = r2.nextFloat()
        //println(randomNumber)
        if (randomNumber <= (streamSize/n.floatValue())) {
          val index = r2.nextInt(100)
          mainList.update(index, user)
        }
      }
    }
    return mainList(0) + "," + mainList(20) + "," + mainList(40) + "," + mainList(60) + "," + mainList(80)
  }
}
