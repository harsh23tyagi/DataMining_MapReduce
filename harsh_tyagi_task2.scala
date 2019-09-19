import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD._
import org.apache.log4j.{Level, Logger}
import java.io.{FileWriter, Writer, File}

object harsh_tyagi_task2 {
  def main(args: Array[String]) {
    var input1 = "review.json"
    var input2 = "business.json"
    var output1 = "data_task2_a.txt"
    var output2 = "data_tasask2_b.json"

    try{
      input1 = args(0)
      input2 = args(1)
      output1 = args(2)
      output2 = args(3)
    }catch {
      case e: Exception => e.printStackTrace
    }
    println("Initializing...")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[2]")
    val sc = new SparkContext(conf)
            val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    sc.setLogLevel("ERROR")
            val items1 = spark.read.json(input1).rdd

            val items2 = spark.read.json(input2).rdd

  println("Beginning Task A...")
          //taska
          val mapA = items2.map(x => {
            (x.getAs[String]("business_id"),x.getAs[String]("state"))
          })
          val mapping= mapA.collect().toMap

          val mapB = items1.map(x => {
            (mapping.get(x.getAs[String]("business_id")),x.getAs[Double]("stars"))
          }).reduceByKey(_+_).collectAsMap()

          //val sumMap = mapB.collect().toMap

          val mapBCount = items1.map(x => {
            (mapping.get(x.getAs[String]("business_id")),1)
          }).reduceByKey(_+_)

      println("Beginning Task B...")

          val divide = mapBCount.map(x=>{
            val sum = mapB(x._1)
            (x._1, (sum/x._2))
          })

    val sorted = divide.sortBy( x => (-x._2, x._1))




  // ----------reusable code-----------------


  val start1 = System.currentTimeMillis()
  val collectSorted = sorted.collect()
  println(collectSorted.take(5).toList)
  val end1 = System.currentTimeMillis()
  val m1 = end1 - start1

  val start2 = System.currentTimeMillis()
  println(sorted.take(5).toList)

  val end2 = System.currentTimeMillis()
  val m2 = end2-start2

  //    //    println("m1:"+m1)
  //    //    println("m2:"+m2)




  val f = new File(output1);
  f.createNewFile();
  val w: Writer = new FileWriter(f)
  w.write("state,stars \n")
  for (n <- collectSorted) {
    // imagine this requires several lines

    w.write(n._1.get+","+n._2+"\n")
  }
  w.close()


  val f2 = new File(output2);
  f2.createNewFile();
  val w2: Writer = new FileWriter(f2)

  // val w2: Writer = new PrintWriter(new File(output2))
  w2.write("{ \n \"m1\":"+m1+", \n \"m2\":"+m2+", \n \"explanation\" : \"The first function collects the entire RDD and transforms into list which is a huge chunk of data. The second method reads the top 5 rows of the RDD and then convert the five rows as a list of tuples. Hence the first method takes more time than the second method. The time in milliseconds\" \n }")
  w2.close()
  // second task


  //collecting first

  //println(sorted.take(20).toList)
  println("Completed")


}
}
