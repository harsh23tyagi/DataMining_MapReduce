import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD._
import org.apache.log4j.{Level, Logger}
import java.io.{FileWriter, Writer, File}
object harsh_tyagi_task1 {

  def main(args: Array[String]) {
    var input1 = "review.json"
    var output1 = "data_task2_a.txt"

    try{
      input1 = args(0)

      output1 = args(1)

    }catch {

      case e: Exception => e.printStackTrace
        input1 = "review.json"
    }


    println("Initializing...")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //---------old code------
        val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
        val items = spark.read.json(input1).rdd


    //--------------------- Old Code--------------------------------





        println("Beginning Task A...")
        // taskA
        val mapA = items.map(x => {
          if(x.getAs[Long]("useful") > 0){
            (1, 1)
          }
          else{
            (0, 1)
          }
        }).reduceByKey(_+_)



        val taskA = mapA.collectAsMap().get(1).get



        //Task B
        println("Beginning Task B...")

        val mapB = items.map(x => {
          if(x.getAs[Double]("stars") == 5){
            (1, 1)
          }
          else{
            (0, 1)
          }
        }).reduceByKey(_+_)

        val taskB = mapB.collectAsMap().get(1).get


        //taskC
        println("Beginning Task C...")
        val mapC = items.map(x => {
          (x.getAs[String]("text").length() , x.getAs[String]("text"))
        })

        val taskC = mapC.max()._1


        //taskD
        println("Beginning Task D...")
        val mapD = items.map(x => {
          (x.getAs[String]("user_id") , 1)
        }).reduceByKey(_+_)

        val taskD = mapD.count()
        //

        //taskE
        println("Beginning Task E...")

        val mapE = items.map(x => {
          (x.getAs[String]("user_id") , 1)
        }).reduceByKey(_+_)
        val sorted = mapE.sortBy(x => (-x._2, x._1)).take(20)
        // line => (line._2, line._1, false, true)
        val taskE = (sorted.toList)

        //taskF
        println("Beginning Task F...")
        val mapF = items.map(x => {
          (x.getAs[String]("business_id") , 1)
        }).reduceByKey(_+_)

        val taskF = mapF.count()


        //taskG
        println("Beginning Task G...")
        val mapG = items.map(x => {
          (x.getAs[String]("business_id") , 1)
        }).reduceByKey(_+_)
        val sortedG = mapG.sortBy(x => (-x._2, x._1)).take(20)
        // line => (line._2, line._1, false, true)

        val taskG = (sortedG.toList)




        println("Writing to JSON...")


    val f = new File(output1);
    f.createNewFile();
    val w: Writer = new FileWriter(f)
    //val w: Writer = new PrintWriter(new File(output1))

    w.write("{ \n")
    w.write("\"n_review_useful\":"+taskA+",\n")
    w.write("\"n_review_5_star\":"+taskB+",\n")
    w.write("\"n_characters\":"+taskC+",\n")
    w.write("\"n_user\":"+taskD+",\n")
    w.write("\"top20_user\":[\n")
    //task E
    var c = 0
    for (n <- taskE){
      c += 1
      if(c == 20){w.write("[\""+n._1+"\","+n._2+"]\n")
        c = 0}
      else{w.write("[\""+n._1+"\","+n._2+"], \n")}

    }
    w.write("],\n")
    //w.write("\"top20_user\":"+taskE+",\n")
    w.write("\"n_business\":"+taskF+",\n")
    w.write("\"top20_business\":[\n")
    //task E

    for (n <- taskG){
      c += 1
      if(c == 20){w.write("[\""+n._1+"\","+n._2+"]\n")}
      else{w.write("[\""+n._1+"\","+n._2+"], \n")}

    }
    w.write("]\n}")
    // w.write("\"top20_business\":"+taskG+"\n}")
    w.close()

    w.close()




    println("Completed")

  }
}
