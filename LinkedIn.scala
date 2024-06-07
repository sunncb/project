


import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LinkedIn  extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "Shopping")

  val input = sc.textFile("C:/Users/Sunny/Desktop/BIG_data/spark-wk-9/friendsdata.csv")

  // val header = List("customer_id","product_Id","Amount")

  val getInfo = input.map(x => (x.split("::")(2),(x.split("::")(3).toInt,1)))

  val summing = getInfo.reduceByKey((x, y) => (x._1 + y._1,x._2+y._2))
  
  val output = summing.map(x=> (x._1,(x._2._1/x._2._2))).sortBy(_._2)
  
  //val sorting = summing.sortBy(x => x._2, false)

  output.collect.foreach(println)

}
  
