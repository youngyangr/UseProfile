import java.util.Properties

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._

case class Interests(uid : String, insterest : String)

object Interest{

  def main(argv:Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    //    val lines = ssc.socketTextStream("localhost", 4700)
    val sc = ssc.sparkContext
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._

    val up = sc.textFile("file:///home/wangyang/Documents/userprofile.txt")
      .map(_.split("\t")).filter(_.length>12)
      .map(a=>UserProfile(a(0),a(1),a(2),a(6),a(7),a(12))).toDF

    var interest = sc.textFile("file:///home/wangyang/Documents/interest.txt")
      .map(_.split("\t")).filter(_.length>1)
      .map(a=>((a(0),a(1)),1)).filter(_._1._2 != "unknown").filter(_._1._2 != "微博").reduceByKey(_+_).filter(_._2>5)
      .map(a=> Interests(a._1._1,a._1._2)).toDF

    val resDF = up.join(interest,"uid")
    resDF.show

    val prop = new Properties()
    prop.setProperty("serverTimezone", "UTC")
    prop.put("user", "root")
    prop.put("password", "wy")

    //resDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/spark", "spark.interest", prop)
  }
}