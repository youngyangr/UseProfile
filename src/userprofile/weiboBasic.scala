import java.util.Properties

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._

case class UserProfile(uid : String, nickname: String, region : String, sex : String, age : String, descp : String)
case class GuanZhu(uid : String, gz : Int)
case class FenSi(uid:String,fs:Int)
case class WeiboN(uid:String, wbn:Int)

object WeiboBasic{
  def main(argv:Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    //    val lines = ssc.socketTextStream("localhost", 4700)
    val sc = ssc.sparkContext
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val up = sc.textFile("file:///home/wangyang/Documents/userprofile.txt").map(_.split("\t")).filter(_.length>12).map(a=>UserProfile(a(0),a(1),a(2),a(6),a(7),a(12))).toDF
    val gz = sc.textFile("file:///home/wangyang/Documents/relation.txt").map(_.split("\t")).map(a=>(a(0),1)).reduceByKey(_+_).map(a=>GuanZhu(a._1,a._2)).toDF()
    val up1 = up.join(gz, "uid")
    val fs = sc.textFile("file:///home/wangyang/Documents/relation.txt").map(_.split("\t")).map(a=>(a(1),1)).reduceByKey(_+_).map(a=>FenSi(a._1,a._2)).toDF()
    val up2 = up1.join(fs, "uid")
    val wbn = sc.textFile("file:///home/wangyang/Documents/weibo.txt").map(_.split("\t")).filter(_.length>5).map(a=>(a(1),1)).reduceByKey(_+_).map(a=>WeiboN(a._1,a._2)).toDF()
    val up3 = up2.join(wbn, "uid")
    up3.show

    val prop = new Properties()
    prop.setProperty("serverTimezone", "UTC")
    prop.put("user", "root")
    prop.put("password", "wy")

    //up3.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/spark", "spark.basic", prop)
  }
}