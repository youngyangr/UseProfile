import java.util.Properties

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.LabelPropagation

case class CommunityProfile(community : String, communityName: String)
case class PageRankRes(uid : String, pr : Double);
case class LabelProRes(uid : String, community : String);

object NetWork{
  def main(argv:Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    //    val lines = ssc.socketTextStream("localhost", 4700)
    val sc = ssc.sparkContext
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val graph = GraphLoader.edgeListFile(sc, "file:///home/wangyang/Documents/relation.txt")
    val ranks = graph.pageRank(0.01).vertices
    val ranksDF = ranks.map(a=>PageRankRes(a._1.toString, a._2)).toDF
    val labpDF = LabelPropagation.run(graph, 3).vertices.map(a=>LabelProRes(a._1.toString(), a._2.toString)).toDF
    val midDF = ranksDF.join(labpDF, "uid")

    val up = sc.textFile("file:///home/wangyang/Documents/userprofile.txt")
      .map(_.split("\t")).filter(_.length>12)
      .map(a=>CommunityProfile(a(0),a(1))).toDF
    val resDF = up.join(midDF,"community")

    resDF.show

    val prop = new Properties()
    prop.setProperty("serverTimezone", "UTC")
    prop.put("user", "root")
    prop.put("password", "wy")

    //resDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/spark", "spark.network", prop)

  }
}