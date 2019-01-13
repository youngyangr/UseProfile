import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._

case class Professions(uid : String, profession: String)

object Profession {
  //key words
  val teacher="教育,大学,中学,小学,职称,班主任,义务教育,教师,老师"
  val student="考试,期末,成绩,高考,排名,班级,同学,同桌,孩子,上学,放学,学生,年级"
  val doctor="门诊,患者,处方,医生,医院,医学,健康,儿科,内科,外科,医师,专家"
  val commerce="商家,推广,代理,货源,加盟,电商,微商"

  val teacherterm = (teacher).split(",")
  val studentterm = (student).split(",")
  val doctorterm = (doctor).split(",")
  val commerceterm = (commerce).split(",")

  def containsTeacher(input : String)={
    teacherterm.map(input.contains(_)).reduce(_||_)
  }
  def containsStudent(input : String)={
    studentterm.map(input.contains(_)).reduce(_||_)
  }
  def containsDoctor(input : String)={
    doctorterm.map(input.contains(_)).reduce(_||_)
  }
  def containsCommerce(input : String)={
    commerceterm.map(input.contains(_)).reduce(_||_)
  }

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

    var pro = sc.textFile("file:///home/wangyang/Documents/userprofile.txt")
      .map(_.split("\t")).filter(_.length>12).filter(a=>containsTeacher(a(12)))
      .map(a=>(Professions(a(0),"Teacher"))).toDF

    pro = sc.textFile("file:///home/wangyang/Documents/userprofile.txt")
      .map(_.split("\t")).filter(_.length>12).filter(a=>containsStudent(a(12)))
      .map(a=>(Professions(a(0),"Student"))).toDF

    pro = sc.textFile("file:///home/wangyang/Documents/userprofile.txt")
      .map(_.split("\t")).filter(_.length>12).filter(a=>containsDoctor(a(12)))
      .map(a=>(Professions(a(0),"Doctor"))).toDF

    pro = sc.textFile("file:///home/wangyang/Documents/userprofile.txt")
      .map(_.split("\t")).filter(_.length>12).filter(a=>containsCommerce(a(12)))
      .map(a=>(Professions(a(0),"Commerce"))).toDF

    val resDF = up.join(pro,"uid")
    val prop = new java.util.Properties
    prop.setProperty("serverTimezone", "UTC")
    prop.setProperty("user","root")
    prop.setProperty("password","wy")
    //resDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/spark", "spark.profession", prop)
    resDF.show
  }

}
