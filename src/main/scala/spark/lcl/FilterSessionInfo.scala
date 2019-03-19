package spark.lcl

import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.util.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FilterSessionInfo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
    val sc = new SparkContext(conf)

    val sessionInfoRdd: RDD[(String, String)] = sc.textFile("f:/bigdata/session/out2").map(line=>{
      val str = line.split("\001")
      val id = str(0)
      val info = str(1)
      (id, info)
    })

    //过滤器过滤
    val test1 = "age:7-13"
    val test2 = "sex:male"
    val test3 = "professional:professional0"
    val test4 = "city:city83"
    filterConditions(sessionInfoRdd, test1,test2, test3, test4).take(10).foreach(println)


  }

  def filterConditions(sessionId2AggregateInfoRDD: RDD[(String, String)], conditions:String*): RDD[(String, String)]={
    var res = sessionId2AggregateInfoRDD
    for(condition <- conditions){
      val con = condition.split(":")
      val key = con(0)
      key match {
        case "age" =>{
          val value1 = con(1).split("-")(0).toInt
          val value2 = con(1).split("-")(1).toInt
          res = res.map(row=>{
            val str = row._2
            val condition = StringUtils.getFieldFromConcatString(str, "\\|", key)
            (row, condition)
          }).filter(t=>(t._2.toInt>=value1 && t._2.toInt<=value2)).map(_._1)
        }
        case "sex" | "city" | "professional" =>{
          val value = con(1)
          res = res.map(row=>{
            val str = row._2
            val condition = StringUtils.getFieldFromConcatString(str, "\\|", key)
            (row, condition)
          }).filter(_._2.equals(value)).map(_._1)
          res
        }
      }
    }
    res
  }

}
