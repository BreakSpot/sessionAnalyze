package spark.lcl

import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.util.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark.SessionAccumulator

object SessionAccumulate {
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
    //累加器累加停留时间和步长
    var resSum: String = getSessionSum(sc, sessionInfoRdd).value

    val items = List(Constants.TIME_PERIOD_1s_3s,
      Constants.TIME_PERIOD_4s_6s,Constants.TIME_PERIOD_7s_9s,
      Constants.TIME_PERIOD_10s_30s,Constants.TIME_PERIOD_30s_60s,
      Constants.TIME_PERIOD_1m_3m,Constants.TIME_PERIOD_3m_10m,
      Constants.TIME_PERIOD_10m_30m,Constants.TIME_PERIOD_30m,
      Constants.STEP_PERIOD_1_3,Constants.STEP_PERIOD_4_6,
      Constants.STEP_PERIOD_7_9,Constants.STEP_PERIOD_10_30,
      Constants.STEP_PERIOD_30_60,Constants.STEP_PERIOD_60)
    val sumCount: Int = StringUtils.getFieldFromConcatString(resSum,"\\|",Constants.SESSION_COUNT).toInt
    for (it <- items){
      val value: Int = StringUtils.getFieldFromConcatString(resSum, "\\|", it).toInt
      val newValue = (value*100.0/sumCount).formatted("%.2f")
      resSum = StringUtils.setFieldInConcatString(resSum, "\\|", it, String.valueOf(newValue)+"%")
    }
    println(resSum)
  }



  def getSessionSum(sc: SparkContext, sessionId2AggregateInfoRDD: RDD[(String, String)]) = {

    val sessionSum = new SessionAccumulator
    sc.register(sessionSum, "SessionAcc")

    sessionId2AggregateInfoRDD.foreach(row=>{
      val str: String = row._2
      //获取访问停留时间和步长
      val stepLength = StringUtils.getFieldFromConcatString(str, "\\|", "stepLength").toInt
      val visitLength = (StringUtils.getFieldFromConcatString(str, "\\|", "visitLength").toInt)/1000
      //总数统计累加
      sessionSum.add(Constants.SESSION_COUNT)
      //停留时间统计累加
      if(visitLength>0 && visitLength<=3)
        sessionSum.add(Constants.TIME_PERIOD_1s_3s)
      else if(visitLength>3 && visitLength<=6)
        sessionSum.add(Constants.TIME_PERIOD_4s_6s)
      else if(visitLength>6 && visitLength<=9)
        sessionSum.add(Constants.TIME_PERIOD_7s_9s)
      else if(visitLength>9 && visitLength<=30)
        sessionSum.add(Constants.TIME_PERIOD_10s_30s)
      else if(visitLength>30 && visitLength<=60)
        sessionSum.add(Constants.TIME_PERIOD_30s_60s)
      else if(visitLength>60 && visitLength<=180)
        sessionSum.add(Constants.TIME_PERIOD_1m_3m)
      else if(visitLength>180 && visitLength<=600)
        sessionSum.add(Constants.TIME_PERIOD_3m_10m)
      else if(visitLength>600 && visitLength<=1800)
        sessionSum.add(Constants.TIME_PERIOD_10m_30m)
      else if(visitLength>1800)
        sessionSum.add(Constants.TIME_PERIOD_30m)
      //步长统计累加
      if(stepLength>0 && stepLength<= 3)
        sessionSum.add(Constants.STEP_PERIOD_1_3)
      else if(stepLength>3 && stepLength<= 6)
        sessionSum.add(Constants.STEP_PERIOD_4_6)
      else if(stepLength>6 && stepLength<= 9)
        sessionSum.add(Constants.STEP_PERIOD_7_9)
      else if(stepLength>9 && stepLength<= 30)
        sessionSum.add(Constants.STEP_PERIOD_10_30)
      else if(stepLength>30 && stepLength<= 60)
        sessionSum.add(Constants.STEP_PERIOD_30_60)
      else if(stepLength>60)
        sessionSum.add(Constants.STEP_PERIOD_60)
    })
    sessionSum
  }
}
