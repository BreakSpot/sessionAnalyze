package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.SessionAggrStat
import com.qf.sessionanalyze.util.{NumberUtils, ParamUtils, StringUtils, ValidUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object NewFilerAndAccumulate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
    val sc = new SparkContext(conf)

    val sessionId2AggregateInfoRDD: RDD[(String, String)] = sc.textFile("f:/bigdata/session/out2").map(line=>{
      val str = line.split("\001")
      val id = str(0)
      val info = str(1)
      (id, info)
    })
    //创建访问数据库的实例
    val taskDao = DAOFactory.getTaskDAO
    //访问taskDAO对应的数据表
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    println(taskId)
    val task = taskDao.findById(taskId)

    val taskParam = JSON.parseObject(task.getTaskParam)
    println(taskParam)
    val sessionAccumulator = new SessionAccumulator
    sc.register(sessionAccumulator)
    val filteridSessionRDD = filterSessionByParamRDD(sessionId2AggregateInfoRDD, taskParam,sessionAccumulator)
    println("=====================================")
    filteridSessionRDD.count()
    calculatePercent(sessionAccumulator.value, taskId)
    println(sessionAccumulator.value)
    println("=====================================")

    sc.stop()

  }

  def fiterFun(tuple: (String, String), param: String, accumlator: SessionAccumulator): Boolean = {
    //session信息取出来
    val info = tuple._2
    //比较info里面信息是否满足param
    if(!ValidUtils.between(info,  Constants.FIELD_AGE, param, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
      return false
    if(!ValidUtils.in(info, Constants.FIELD_PROFESSIONAL, param, Constants.PARAM_PROFESSIONALS))
      return false
    if(!ValidUtils.in(info, Constants.FIELD_CITY , param, Constants.PARAM_CITIES ))
      return false
    if(!ValidUtils.in(info, Constants.FIELD_SEX , param, Constants.PARAM_SEX ))
      return false
    if(!ValidUtils.in(info, Constants.FIELD_SEARCH_KEYWORDS , param, Constants.PARAM_KEYWORDS ))
      return false
    if(!ValidUtils.in(info, Constants.FIELD_CATEGORY_ID , param, Constants.PARAM_CATEGORY_IDS ))
      return false
    //把满足条件的session的访问时长，访问步长取出来，使用累加器累加
    //累加总的session_count
    accumlator.add(Constants.SESSION_COUNT)
    //先把该条session的访问时长和访问步长取出来
    val visitLength = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_VISIT_LENGTH).toInt/1000
    val stepLength = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_STEP_LENGTH ).toInt
    //判断当前访问的时长和步长
    //停留时间统计累加
    if(visitLength>0 && visitLength<=3)
      accumlator.add(Constants.TIME_PERIOD_1s_3s)
    else if(visitLength>3 && visitLength<=6)
      accumlator.add(Constants.TIME_PERIOD_4s_6s)
    else if(visitLength>6 && visitLength<=9)
      accumlator.add(Constants.TIME_PERIOD_7s_9s)
    else if(visitLength>9 && visitLength<=30)
      accumlator.add(Constants.TIME_PERIOD_10s_30s)
    else if(visitLength>30 && visitLength<=60)
      accumlator.add(Constants.TIME_PERIOD_30s_60s)
    else if(visitLength>60 && visitLength<=180)
      accumlator.add(Constants.TIME_PERIOD_1m_3m)
    else if(visitLength>180 && visitLength<=600)
      accumlator.add(Constants.TIME_PERIOD_3m_10m)
    else if(visitLength>600 && visitLength<=1800)
      accumlator.add(Constants.TIME_PERIOD_10m_30m)
    else if(visitLength>1800)
      accumlator.add(Constants.TIME_PERIOD_30m)
    //步长统计累加
    if(stepLength>0 && stepLength<= 3)
      accumlator.add(Constants.STEP_PERIOD_1_3)
    else if(stepLength>3 && stepLength<= 6)
      accumlator.add(Constants.STEP_PERIOD_4_6)
    else if(stepLength>6 && stepLength<= 9)
      accumlator.add(Constants.STEP_PERIOD_7_9)
    else if(stepLength>9 && stepLength<= 30)
      accumlator.add(Constants.STEP_PERIOD_10_30)
    else if(stepLength>30 && stepLength<= 60)
      accumlator.add(Constants.STEP_PERIOD_30_60)
    else if(stepLength>60)
      accumlator.add(Constants.STEP_PERIOD_60)

    return true
  }
  //计算各个范围访问时长、在总的session中的占比
  def calculatePercent(accumulatorValue: String, taskId:Long): Unit ={

    //计算各个访问时长和步长
    val sessionCount = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.SESSION_COUNT).toDouble

    val visitLength_1s_3s = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_1s_3s)
    val visitLength_4s_6s = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_4s_6s)
    val visitLength_7s_9s = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_7s_9s)
    val visitLength_10s_30s = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_10s_30s)
    val visitLength_30s_60s = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_30s_60s)
    val visitLength_1m_3m = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_1m_3m)
    val visitLength_3m_10m = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_3m_10m)
    val visitLength_10m_30m = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_10m_30m)
    val visitLength_30m = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.TIME_PERIOD_30m)

    val stepLength_1_3 = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.STEP_PERIOD_1_3)
    val stepLength_4_6 = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.STEP_PERIOD_4_6)
    val stepLength_7_9 = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.STEP_PERIOD_7_9)
    val stepLength_10_30 = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.STEP_PERIOD_10_30)
    val stepLength_30_60 = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.STEP_PERIOD_30_60)
    val stepLength_60 = StringUtils.getFieldFromConcatString(accumulatorValue,"\\|",Constants.STEP_PERIOD_60)

    val visitLength_1s_3s_ratio = NumberUtils.formatDouble(visitLength_1s_3s.toDouble/sessionCount,2)
    val visitLength_4s_6s_ratio = NumberUtils.formatDouble(visitLength_4s_6s.toDouble/sessionCount,2)
    val visitLength_7s_9s_ratio = NumberUtils.formatDouble(visitLength_7s_9s.toDouble/sessionCount,2)
    val visitLength_10s_30s_ratio = NumberUtils.formatDouble(visitLength_10s_30s.toDouble/sessionCount,2)
    val visitLength_30s_60s_ratio = NumberUtils.formatDouble(visitLength_30s_60s.toDouble/sessionCount,2)
    val visitLength_1m_3m_ratio = NumberUtils.formatDouble(visitLength_1m_3m.toDouble/sessionCount,2)
    val visitLength_3m_10m_ratio = NumberUtils.formatDouble(visitLength_3m_10m.toDouble/sessionCount,2)
    val visitLength_10m_30m_ratio = NumberUtils.formatDouble(visitLength_10m_30m.toDouble/sessionCount,2)
    val visitLength_30m_ratio = NumberUtils.formatDouble(visitLength_30m.toDouble/sessionCount,2)

    val stepLength_1_3_ratio = NumberUtils.formatDouble(stepLength_1_3.toDouble/sessionCount,2)
    val stepLength_4_6_ratio = NumberUtils.formatDouble(stepLength_4_6.toDouble/sessionCount,2)
    val stepLength_7_9_ratio = NumberUtils.formatDouble(stepLength_7_9.toDouble/sessionCount,2)
    val stepLength_10_30_ratio = NumberUtils.formatDouble(stepLength_10_30.toDouble/sessionCount,2)
    val stepLength_30_60_ratio = NumberUtils.formatDouble(stepLength_30_60.toDouble/sessionCount,2)
    val stepLength_60_ratio = NumberUtils.formatDouble(stepLength_60.toDouble/sessionCount,2)



    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskId)
    sessionAggrStat.setSession_count(sessionCount.toLong)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visitLength_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visitLength_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visitLength_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visitLength_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visitLength_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visitLength_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visitLength_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visitLength_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visitLength_30m_ratio)

    sessionAggrStat.setStep_length_1_3_ratio(stepLength_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(stepLength_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(stepLength_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(stepLength_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(stepLength_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(stepLength_60_ratio)

    val sessionAggrStatDao = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDao.insert(sessionAggrStat)

  }
  def filterSessionByParamRDD(sessionId2AggregateInfoRDD: RDD[(String, String)], taskParam:JSONObject, sessionAccumulator: SessionAccumulator): RDD[(String, String)] ={
    val startAge = ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)
    val professionInfo = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS)
    val city = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX)
    val keyword = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS)
    val category = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS)

    //参数拼接成一个字符串
    var param = (if(startAge!=null) Constants.PARAM_START_AGE+"="+startAge+"|"
    else "")+(if(endAge!=null) Constants.PARAM_END_AGE+"="+endAge+"|"
    else "")+(if(professionInfo!=null) Constants.PARAM_PROFESSIONALS+"="+professionInfo+"|"
    else "")+(if(city!=null) Constants.PARAM_CITIES+"="+city+"|"
    else "")+(if(sex!=null) Constants.PARAM_SEX +"="+sex+"|"
    else "")+(if(keyword!=null) Constants.PARAM_KEYWORDS +"="+keyword +"|"
    else "")+(if(category !=null) Constants.PARAM_CATEGORY_IDS +"="+category+"|"
    else "")

    //结尾是|，截掉
    if(param.endsWith("|")) param = param.substring(0,param.length-1)
    //把拼接之后的参数，传给过滤函数，根据参数值对数据集过滤
    //把每一条数据过滤，如果满足参数条件，数据中访问时长，访问步数取出，通过累加器把相应字段进行累加
    val res: RDD[(String, String)] = sessionId2AggregateInfoRDD.filter(fiterFun(_,param, sessionAccumulator))
    res
  }

}
