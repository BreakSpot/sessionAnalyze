package spark

import java.util.Date
import java.util.Locale.Category

import com.alibaba.fastjson.{JSON, JSONObject}
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.SessionAggrStat
import com.qf.sessionanalyze.test.MockData
import com.qf.sessionanalyze.util._
import org.apache.hadoop.mapreduce.v2.api.records.TaskId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import spark.lcl.{AreaTop3, CategoryTop10, Top10Session}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object UsrActionAnalyze {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder()
      .appName(Constants.SPARK_APP_NAME_SESSION)
      .getOrCreate()
    //生成模拟数据
    MockData.mock(sc, sparkSession)
    //获取任务
    //创建访问数据库的实例
    val taskDao = DAOFactory.getTaskDAO
    //访问taskDAO对应的数据表
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    println(taskId)
    val task = taskDao.findById(taskId)

    if (task == null) {
      sc.stop()
      println("没有获取到对应taskID的task信息")
      return
    }


    val taskParam = JSON.parseObject(task.getTaskParam)

    //获取参数指定范围的数据
    val actionRDD: DataFrame = getActionRDDByDataRange(sparkSession, taskParam)
    //(action)
    //(sessionid, action)
    val sessionId2ActionRDD = actionRDD.rdd.map(row => (row(2).toString, row))
    sessionId2ActionRDD.take(3).foreach(println)

    //    sessionId2ActionRDD.saveAsTextFile("f:/bigdata/session/out")

    //1.缓存后面要反复使用的RDD
    val sessionId2ActionRDDCache = sessionId2ActionRDD.cache()
    //2.分区数20，提高并行度，充分区，repartition
    //3.sc.textFile, sc.parallelize，分区或分片，过程中不会发生shuffle

    //把用户数据和用户访问的行为数据进行一个整合
    val sessionId2AggregateInfoRDD = aggregateByUserid(sc, sparkSession, sessionId2ActionRDD)
    //      .map(x=>{
    //      (x._1+"\001"+x._2+"task_id="+taskId+"|")
    //    })
    //    sessionId2AggregateInfoRDD.saveAsTextFile("f:/bigdata/session/out3")

    println("============按条件过滤并且累加===============")
    val sessionAccumulator = new SessionAccumulator
    sc.register(sessionAccumulator)
    val filteridSessionRDD = NewFilerAndAccumulate.filterSessionByParamRDD(sessionId2AggregateInfoRDD, taskParam, sessionAccumulator)
    filteridSessionRDD.cache().count()
    //NewFilerAndAccumulate.calculatePercent(sessionAccumulator.value, taskId)
    println(sessionAccumulator.value)
    println("=============join原表按时间进行抽取==========")
    //    //过滤后的数据session的明细
    //    val sessionDetailRDD = filteridSessionRDD.join(sessionId2ActionRDD).map(tuple=>{
    //      (tuple._1,(tuple._1,tuple._2._2))
    //    })
    //    //过滤后按时间抽取数据
    //    NewSample.extractSessionByRatio(filteridSessionRDD,sessionDetailRDD,taskId,sc)

    println("========获取点击、下单、支付次数排名的top10=======")
    /*
    //将筛选出来的数据join原数据处理成 (click_category_id, order_category_ids, pay_category_ids)格式
    val categoryInfo: RDD[(Long, String, String)] = filteridSessionRDD.join(sessionId2ActionRDD).map(tuple=>{
      val row = tuple._2._2
        val click_category_id = row.getAs[Long](6)
      val order_category_ids = row.getString(8)
      val pay_category_ids = row.getString(10)
      (click_category_id, order_category_ids, pay_category_ids)
    })
    val categoryTop10List = CategoryTop10.getCategoryTop10(categoryInfo, taskId,sc).take(10)
    val categoryTop10Rdd: RDD[(Long, (Int, Int, Int))] = sc.makeRDD(categoryTop10List)
    categoryTop10Rdd.foreach(println)
    */

    println("========top10活跃session=======")
    /*
    //将筛选出来的数据处理成（click_category_id，（xxxx））
    //top10去join处理的数据
    val categropAndSessionID = sessionId2ActionRDD.map(row=>{
      val sessionid = row._1
      val clickCategoryIds = row._2.getAs[Long](6)
      (clickCategoryIds, sessionid)
    })
//    (top10的category，sessionId)
    val categoryTop10AndSesstionId= categoryTop10Rdd.join(categropAndSessionID).map(row=>{
      (row._1, List((row._2._2, 1)))
    })
    Top10Session.getTop10Session(categoryTop10AndSesstionId, taskId)
    */

    println("========AreaTop3活跃session=======")
    AreaTop3.getAreaTop3(sessionId2ActionRDD, taskId, sc, sparkSession)

    sc.stop()
  }

  def aggregateByUserid(context: SparkContext, sparkSession: SparkSession, sessionId2ActionRDD: RDD[(String, Row)]) = {
    //先把数据根据sessionid进行聚合
    val sessionIdGroupBy: RDD[(String, Iterable[Row])] = sessionId2ActionRDD.groupByKey()
    sessionIdGroupBy.take(3).foreach(println)
    //把当前session下的所有行为进行聚合
    val sessionUsrInfo = sessionIdGroupBy.map(tuple => {
      val sessionId = tuple._1
      val searchKeyWordBuffer = new StringBuilder()
      val clickCategoryIdsBuffer = new StringBuilder()
      //用户id信息
      var usrId = 0L
      var startTime = new Date()
      var endTime = new Date()
      //当前会话访问的步长
      var stepLength = 0

      val it = tuple._2.iterator
      while (it.hasNext) {
        val row = it.next()
        usrId = row.getLong(1)
        val searcheKeyWord = row.getString(5)
        val clickCategoryId = String.valueOf(row.getAs[Long](6))
        //搜索词和点击的品类信息追加到汇总的stringBuffer
        if (!StringUtils.isEmpty(searcheKeyWord))
          if (!searchKeyWordBuffer.toString.contains(searcheKeyWord))
            searchKeyWordBuffer.append(searcheKeyWord + ",")
        if (clickCategoryId != null)
          if (!clickCategoryIdsBuffer.toString.contains(clickCategoryId))
            clickCategoryIdsBuffer.append(clickCategoryId + ",")
        //计算session的开始时间和结束时间
        val actionTime = DateUtils.parseTime(row.getString(4))
        if (actionTime.before(startTime))
          startTime = actionTime
        if (actionTime.after(endTime))
          endTime = actionTime
        stepLength += 1
      }
      //截取搜索关键字和点击品类的字符串的","
      val searcheWord = StringUtils.trimComma(searchKeyWordBuffer.toString)
      val clickCategroys = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

      //计算当前会话的访问时长
      val visitLength: Long = endTime.getTime - startTime.getTime

      //聚合数据，key=value|key=value|....
      val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searcheWord + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategroys + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"

      (usrId, aggrInfo)
    })
    //查询用户的信息
    val sql = "select * from user_info"
    val usrInfoRDD = sparkSession.sql(sql).rdd
    val userInfoRDD2 = usrInfoRDD.map(row => (row.getLong(0), row))
    val userFullInfo: RDD[(Long, (String, Row))] = sessionUsrInfo.join(userInfoRDD2)

    //处理成（sessionId, session行为信息+用户信息）
    val userSessionFullInfo: RDD[(String, String)] = userFullInfo.map(tuple => {
      //整个会话的信息
      val sessionInfo = tuple._2._1
      //用户信息
      val usrInfo = tuple._2._2
      //取用户信息
      val sessionId = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SESSION_ID)
      // 获取用户信息的age
      val age = usrInfo.getInt(3)
      // 获取用户信息的职业
      val professional = usrInfo.getString(4)
      // 获取用户信息的所在城市
      val city = usrInfo.getString(5)
      // 获取用户信息的性别
      val sex = usrInfo.getString(6)
      //整个会话的信息+用户的信息拼接
      val fullAggrInfo = sessionInfo + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex + "|"
      (sessionId, fullAggrInfo)
    })
    userSessionFullInfo
  }


  def getActionRDDByDataRange(sparkSession: SparkSession, taskParam: JSONObject): DataFrame = {
    //解析参数，拿到开始日期，结束日期
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDatete = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    //写sql，过滤满足条件的数据
    val sqlstr = "select * from user_visit_action where Date>='" +
      startDate + "' and Date<='" + endDatete + "' "
    val actionDF = sparkSession.sql(sqlstr)
    println("count:" + actionDF.count)
    actionDF.show(5)
    actionDF
  }
}