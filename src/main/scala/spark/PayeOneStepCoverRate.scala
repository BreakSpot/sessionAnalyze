package spark

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.PageSplitConvertRate
import com.qf.sessionanalyze.test.MockData
import com.qf.sessionanalyze.util.{DateUtils, NumberUtils, ParamUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import spark.UsrActionAnalyze.getActionRDDByDataRange

import scala.collection.mutable

object PayeOneStepCoverRate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE).setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName(Constants.SPARK_APP_NAME_PAGE).getOrCreate()

    // 生成模拟数据
    MockData.mock(sc, sparkSession)
    // 获取任务
    // 创建访问数据库的实例
    val taskDAO = DAOFactory.getTaskDAO
    // 访问taskDAO对应的数据表
    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE)
    println("================================================================")
    println("taskid:" + taskId)
    println("================================================================")
    val task = taskDAO.findById(taskId)
    if (task == null) {
      println("没有获取到对应taskID的task信息")
      return
    }

    val taskParam = JSON.parseObject(task.getTaskParam)
    // 获取参数指定范围的数据 .rdd 后变成RDD
    // row
    val actionRDD: RDD[Row] = getActionRDDByDataRange(sparkSession, taskParam).rdd
    // (sessionid,row)
    val sessionId2Action: RDD[(String, Row)] = actionRDD.map(row => {
      (row.getString(2), row)
    })
    // 按sessionID分组，得到每一个会话的所有行为
    val groupedsessionId2Action: RDD[(String, Iterable[Row])] = sessionId2Action.groupByKey()
    // 计算每个session用户的访问轨迹（1,2,3）=> 得到页面切片
    val pageSplitRDD: RDD[(String, Int)] = generateSplit(sc, groupedsessionId2Action, taskParam)
    println("--------------")
    //("1_2",count)
    val pageSplitMap = pageSplitRDD.countByKey()

    // 统计开始页面的访问次数
    val startPageVisitCount = getStartPageVisit(groupedsessionId2Action, taskParam)

    // 计算每一个页面的单跳转化率
    val converRateMap: mutable.HashMap[String, Double] = computePageSplitConverRate(taskParam, pageSplitMap, startPageVisitCount)
    println(converRateMap)
    // 把上一步结果保存到数据库，(taskId,"1_2=0.88|2_3=0.22...")
    insertConverRateToDB(taskId, converRateMap)

  }

  def generateSplit(sc: SparkContext, groupedsessionId2Action: RDD[(String, Iterable[Row])], taskparam: JSONObject) = {
    // 解析参数，拿到页面流
    val targetPageFlow: String = ParamUtils.getParam(taskparam, Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPageFlowBroadcast = sc.broadcast(targetPageFlow)
    // ("1_2",1)
    var list = new mutable.ListBuffer[(String, Int)]()
    // 计算每一个session中符合条件的页面切片
    groupedsessionId2Action.flatMap(tup => {
      val it = tup._2.iterator
      // 取目标页面流
      val targetPages = targetPageFlowBroadcast.value.split(",")
      // 遍历当前会话的页面流
      var rows = new mutable.ListBuffer[Row]
      while (it.hasNext)
        rows += it.next()
      //  按照时间，把当前会话的所有行为进行排序
      implicit val keyOrder = new Ordering[Row] {
        override def compare(x: Row, y: Row): Int = {
          //"yyyy--mm--dd hh:mm:ss"
          val actionTime1 = x.getString(4)
          val actionTime2 = y.getString(4)
          val dateTime1: Date = DateUtils.parseTime(actionTime1)
          val dateTime2: Date = DateUtils.parseTime(actionTime2)
          (dateTime1.getTime - dateTime2.getTime).toInt
        }
      }
      rows = rows.sorted
      // 生成页面切片
      var lastpageId = -1L
      for (elem <- rows) {
        val pageId = elem.getLong(3)
        import scala.util.control.Breaks._
        if (lastpageId == -1) {
          lastpageId = pageId
        }
        else {
          val pageSplit = lastpageId + "_" + pageId
          // 判断当前切片在不在目标页面流
          var i = 1
          breakable {
            while (i < targetPages.length) {
              val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
              if (pageSplit == targetPageSplit) {
                list:+=((pageSplit, 1))
                break
              }
              i = i + 1
            }
          }
        }
        lastpageId = pageId
      }
      list
    })
  }

  // 获取开始页面的访问量
  def getStartPageVisit(groupedsessionId2Action: RDD[(String, Iterable[Row])], taskParam: JSONObject) = {
    // 解析首页是哪个页面
    val targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    val startPageId = targetPageFlow.split(",")(0).toLong

    // 定义一个容器，存放到达第一个页面的访问
    val list = new mutable.ListBuffer[Long]
    val startPageRDD = groupedsessionId2Action.map(tup => {
      val it = tup._2.iterator
      while (it.hasNext) {
        val row = it.next()
        val pageId = row.getLong(3)
        if (pageId == startPageId) {
          list += pageId
        }
      }
    })
    startPageRDD.count()

  }

  def computePageSplitConverRate(taskparm: JSONObject, pageSplitMap: scala.collection.Map[String, Long], startPageVisitCount: Long) = {
    // 解析参数，获取目标页面流
    val targetPages = ParamUtils.getParam(taskparm, Constants.PARAM_TARGET_PAGE_FLOW).split(",")
    // 上个切片的访问pv
    var lastPageSplitPV = 0.0
    // 根据要计算的切片的访问率，计算每一个切片的转化率（即页面单跳转化率）
    var convertRateMap = new mutable.HashMap[String, Double]()
    var i = 1
    while (i < targetPages.length) {
      val targetPageSplit = targetPages(i - 1) + "_" + targetPages(i)
      val targetPageSplitPV: Long = pageSplitMap.getOrElse(targetPageSplit, 0)
      var convertRate = 0.0
      if (startPageVisitCount != 0) {
        if (i == 1) convertRate = NumberUtils.formatDouble(targetPageSplitPV / startPageVisitCount, 2)
        else convertRate = NumberUtils.formatDouble(targetPageSplitPV / lastPageSplitPV, 2)
      }
      convertRateMap.put(targetPageSplit, convertRate)
      lastPageSplitPV = targetPageSplitPV
      i += 1
    }
    convertRateMap
  }

  def insertConverRateToDB(taskid: Long, converRateMap: mutable.HashMap[String, Double]) = {
    // 把convertMap中字符串"1_2=0.88|2_3=0.22..."
    val pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO
    val keys = converRateMap.keySet
    for (key <- keys) {
      val pageSplitConvertRate = new PageSplitConvertRate
      pageSplitConvertRate.setTaskid(taskid)
      pageSplitConvertRate.setConvertRate(key + "=" + converRateMap.get(key))
      pageSplitConvertRateDAO.insert(pageSplitConvertRate)
    }
  }

}
