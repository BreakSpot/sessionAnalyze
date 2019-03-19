package spark
import java.util

import org.apache.spark.{SparkConf, SparkContext}
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.{SessionDetail, SessionRandomExtract}
import com.qf.sessionanalyze.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object NewSample {

  /**
    * 按比例随机抽取session
    * @param filterSessionRDD 筛选后的数据
    * @param sessionDetailRDD 筛选后的session及其对应的原数据
    * @param taskid
    * @param sc
    */
  def extractSessionByRatio(filterSessionRDD:RDD[(String,String)],sessionDetailRDD:RDD[(String,(String,Row))],taskid:Long,sc:SparkContext): Unit ={
    //1,计算每个小时session个数
    //最后数据格式结果（date+hour,count）
    val time2SessionIdRDD: RDD[(String, String)] = filterSessionRDD.map(tuple=>{
    //当前session对应的数据信息取出来
    val sessionInfo = tuple._2
    //取出当前会话的开始时间yyyy-mm--dd hh:mm:ss
    val startTime = StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_START_TIME)
    //解析出来session的日期和小时
    val dateHour = DateUtils.getDateHour(startTime)
    (dateHour,sessionInfo)

    })
    //返回的每天每小时的session个数(dateHour,count)
    val countMap: collection.Map[String, Long] = time2SessionIdRDD.countByKey()

    //2,按比例随机抽取
    //每天应该抽取的session个数
    //数据格式（date,(hour,count)）
    val dateHourCountMap = new mutable.HashMap[String,mutable.HashMap[String,Long]]()
    //遍历countMap(dateHour,count)=》（date,(hour,count)）
    import scala.collection.JavaConversions._
    
    
    for(countEntry<-countMap.entrySet()){
      //取出dateHour日期和小时
      val dateHour = countEntry.getKey
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      //取出session个数
      val count = countEntry.getValue
      var hourCountMap = dateHourCountMap.get(date).getOrElse(null)
      if(hourCountMap == null){
        hourCountMap = new mutable.HashMap[String,Long]()
        dateHourCountMap.put(date,hourCountMap)
      }
      hourCountMap.put(hour,count)
    }

    //计算数据集总天数，计算每天抽取的session个数(抽取总的session100)
    val extractNum = 100/dateHourCountMap.size
    //每天每小时抽取的个数
    val dateHourExtrarctMap=new mutable.HashMap[String,mutable.HashMap[String,ListBuffer[Int]]]()
    for(dateHourCountEntry<-dateHourCountMap.entrySet){
      //获取日期
      val date = dateHourCountEntry.getKey
      //当前日期下每小时的session个数
      val hourCountMap = dateHourCountEntry.getValue
      //计算当天的session总数
      var sessionCount = 0L
      for(count<-hourCountMap.values){
        sessionCount +=count
      }

      var hourExtractMap = dateHourExtrarctMap.get(date).getOrElse(null)
      if(hourExtractMap == null){
        hourExtractMap = new mutable.HashMap[String,ListBuffer[Int]]()
        dateHourExtrarctMap.put(date,hourExtractMap)
      }
      //计算每个小时抽取的session个数
      for(hourCountEntry<-hourCountMap.entrySet()){
        val hour = hourCountEntry.getKey
        //取当前小时的session个数
        val count = hourCountEntry.getValue
        //计算当前小时要抽取的session个数
        var extractCount = (count.toDouble/sessionCount.toDouble*extractNum).toInt
        if(extractCount > count)extractCount = count.toInt
        //计算抽取的session索引信息
        var extractIndexList = new ListBuffer[Int]()
        extractIndexList = hourExtractMap.get(hour).getOrElse(null)
        if(extractIndexList == null){
          extractIndexList = new ListBuffer[Int]()
          hourExtractMap.put(hour,extractIndexList)
        }

        var random = new Random()

        //随机生成要抽取的数据的索引
        var i = 0
        while (i<extractCount){
          var extractIndex = random.nextInt(count.toInt)
          //判断随机的索引是否重复
          while(extractIndexList.contains(extractIndex)){
            extractIndex = random.nextInt(count.toInt)
          }
          extractIndexList.add(extractIndex)
          i+=1
        }
      }
    }

    //把抽取的信息（date,(hour,(1,34,56,90))）广播到每一个节点上
    val dataHourExtractMapBroadcast = sc.broadcast(dateHourExtrarctMap)

    //3,根据生成的dateHourExtrarctMap字典，在task上抽取相应的session
    //(datehour,sessioninfo)=>(datahour,iterator(sessioninfo))
    val time2SessionRDD = time2SessionIdRDD.groupByKey()
    //根据计算出来的索引信息抽取具体的session
    val extractSessionRDD = time2SessionRDD.map(
      tuple=>{
        //存储抽取的结果（sessionid,sessionid）
        val extractSessionids = new util.ArrayList[(String,String)]()
        val dateHour = tuple._1
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        //获取广播变量
        val dateHourExtractMap = dataHourExtractMapBroadcast.value
        //获取抽取的索引列表
        val extractIndexList = dateHourExtractMap.get(date).get(hour)
        val sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO
        val it = tuple._2.iterator
        var index = 0
        var sessionid =""
        while (it.hasNext){
          //当前的索引是否在抽取的索引列表里
          val sessioninfo = it.next()
          if(extractIndexList.contains(index)) {
            sessionid = StringUtils.getFieldFromConcatString(sessioninfo, "\\|", Constants.FIELD_SESSION_ID)
            val sessionRandomExtract = new SessionRandomExtract
            sessionRandomExtract.setTaskid(taskid)
            sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessioninfo,"\\|",Constants.FIELD_START_TIME))
            sessionRandomExtract.setSessionid(sessionid)
            sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessioninfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS))
            sessionRandomExtract.setClickCategoryIds(
              StringUtils.getFieldFromConcatString(sessioninfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS)
            )
            sessionRandomExtractDAO.insert(sessionRandomExtract)
            //将抽取的sessionid存入list
            extractSessionids.add((sessionid,sessionid))
          }
          index +=1
        }
        (sessionid,sessionid)
      }
    )

    //4,获取抽取的session的明细数据(sessionid,(sessionid,(sessionid,row)))
    val extractSessionDetailRDD: RDD[(String, (String, (String, Row)))] = extractSessionRDD.join(sessionDetailRDD)
    val sessionDetails = new util.ArrayList[SessionDetail]()

    extractSessionDetailRDD.foreachPartition(partition=>{
      partition.foreach(tuple=>{
        val row = tuple._2._2._2
        val sessionDetail = new SessionDetail
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getLong(1))
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getLong(3))
        sessionDetail.setActionTime(row.getString(4))
        sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setClickCategoryId(row.getAs[Long](6))
        sessionDetail.setClickProductId(row.getAs[Long](7))
        sessionDetail.setOrderCategoryIds(row.getString(8))
        sessionDetail.setOrderProductIds(row.getString(9))
        sessionDetail.setPayCategoryIds(row.getString(10))
        sessionDetail.setPayProductIds(row.getString(11))
        sessionDetails.add(sessionDetail)
      })
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insertBatch(sessionDetails)
    })

  }
}
