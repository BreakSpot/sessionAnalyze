package spark.lcl

import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.util.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SessionSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName(Constants.SPARK_APP_NAME_SESSION)
    val sc = new SparkContext(conf)

    //添加一个hour的标签值
    val sessionInfoRdd = sc.textFile("f:/bigdata/session/out3").map(line => {
      val str = line.split("\001")
      val id = str(0)
      val info = str(1)
      val time = StringUtils.getFieldFromConcatString(info, "\\|", "startTime")
      val hour = time.split(" ")(1).split(":")(0)
      (id, info, hour)
    })

    //将总数进行统计并广播
    val sessionCount = sessionInfoRdd.count()
    val broadcast = sc.broadcast(sessionCount)
    //以小时为key进行分组
    val res = sessionInfoRdd.groupBy(_._3).map(row=>{
      val sum = broadcast.value.toInt
      //转换成List
      val sessionList= row._2.iterator.toList
      val hourSessionCount = sessionList.length
      //此小时内session占这一天的百分比
      val num: Double = (hourSessionCount*100.0/sum).formatted("%.2f").toDouble
      //要抽取的数据量
      val n = (hourSessionCount*100.0/sum).round
      //随机列表，保证数据不重复
      var resultList:List[Int]=Nil
      while(resultList.length<n){
        val randomNum=(new Random).nextInt(hourSessionCount)
        if(!resultList.exists(s=>s==randomNum)){
          resultList=resultList:::List(randomNum)
        }
      }
      var hourSampleList = List[String]()
      for(i <- resultList){
        val sessionInfo = sessionList(i)._2

        val task_id = StringUtils.getFieldFromConcatString(sessionInfo,"\\|","task_id")
        val sessionId = StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_SESSION_ID)
        val start_time = StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_START_TIME)
        val search_keywords = StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS)
        val click_category_ids = StringUtils.getFieldFromConcatString(sessionInfo,"\\|",Constants.PARAM_CATEGORY_IDS)
        hourSampleList :+= ("task_id="+task_id+"|"+
          Constants.FIELD_SESSION_ID+"="+sessionId+"|"+Constants.FIELD_START_TIME+"="+start_time+"|"+
          Constants.FIELD_SEARCH_KEYWORDS+"="+search_keywords+"|"+Constants.PARAM_CATEGORY_IDS+"="+click_category_ids)
      }
      (row._1, hourSampleList)
    })

    res.foreach(println)
    sc.stop()

  }





}
