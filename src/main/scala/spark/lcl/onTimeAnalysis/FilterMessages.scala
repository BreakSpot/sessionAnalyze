package spark.lcl.onTimeAnalysis

import java.util
import java.util.Date

import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.AdBlacklist
import com.qf.sessionanalyze.util.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable

/**
  * 基于黑名单的非法广告点击流量过滤机制
  */

//1545053277205 Henan Zhengzhou 5 1
object FilterMessages {
  def getFilterMessages(messages: InputDStream[(String, String)], sc:SparkContext): DStream[(Date, String, String, String, String)] = {

    //基于黑名单的非法广告点击流量过滤机制
    //从mysql中动态获取黑名单中的用户id
    val blackList: RDD[(Long, Boolean)] = getAdBlackList(sc)
    //原数据处理
    val adInfo: DStream[(Long, Array[String])] = messages.map(tuple=>{
      val messagesInfo = tuple._2.split(" ")
      val userID = messagesInfo(3).toLong
      (userID, messagesInfo)
    })
    //两块数据进行join并过滤
    val filterBlackMessages = adInfo.transform(_.leftOuterJoin(blackList)).filter(x=>{
      val flag = x._2._2.toString
      if(flag.equals("Some(true)")){
        false
      }
      else
        true
    }).map(tuple=>{
      val mess = tuple._2._1
      val dateTimeLong = mess(0)
      val dateTime = DateUtils.stampToDate(dateTimeLong)
      val province = mess(1)
      val city = mess(2)
      val userID = mess(3)
      val adID = mess(4)
      (dateTime, province, city, userID, adID)
    })
    filterBlackMessages
  }
  /**
    * 获取黑名单
    * @param sc
    * @return
    */
  def getAdBlackList(sc:SparkContext): RDD[(Long, Boolean)] ={
    val blacklists = DAOFactory.getAdBlacklistDAO.findAll().iterator()
    var listRdd = new mutable.ListBuffer[(Long, Boolean)]()
    while (blacklists.hasNext){
      val uid = blacklists.next().getUserid
      listRdd:+=(uid,true)
    }
    val res = sc.makeRDD(listRdd)
    res
  }
}
