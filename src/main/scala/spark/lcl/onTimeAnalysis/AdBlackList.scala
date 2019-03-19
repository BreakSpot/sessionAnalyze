package spark.lcl.onTimeAnalysis

import java.util

import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.{AdBlacklist, AdUserClickCount}
import com.qf.sessionanalyze.util.DateUtils
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.util.control.Breaks._

/**
  * 实时的动态获取黑名单机制，将每天对每个广告点击超过100次的用户拉黑
  */
//1545053277205 Henan Zhengzhou 5 1
object AdBlackList {

  def addAdBlackList(messages: InputDStream[(String, String)]): Unit = {
    val lines = messages.map(_._2)
    val userInfoRDD = lines.map(row => {
      val userInfo = row.split(" ")
      val dateTimeLong = userInfo(0)
      val dateTime = DateUtils.stampToDate(dateTimeLong)
      val date = DateUtils.formatDate(dateTime)
      val province = userInfo(1)
      val city = userInfo(2)
      val userID = userInfo(3)
      val adID = userInfo(4)
      ((date, userID, adID), 1)
    })
    val allAdUserClickCount = userInfoRDD.reduceByKey(_+_)
    AdUserClickCountFunc(allAdUserClickCount)
    //过滤
    val adUserBlack: DStream[((String, String, String), Int)] = allAdUserClickCount.filter(x => findBackList(x))
    //整理
    val adUserBlackUid = adUserBlack.map(_._1._2)
    //去重
    val distinctUidList = adUserBlackUid.transform(_.distinct())
    //存入数据库
    adUuserBlackListinputMySql(distinctUidList)
//    adUserBlack.print()

  }



  /**
    * 存入黑名单数据库
    * @param userId
    */
  def adUuserBlackListinputMySql(userId: DStream[String]) = {
    userId.foreachRDD(tuple=>{
      tuple.foreachPartition(partiton => {
        val adBlacklistDAO = DAOFactory.getAdBlacklistDAO
        var list = new util.ArrayList[AdBlacklist]()
        while(partiton.hasNext){
          val row = partiton.next()
          val userID: Long = row.toLong
          var flag = true
          val adBlacklist = adBlacklistDAO.findAll().iterator()
          //判断是否在数据中
          breakable {
            while (adBlacklist.hasNext) {
              val id: Long = adBlacklist.next().getUserid
              if (id == userID) {
                flag = false
                break
              }
            }
          }
//          println()
          if(flag){
            val usr = new AdBlacklist
            usr.setUserid(userID)
            list.add(usr)
          }
        }
        adBlacklistDAO.insertBatch(list)
      })
    })
  }

  /**
    * 过滤函数
    *
    * @param tuple
    * @return
    */
  def findBackList(tuple: ((String, String, String), Int)): Boolean = {
    val data = tuple._1._1
    val userId = tuple._1._2.toLong
    val adId = tuple._1._3.toLong

    val adUserClickCount = DAOFactory.getAdUserClickCountDAO
    val count = adUserClickCount.findClickCountByMultiKey(data, userId, adId)
    if (count > 10) {
      return true
    }
    return false
  }
  /**
    * 实时添加到数据库
    *
    * @param allAdUserClickCount
    */
  def AdUserClickCountFunc(allAdUserClickCount: DStream[((String, String, String), Int)]): Unit = {
    allAdUserClickCount.foreachRDD(tuple => {
      tuple.foreachPartition(partition => {
        var list = new util.ArrayList[AdUserClickCount]()
        while(partition.hasNext){
          val row = partition.next()
          val date = row._1._1
          val userID = row._1._2.toLong
          val adId = row._1._3.toLong
          val adCount = row._2

          val info = new AdUserClickCount
          info.setUserid(userID.toInt)
          info.setDate(date)
          info.setAdid(adId)
          info.setClickCount(adCount)
          list.add(info)
        }
        val adUserClickCount = DAOFactory.getAdUserClickCountDAO
        adUserClickCount.updateBatch(list)
      })
    })
  }
}
