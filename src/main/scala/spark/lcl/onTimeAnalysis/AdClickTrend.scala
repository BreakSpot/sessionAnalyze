package spark.lcl.onTimeAnalysis

import java.util
import java.util.Date

import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.AdClickTrend
import com.qf.sessionanalyze.util.DateUtils
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

object AdClickTrend {
  def getAdClickTrend(currentMessages: DStream[(Date, String, String, String, String)]) {
    val DHMAdClick = currentMessages.map(tuple=>{
      val dataTime = DateUtils.formatTime(tuple._1)
      val date = DateUtils.formatDate(tuple._1)
      val hourMin = dataTime.split(" ")(1).split(":")
      val hour = hourMin(0)
      val min = hourMin(1)
      val adId = tuple._5
      ((date, hour, min, adId), 1)
    })
    val windowDHMAdClick = DHMAdClick.reduceByKeyAndWindow((x:Int, y:Int) =>x+y, Seconds(3600), Seconds(5))
    windowDHMAdClick.print()
    windowDHMAdClick.foreachRDD(partition=>{
      partition.foreachPartition(tuple=>{
        val list = new util.ArrayList[AdClickTrend]()
        while (tuple.hasNext){
          val info = tuple.next()
          val date = info._1._1
          val hour = info._1._2
          val min = info._1._3
          val adId = info._1._4
          val num = info._2

          val adClickTrend = new AdClickTrend
          adClickTrend.setDate(date)
          adClickTrend.setHour(hour)
          adClickTrend.setMinute(min)
          adClickTrend.setAdid(adId.toLong)
          adClickTrend.setClickCount(num.toLong)
          list.add(adClickTrend)
        }
        val clickTrendDAO = DAOFactory.getAdClickTrendDAO
        clickTrendDAO.updateBatch(list)
      })
    })
  }
}