package spark.lcl.onTimeAnalysis

import java.util

import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.AdStat
import org.apache.spark.streaming.dstream.DStream


object AdStatSql {
  def deteCityAdCount(adStateList: DStream[((String, String, String, String), Int)]): Unit ={
    adStateList.foreachRDD(partition=>{
      partition.foreachPartition(tuple=>{
        val list = new util.ArrayList[AdStat]()
        while (tuple.hasNext){
          val info = tuple.next()
          val date = info._1._1
          val province = info._1._2
          val city = info._1._3
          val adId = info._1._4.toLong
          val count = info._2.toLong

          val adStat = new AdStat
          adStat.setDate(date)
          adStat.setProvince(province)
          adStat.setCity(city)
          adStat.setAdid(adId)
          adStat.setClickCount(count)
          list.add(adStat)
        }
        val statDAO = DAOFactory.getAdStatDAO
        statDAO.updateBatch(list)
      })
    })
  }
}
