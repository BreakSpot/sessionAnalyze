package spark.lcl

import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.Top10Session
import org.apache.spark.rdd.RDD

object Top10Session {
  def getTop10Session(categoryTop10AndSesstionId: RDD[(Long, List[(String, Int)])], taskId: Long): Unit = {
    categoryTop10AndSesstionId.reduceByKey((list1, list2) => (list1 ::: list2)
      .groupBy(_._1)
      .mapValues(_.foldLeft[Int](0)(_ + _._2))
      .toList).map(row => {
      val sortList = row._2.sortBy(_._2).reverse.take(10)
      (row._1, sortList)
    }).flatMap(tp => {
      tp._2.map(id => (tp._1, id._1, id._2))
    }).foreach(row => {
      val top10SessionDao = new Top10Session
      top10SessionDao.setTaskid(taskId)
      top10SessionDao.setCategoryid(row._1)
      top10SessionDao.setSessionid(row._2)
      top10SessionDao.setClickCount(row._3)
      val sessionTop = DAOFactory.getTop10SessionDAO
      sessionTop.insert(top10SessionDao)
    })
  }
}