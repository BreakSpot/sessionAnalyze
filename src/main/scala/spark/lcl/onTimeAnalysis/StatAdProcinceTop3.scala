package spark.lcl.onTimeAnalysis

import java.util

import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.AdProvinceTop3
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

object StatAdProcinceTop3 {
  def getStatAdProcinceTop3(adStateList: DStream[((String, String, String, String), Int)], sparkSession:SparkSession): Unit ={
    val provinceAdCount = adStateList.map(tuple=>{
      ((tuple._1._1, tuple._1._2, tuple._1._4),tuple._2)
    }).reduceByKey(_+_).map(tuple=>{
      (tuple._1._1, tuple._1._2, tuple._1._3.toLong, tuple._2.toLong)
    })
    provinceAdCount.print()
    import sparkSession.implicits._
    val s = provinceAdCount.foreachRDD{rdd =>
      val DataFrame = rdd.toDF("date", "province","adId", "num")
      DataFrame.createOrReplaceTempView("table")
      val wordCountsDataFrame =
        sparkSession.sql(
          """
            |select date, province, adId, num, rank
            |from(
            | select date, province, adId, num,
            | row_number() over (partition by date, province order by num desc) as rank
            | from table)
            |where rank < 4
          """.stripMargin)
      //+----------+--------+----+---+----+
      //|      date|province|adId|num|rank|
      //+----------+--------+----+---+----+
      //|2018-12-18|   Hubei|   4|  3|   1|
      //|2018-12-18|   Hubei|   2|  2|   2|
      //|2018-12-18|   Hubei|   3|  1|   3|
      //|2018-12-18| Jiangsu|   4|  3|   1|
      //+----------+--------+----+---+----+

      val list = new util.ArrayList[AdProvinceTop3]()
      wordCountsDataFrame.foreachPartition(partition=>{

        while (partition.hasNext){
          val row = partition.next()
          val date = row.getAs[String]("date")
          val province = row.getAs[String]("province")
          val adId = row.getAs[Long]("adId")
          val num = row.getAs[Long]("num")

          val provinceTop = new AdProvinceTop3
          provinceTop.setAdid(adId)
          provinceTop.setClickCount(num)
          provinceTop.setDate(date)
          provinceTop.setProvince(province)
          list.add(provinceTop)
        }
        val provinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
        provinceTop3DAO.updateBatch(list)
      })
    }
  }
}
