package spark

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.qf.sessionanalyze.conf.ConfigurationManager
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.{AdBlacklist, AdUserClickCount}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ConsumerTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DirectKafkaConsumer").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    // 定义需要读取的topic
    val topic = Constants.KAFKA_TOPICS

    val topicsSet: Set[String] = topic.split(",").toSet
    val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // 每天每个用户点击每个广告的次数DS
    val dailyUserAdClickCountDS: DStream[((String, String, String), Int)] = messages.map(x => {
      val timestamp = x._2.split(" ")(0).toLong
      val dateTime = new Date(timestamp)
      val simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
      val date = simpleDateFormat.format(dateTime)
      val userId = x._2.split(" ")(3)
      val adId = x._2.split(" ")(4)

      ((date,userId,adId),1)
    }).reduceByKey(_+_)

    // 写入mySql
    dailyUserAdClickCountDS.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val adUserClickCountLists = new util.ArrayList[AdUserClickCount]()
        partition.foreach(x => {
          val date = x._1._1
          val userId = x._1._2
          val adId = x._1._3
          val clickCount = x._2
          val adUserClickCount = new AdUserClickCount
          adUserClickCount.setDate(date)
          adUserClickCount.setUserid(userId.toLong)
          adUserClickCount.setAdid(adId.toLong)
          adUserClickCount.setClickCount(clickCount)
          adUserClickCountLists.add(adUserClickCount)
        })
        val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
        adUserClickCountDAO.updateBatch(adUserClickCountLists)
      })
    })

    // 黑名单的用户信息和点击数DS
    val blackListDS = dailyUserAdClickCountDS.filter(x => {
      val date = x._1._1
      val userId = x._1._2
      val adId = x._1._3

      val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
      println(date)
      val clickCount = adUserClickCountDAO.findClickCountByMultiKey(date,userId.toLong,adId.toLong)
      if (clickCount >= 10){
        true
      }else {
        false
      }
    })

    // 黑名单用户名列表DS
    val blackUserIdListDS: DStream[String] = blackListDS.map(x => {
      val userId = x._1._2

      userId
    })

    // 当前batch去重后的黑名单用户名列表DS（只能去重当前批次）
    val distblackUserIdListDS: DStream[String] = blackUserIdListDS.transform(rdd => {
      rdd.distinct()
    })

    // 将黑名单用户写入mySql
    distblackUserIdListDS.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        partition.foreach(x => {
          val adBlacklistDAO = DAOFactory.getAdBlacklistDAO
          val blacklists: util.List[AdBlacklist] = adBlacklistDAO.findAll()
          val it = blacklists.iterator()
          val adBlackList = new util.ArrayList[AdBlacklist]()
          val adBlack: AdBlacklist = new AdBlacklist
          val userId = x
          adBlack.setUserid(userId.toLong)
          var flag = 0 // 标记 0表示数据库中没有这个黑名单用户ID，需要插入；1表示数据库中已存在，不需插入
          import scala.util.control.Breaks._
          breakable{
            while (it.hasNext){
              if (adBlack.getUserid == it.next().getUserid){
                flag = 1
                break
              }
            }
          }
          if (flag == 0){
            adBlackList.add(adBlack)
            adBlacklistDAO.insertBatch(adBlackList)
          }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1,x._2.sum + x._3.getOrElse(0))
    })
  }
}
