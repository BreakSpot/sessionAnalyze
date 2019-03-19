package spark.lcl.onTimeAnalysis

import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.util.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import spark.lcl.onTimeAnalysis.AdBlackList.addAdBlackList
import spark.lcl.onTimeAnalysis.FilterMessages.getFilterMessages
import spark.lcl.onTimeAnalysis.AdStatSql.deteCityAdCount
import spark.lcl.onTimeAnalysis.StatAdProcinceTop3.getStatAdProcinceTop3
import spark.lcl.onTimeAnalysis.AdClickTrend.getAdClickTrend

object MainAdUserInfo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ScalaKafkaStream").setMaster("local")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder()
      .appName(Constants.SPARK_APP_NAME_SESSION)
      .getOrCreate()
    val ssc = new StreamingContext(sc, Seconds(5))

      ssc.checkpoint("F:/bigdata/kafka")

    val bootstrapServers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"
    val topicsSet = Set(Constants.KAFKA_TOPICS)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> bootstrapServers,
      "group.id" -> "kafkaGroup"
    )
    val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    //添加黑名单
    addAdBlackList(messages)

    //基于黑名单的非法广告点击流量过滤机制(Tue Dec 18 18:57:35 CST 2018,Hubei,Wuhan,30,0)
    val currentMessages = getFilterMessages(messages, sc)

    //每天各省各城市各广告的点击流量实时统计
    val dateCityMess = currentMessages.map(tuple=>{
      val data = DateUtils.formatDate(tuple._1)
      val province = tuple._2
      val city = tuple._3
      val adId = tuple._5
      ((data, province, city, adId), 1)
    })

    val adStateList = dateCityMess.updateStateByKey(fun, new HashPartitioner(3), true)

    //添加到数据库
    deteCityAdCount(adStateList)

    //统计每天各省top3热门广告((2018-12-18,Hubei,Jingzhou,4),5)
    getStatAdProcinceTop3(adStateList, sparkSession)

    //最近1小时内各分钟的点击量
    getAdClickTrend(currentMessages)

    ssc.start()
    ssc.awaitTermination()
  }

  val fun = (it: (Iterator[((String, String, String, String), Seq[Int], Option[Int])] )) => {
    it.map(x => {
      ((x._1._1, x._1._2, x._1._3, x._1._4), x._2.sum + x._3.getOrElse(0))
    })
  }
}
