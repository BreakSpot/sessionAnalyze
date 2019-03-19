package spark.lcl

import java.util

import com.qf.sessionanalyze.conf.ConfigurationManager
import com.qf.sessionanalyze.constant.Constants
import com.qf.sessionanalyze.dao.ITop10CategoryDAO
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.Top10Category
import org.apache.hadoop.mapred.TaskID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object CategoryTop10 {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local").setAppName("categoryTop10")
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//
//    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
//    val tableName ="session_detail"
//
//    // 设置连接用户&密码
//    val prop = new java.util.Properties
//    prop.setProperty("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
//    prop.setProperty("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
//
//    val rdd = sqlContext.read.jdbc(url, tableName, prop).rdd.map(row=>{
//      val taskId = row.getInt(0)
//      val click_category_id = row.getInt(6).toString
//      val order_category_ids = row.getString(8)
//      val pay_category_ids = row.getString(10)
//      (click_category_id, order_category_ids, pay_category_ids)
//    })
//    rdd.take(3).foreach(println)
//    sc.stop()
//  }

  def getCategoryTop10(categoryInfo: RDD[(Long, String, String)], taskID: Long, sc:SparkContext): RDD[(Long, (Int, Int, Int))] ={


    val clickCount = categoryInfo.filter(_._1!=0).groupBy(_._1).map(row=>{
      val count = row._2.iterator.size
      (row._1, count)
    })
    val orderCount = categoryInfo.filter(_._2!=null).flatMap(_._2.split(",")).map((_,1)).reduceByKey(_+_).collect.toMap
    val order = sc.broadcast(orderCount)

    val payCount = categoryInfo.filter(_._3!=null).flatMap(_._3.split(",")).map((_,1)).reduceByKey(_+_).collect.toMap
    val pay = sc.broadcast(payCount)

    val joinAllCategroy = clickCount.map(row=>{
      val key = row._1.toString
      val ordernum = order.value.getOrElse(key, 0)
      val paynum = pay.value.getOrElse(key, 0)
      (row._1, row._2, ordernum, paynum)
    })
    val res: RDD[(Long, (Int, Int, Int))] = joinAllCategroy.sortBy(c =>  Category(c._2, c._3, c._4)).map(row=>{
      (row._1,(row._2, row._3, row._4))
    })

    res.take(10).foreach(row=>{
      val category = new Top10Category
      category.setTaskid(taskID)
      category.setCategoryid(row._1.toLong)
      category.setClickCount(row._2._1)
      category.setOrderCount(row._2._2)
      category.setPayCount(row._2._3)
      val sesionCategoryDao = DAOFactory.getTop10CategoryDAO
      sesionCategoryDao.insert(category)
    })
    res
  }


}
case class Category(click: Int, order: Int, pay:Int) extends Ordered[Category] {
  override def compare(that: Category):Int = {
    if (this.click == that.click) {
      if (this.order == that.order){
        return that.pay - this.pay
      }
      return that.order - this.order
    }
    return that.click - this.click
  }
}