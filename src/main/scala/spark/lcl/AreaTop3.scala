package spark.lcl

import java.util

import com.alibaba.fastjson.JSON
import com.qf.sessionanalyze.dao.factory.DAOFactory
import com.qf.sessionanalyze.domain.AreaTop3Product
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkContext

import scala.collection.mutable

object AreaTop3 {

  def getAreaTop3(sessionId2ActionRDD: RDD[(String, Row)],taskId:Long, sc:SparkContext, sparkSession: SparkSession): Unit = {

    //|date|user_id|session_id|page_id|action_time|search_keyword|click_category_id|click_product_id|order_category_ids|order_product_ids|pay_category_ids|pay_product_ids|city_id|
    //(b6ff5245c09048d891bf5d240dd4dd8d,[2018-12-14,82,b6ff5245c09048d891bf5d240dd4dd8d,8,2018-12-14 6:20:00,呷哺呷哺,null,null,null,null,null,null,2])
    //1.查询task，获取日期范围，通过SparkSQL查询user_visit_action表中的指定日期范围内的数据，
    // 过滤出商品点击行为，click_product_id，此时得到的数据为：city_id, click_product_id
    val cityAndProduct = sessionId2ActionRDD.filter(_._2.getAs[Long]("click_product_id")!=0L).map(tuple=>{
      val row = tuple._2
      val city_id = row.getAs[Long]("city_id").toInt
      val click_product_id = row.getAs[Long]("click_product_id")
      (city_id,click_product_id)
    })
    //2.使用SparkSQL从MySQL中查询出来城市信息（city_id,city_name,area），
    //用户访问行为数据要跟城市信息进行join，
    //生成 的RDD的数据字段有：city_id,city_name,area,product_id,
    //转换成DataFrame，注册成一张临时表
    val cityInfo: RDD[(Int, (String, String))] = MysqlUtil.getMySqlInfo(sc,"city_info").rdd.map(row=>{
      val city_id = row.getAs[Int]("city_id")
      val city_name = row.getAs[String]("city_name")
      val area = row.getAs[String]("area")
      (city_id, (city_name, area))
    })

    import sparkSession.implicits._
    val cityAndAreaAndProduct: RDD[AreaAndProduct] = cityInfo.join(cityAndProduct).map(row=>{
      val city_id = row._1
      val city_name = row._2._1._1
      val area = row._2._1._2
      val product_id = row._2._2
      AreaAndProduct(city_id, city_name, area, product_id)
    })

    //cityinfos
    var areaMap =new mutable.HashMap[String, String]()
    val areaCity: Seq[((Long, String), String)] = cityAndAreaAndProduct.map(row=>{
      ((row.product_id, row.area), row.city_name)
    }).collect.toList
    for(item <- areaCity){
      var value = areaMap.getOrElse(item._1._1+item._1._2, "")
      if(value.equals("")){
        areaMap.put(item._1._1+item._1._2, item._2)
      }else if(!value.contains(item._2)){
        value = value+","+item._2
        areaMap.put(item._1._1+item._1._2, value)
      }
    }
    val areaAndCityMap = sc.broadcast(areaMap)

    //3、SparkSQL内置函数，对area打标记
    val cityAndAreaAndProductDF = cityAndAreaAndProduct.toDF()
    cityAndAreaAndProductDF.createOrReplaceTempView("table")
    val newCityAndAreaAndProductDF = sparkSession.sql(
      """
        |select city_id, city_name,area,
        |case area when "华东" then "A" when "华中" then "B" when "东北" then "C"
        |when "西北" then "D" when "华北" then "E" when "西南" then "F"
        |when "华南" then "G" end as area_leval,
        |product_id
        |from table
      """.stripMargin)
    newCityAndAreaAndProductDF.show(5)
    //4、计算出来每个区域下每个商品的点击次数，此时用到的技术点有自定义UDF
    newCityAndAreaAndProductDF.createOrReplaceTempView("area_table")
    val areaAndProduct = sparkSession.sql(
      """
        |select area, area_leval, product_id, click_count,
        |row_number() over (partition by area_leval order by click_count desc) as ranknum
        |from (
        |select area, area_leval, product_id, count(product_id) as click_count
        |from area_table
        |group by area_leval, area, product_id)
      """.stripMargin)
      areaAndProduct.show(20)

    areaAndProduct.createOrReplaceTempView("area_Product_table")
    val sortTop3 = sparkSession.sql(
      """
        |select area, area_leval, product_id, click_count, ranknum
        |from area_Product_table
        |where ranknum<4
      """.stripMargin)
    sortTop3.show(6)
    //5、join商品明细表，自定义UDF、自定义UDAF
    //用if()函数（SparkSQL内置函数）,判断商品属于自营还是第三方
    val productInfoDF = getProduct_info(sparkSession)
    val newProductInfoDF: RDD[ProductInfoClass] = productInfoDF.rdd.map(row=>{
      val product_id = row.getAs[Long]("product_id")
      val product_name = row.getAs[String]("product_name")
      val stateStr = row.getAs[String]("extend_info")
      val state = JSON.parseObject(stateStr).getInteger("product_status")
      ProductInfoClass(product_id, product_name, state)
    })
    newProductInfoDF.toDF().createOrReplaceTempView("new_product_table")
    sortTop3.toDF().createOrReplaceTempView("sortTop3_table")
    newProductInfoDF.take(3).foreach(println)

    var arrTop = new util.ArrayList[AreaTop3Product]()
    sparkSession.sql(
      """
        |select area, a.area_leval, a.product_id, a.click_count, n.product_name, n.state
        |from sortTop3_table as a
        |left join new_product_table as n
        |on a.product_id = n.product_id
      """.stripMargin).rdd.foreachPartition(partition=> {
      partition.foreach(row => {
        val oneTop = new AreaTop3Product
        oneTop.setTaskid(taskId)
        oneTop.setArea(row.getAs[String]("area"))
        oneTop.setAreaLevel(row.getAs[String]("area_leval"))
        oneTop.setProductid(row.getAs[Long]("product_id"))
        oneTop.setClickCount(row.getAs[Long]("click_count"))
        oneTop.setProductName(row.getAs[String]("product_name"))
        oneTop.setProductStatus(row.getAs[Int]("state").toString)
        val broat = areaAndCityMap.value
        val areaproductid = row.getAs[Long]("product_id")+row.getAs[String]("area")
        val cityInfo= broat.getOrElse(areaproductid, "未知")
        oneTop.setCityInfos(cityInfo)
        arrTop.add(oneTop)
      })
      val areatop3 = DAOFactory.getAreaTop3ProductDAO
      areatop3.insertBatch(arrTop)
    })
  }
  def getProduct_info(sparkSession: SparkSession): DataFrame ={
    //写sql，过滤满足条件的数据
    val sqlstr = "select * from product_info"
    val actionDF = sparkSession.sql(sqlstr)
    actionDF
  }
}
case class AreaAndProduct(city_id:Long, city_name:String, area:String, product_id:Long)
case class ProductInfoClass(product_id:Long, product_name:String, state:Int)