package spark.lcl

import com.qf.sessionanalyze.conf.ConfigurationManager
import com.qf.sessionanalyze.constant.Constants
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object MysqlUtil {
    def getMySqlInfo(sc:SparkContext, tableName:String): DataFrame = {
      val sqlContext = new SQLContext(sc)

      val url = ConfigurationManager.getProperty(Constants.JDBC_URL)

      // 设置连接用户&密码
      val prop = new java.util.Properties
      prop.setProperty("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
      prop.setProperty("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))

      val df: DataFrame = sqlContext.read.jdbc(url, tableName, prop)
      df
    }
}
