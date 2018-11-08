package com.gikee.blockchain.eth.token

import org.apache.spark.sql.SparkSession

/**
  * 类注释：
  * 测试类，测试本地 spark 程序
  *
  */
object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().master("local[*]").appName("").getOrCreate()

    spark.stop()

  }

  /**
    * 方法注释：
    *    查询 temp.user 条数，如果条数大于 0 返回 1，否则返回 0
    * @param spark
    * @return 0/1
    */
  def getDataBase(spark: SparkSession): String = {
    val tmpDF = spark.sql("select * from temp.user")
    if (tmpDF.count > 0) "1" else "0"
  }

}
