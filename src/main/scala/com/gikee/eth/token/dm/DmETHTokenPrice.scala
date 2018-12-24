package com.gikee.eth.token.dm

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.SparkSession

/**
  * 每天所有 token 价格获取
  */
object DmETHTokenPrice {

  var mysqlDataBase, mysqlTableName, writeDataBase, writeTableName, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    mysqlDataBase = sc.getConf.get("spark.dmETHTokenPrice.mysqlDataBase")
    mysqlTableName = sc.getConf.get("spark.dmETHTokenPrice.mysqlTableName")
    writeDataBase = sc.getConf.get("spark.dmETHTokenPrice.writeDatabase")
    writeTableName = sc.getConf.get("spark.dmETHTokenPrice.writeTableName")
    dateTime = sc.getConf.get("spark.dmETHTokenPrice.dateTime")

    getDmETHTokenPrice(spark)

    spark.stop()

  }

  def getDmETHTokenPrice(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    import spark.implicits._

    val targetDF = spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://106.14.200.2:3306/${mysqlDataBase}",
        "driver" -> "com.mysql.jdbc.Driver",
        //"dbtable" -> s"${mysqlTableName}",
        "dbtable" -> s"(select id, symbol, priceUs, time from ${mysqlTableName} where time = '${dateTime}' and priceUs > '0.0' ) as coinEverydayInfo",
        "user" -> "lyjm_data",
        "password" -> "lyjm_python"
      )).load().rdd.map(x => {
      val id = x.get(0).toString
      val token_symbol = x.get(1).toString
      val price_us = BigDecimal(x.get(2).toString).bigDecimal.toPlainString
      val transaction_date = x.get(3).toString
      (id, token_symbol, price_us, transaction_date)
    }).toDF("id", "token_symbol", "price_us", "transaction_date")
    //.where(" price_us > '0.0' ")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")

    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
