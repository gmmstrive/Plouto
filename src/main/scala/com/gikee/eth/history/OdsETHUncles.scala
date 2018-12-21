package com.gikee.eth.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{ParsingJson, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * eth uncles info by lucas 20181114
  */
object OdsETHUncles {

  var readOdsDataBase, readOdsTableName, writeDataBase, writeTableName, dateMonthly, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDataBase = spark.sparkContext.getConf.get("spark.odsETHUncles.readOdsDataBase")
    readOdsTableName = spark.sparkContext.getConf.get("spark.odsETHUncles.readOdsTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHUncles.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHUncles.writeTableName")
    dateMonthly = spark.sparkContext.getConf.get("spark.odsETHUncles.transactionMonthly")
    dateTime = spark.sparkContext.getConf.get("spark.odsETHUncles.transactionDate")

    getOdsETHUncles(spark)

    spark.stop()

  }

  def getOdsETHUncles(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val query_sql = if (dateMonthly != "") s" transaction_date rlike '${dateMonthly}' " else s" transaction_date = '${dateTime}' "

    import spark.implicits._

    val targetDF = spark.read.table(s"${readOdsDataBase}.${readOdsTableName}").where(query_sql).rdd.map(x => {
      val info = x.get(0).toString
      val block_number = x.get(1).toString
      val date_time = x.get(2).toString
      val transaction_date = x.get(3).toString
      val infoJson = JSON.parseObject(info)
      val uncles = ParsingJson.getStrArray(infoJson, "uncles")
      if (uncles != null) {
        for (i <- 0 until uncles.size()) {
          uncles.getJSONObject(i).put("block_number", block_number)
          uncles.getJSONObject(i).put("date_time", date_time)
          uncles.getJSONObject(i).put("transaction_date", transaction_date)
        }
      }
      uncles
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val unclesJson = JSON.parseObject(x.toString)
      val block_number = ParsingJson.getStrTrim(unclesJson, "block_number").toLowerCase
      val uncles_hash = ParsingJson.getStrTrim(unclesJson, "hash").toLowerCase
      val uncles_miner = ParsingJson.getStrTrim(unclesJson, "miner").toLowerCase
      val uncles_number = ParsingJson.getStrTrim(unclesJson, "number").toLowerCase
      val uncles_time = ParsingJson.getStrDate(unclesJson, "time")._1
      val date_time = ParsingJson.getStrTrim(unclesJson, "date_time")
      val transaction_date = ParsingJson.getStrTrim(unclesJson, "transaction_date")
      (block_number, uncles_hash, uncles_miner, uncles_number, uncles_time, date_time, transaction_date)
    }).filter(_._2 != "")
      .toDF("block_number", "uncles_hash", "uncles_miner", "uncles_number", "uncles_time", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
