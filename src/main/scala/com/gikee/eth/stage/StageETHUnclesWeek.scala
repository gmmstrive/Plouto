package com.gikee.eth.stage

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, ParsingJson, TableUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * eth uncles info by lucas 20181122
  */
object StageETHUnclesWeek {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.stageETHUnclesWeek.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.stageETHUnclesWeek.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.stageETHUnclesWeek.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.stageETHUnclesWeek.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.stageETHUnclesWeek.transactionDate")

    getStageETHUncles(spark)

    spark.stop()

  }

  def getStageETHUncles(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)
    var tempDF: DataFrame = null

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    if (transactionDate != "") {
      val beforeDate = DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -2)
      tempDF = spark.read.table(s"${readStageDataBase}.${readStageTableName}").where(s" transaction_date >= '${beforeDate}' ")
    } else {
      tempDF = spark.read.table(s"${readStageDataBase}.${readStageTableName}")
    }

    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val info = x.get(0).toString
      val block_number = x.get(1).toString
      val dh = x.get(2).toString
      val date_time = x.get(3).toString
      val transaction_date = x.get(4).toString
      val infoJson = JSON.parseObject(info)
      val uncles = ParsingJson.getStrArray(infoJson, "uncles")
      if (uncles != null) {
        for (i <- 0 until uncles.size()) {
          uncles.getJSONObject(i).put("block_number", block_number)
          uncles.getJSONObject(i).put("dh", dh)
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
      val dh = ParsingJson.getStrTrim(unclesJson, "dh")
      val date_time = ParsingJson.getStrTrim(unclesJson, "date_time")
      val transaction_date = ParsingJson.getStrTrim(unclesJson, "transaction_date")
      (block_number, uncles_hash, uncles_miner, uncles_number, uncles_time, dh, date_time, transaction_date)
    }).filter(_._2 != "")
      .toDF("block_number", "uncles_hash", "uncles_miner", "uncles_number", "uncles_time", "dh", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
