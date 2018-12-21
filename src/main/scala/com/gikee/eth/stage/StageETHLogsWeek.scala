package com.gikee.eth.stage

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{ParsingJson, TableUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * eth log info by lucas 20181122
  */
object StageETHLogs {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.stageETHLogs.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.stageETHLogs.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.stageETHLogs.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.stageETHLogs.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.stageETHLogs.transactionDate")

    getStageETHLogs(spark)

    spark.stop()

  }

  def getStageETHLogs(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)
    var tempDF: DataFrame = null

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    if (transactionDate != "") {
      tempDF = spark.read.table(s"${readStageDataBase}.${readStageTableName}").where(s" transaction_date = '${transactionDate}' ")
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
      val transactions = ParsingJson.getStrArray(infoJson, "transactions")
      if (transactions != null) {
        for (i <- 0 until transactions.size()) {
          transactions.getJSONObject(i).put("block_number", block_number)
          transactions.getJSONObject(i).put("date_time", date_time)
          transactions.getJSONObject(i).put("dh", dh)
          transactions.getJSONObject(i).put("transaction_date", transaction_date)
        }
      }
      transactions
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val receipt = x.getJSONObject("receipt")
      val transaction_date = ParsingJson.getStrTrim(x, "transaction_date")
      val date_time = ParsingJson.getStrTrim(x, "date_time")
      val logs = ParsingJson.getStrArray(receipt, "logs")

      if (logs != null) {
        for (i <- 0 until logs.size()) {
          logs.getJSONObject(i).put("transaction_date", transaction_date)
          logs.getJSONObject(i).put("date_time", date_time)
        }
      }
      logs
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val logs_address = ParsingJson.getStrTrim(x, "address").toLowerCase
      val topicsArray = ParsingJson.getStrArray(x, "topics")
      var logs_topics, logs_topics_one: String = ""
      if (topicsArray != null) {
        logs_topics = topicsArray.toJSONString.toLowerCase
        logs_topics_one = topicsArray.get(0).toString.toLowerCase
      }
      val logs_data = ParsingJson.getStrTrim(x, "data").toLowerCase
      val block_number = ParsingJson.getStrTrim(x, "blockNumber").toLowerCase
      val block_hash = ParsingJson.getStrTrim(x, "blockHash").toLowerCase
      val logs_transaction_hash = ParsingJson.getStrTrim(x, "transactionHash").toLowerCase
      val logs_transaction_index = ParsingJson.getStrTrim(x, "transactionIndex").toLowerCase
      val logs_index = ParsingJson.getStrTrim(x, "logIndex").toLowerCase
      val logs_removed = ParsingJson.getStrTrim(x, "removed").toLowerCase
      val logs_id = ParsingJson.getStrTrim(x, "id").toLowerCase
      val dh = ParsingJson.getStrTrim(x, "dh")
      val date_time = ParsingJson.getStrTrim(x, "date_time")
      val transaction_date = ParsingJson.getStrTrim(x, "transaction_date")
      (logs_address, logs_topics, logs_topics_one, logs_data, block_number, block_hash,
        logs_transaction_index, logs_transaction_hash, logs_index, logs_removed, logs_id, dh, date_time, transaction_date)
    }).toDF("logs_address", "logs_topics", "logs_topics_one", "logs_data", "block_number", "block_hash",
      "logs_transaction_index", "logs_transaction_hash", "logs_index", "logs_removed", "logs_id", "dh", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
