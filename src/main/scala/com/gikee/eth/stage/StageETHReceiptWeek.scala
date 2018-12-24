package com.gikee.eth.stage

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, ParsingJson, TableUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * eth receipt info by lucas 20181122
  */
object StageETHReceiptWeek {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.stageETHReceiptWeek.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.stageETHReceiptWeek.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.stageETHReceiptWeek.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.stageETHReceiptWeek.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.stageETHReceiptWeek.transactionDate")

    getStageETHReceiptWeek(spark)

    spark.stop()

  }

  def getStageETHReceiptWeek(spark: SparkSession): Unit = {

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
      val transactions = ParsingJson.getStrArray(infoJson, "transactions")
      if (transactions != null) {
        for (i <- 0 until transactions.size()) {
          transactions.getJSONObject(i).put("block_number", block_number)
          transactions.getJSONObject(i).put("dh", dh)
          transactions.getJSONObject(i).put("date_time", date_time)
          transactions.getJSONObject(i).put("transaction_date", transaction_date)
        }
      }
      transactions
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val receipt = x.getJSONObject("receipt")
      val block_hash = ParsingJson.getStrTrim(receipt, "blockHash").toLowerCase
      val block_number = ParsingJson.getStrTrim(receipt, "blockNumber").toLowerCase
      val receipt_contract_address = ParsingJson.getStrTrim(receipt, "contractAddress").toLowerCase
      val receipt_cumulative_gas_used = ParsingJson.getStrTrim(receipt, "cumulativeGasUsed").toLowerCase
      val receipt_from = ParsingJson.getStrTrim(receipt, "from").toLowerCase
      val receipt_gas_used = ParsingJson.getStrTrim(receipt, "gasUsed").toLowerCase
      val receipt_logs_bloom = ParsingJson.getStrTrim(receipt, "logsBloom").toLowerCase
      val receipt_root = ParsingJson.getStrTrim(receipt, "root").toLowerCase
      val status = ParsingJson.getStrTrim(receipt, "status").toLowerCase
      var receipt_status = ""
      if (status != "") {
        if (status == "0x0" || status == "false") {
          receipt_status = "false"
        } else {
          receipt_status = "true"
        }
      }
      val receipt_to = ParsingJson.getStrTrim(receipt, "to").toLowerCase
      val receipt_transaction_index = ParsingJson.getStrTrim(receipt, "transactionIndex").toLowerCase
      val receipt_transaction_hash = ParsingJson.getStrTrim(receipt, "transactionHash").toLowerCase
      val dh = ParsingJson.getStrTrim(x, "dh")
      val date_time = ParsingJson.getStrTrim(x, "date_time")
      val transaction_date = ParsingJson.getStrTrim(x, "transaction_date")
      (block_hash, block_number, receipt_contract_address, receipt_cumulative_gas_used, receipt_from, receipt_gas_used, receipt_logs_bloom, receipt_root, receipt_status,
        receipt_to, receipt_transaction_index, receipt_transaction_hash, dh, date_time, transaction_date)
    }).toDF("block_hash", "block_number", "receipt_contract_address", "receipt_cumulative_gas_used", "receipt_from", "receipt_gas_used",
      "receipt_logs_bloom", "receipt_root", "receipt_status", "receipt_to", "receipt_transaction_index", "receipt_transaction_hash", "dh", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
