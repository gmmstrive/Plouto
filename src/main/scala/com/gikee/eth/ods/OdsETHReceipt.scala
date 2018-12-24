package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 每天更新 ODS ETH Receipt by lucas 20181124
  */
object OdsETHReceipt {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.odsETHReceipt.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.odsETHReceipt.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHReceipt.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHReceipt.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.odsETHReceipt.transactionDate")

    getOdsETHReceipt(spark)

    spark.stop()

  }

  def getOdsETHReceipt(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)
    val beforeDate = DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -2)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val targetDF = spark.read.table(s"${readStageDataBase}.${readStageTableName}")
      .where(s" transaction_date >= '${beforeDate}' ")
      .select("block_hash", "block_number", "receipt_contract_address", "receipt_cumulative_gas_used", "receipt_from",
        "receipt_gas_used", "receipt_logs_bloom", "receipt_root", "receipt_status", "receipt_to", "receipt_transaction_index",
        "receipt_transaction_hash", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
