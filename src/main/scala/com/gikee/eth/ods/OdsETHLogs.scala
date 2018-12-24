package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 每天更新 ODS ETH Logs by lucas 20181124
  */
object OdsETHLogs {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.odsETHLogs.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.odsETHLogs.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHLogs.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHLogs.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.odsETHLogs.transactionDate")

    getOdsETHLogs(spark)

    spark.stop()

  }

  def getOdsETHLogs(spark: SparkSession): Unit = {

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
      .select("logs_address", "logs_topics", "logs_topics_one", "logs_data", "block_number", "block_hash",
        "logs_transaction_index", "logs_transaction_hash", "logs_index", "logs_removed", "logs_id", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
