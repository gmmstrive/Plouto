package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 每天更新 ODS ETH Calls by lucas 20181124
  */
object OdsETHCalls {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.odsETHCalls.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.odsETHCalls.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHCalls.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHCalls.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.odsETHCalls.transactionDate")

    getOdsETHCalls(spark)

    spark.stop()

  }

  def getOdsETHCalls(spark: SparkSession): Unit = {

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
      .select("calls_id", "calls_parent_id", "calls_index", "calls_type", "calls_from", "calls_gas", "calls_gas_used", "calls_to",
        "calls_value", "calls_input", "calls_output", "calls_error", "block_number", "calls_transaction_index", "calls_transaction_hash",
        "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
