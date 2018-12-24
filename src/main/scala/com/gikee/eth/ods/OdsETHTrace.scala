package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 每天更新 ODS ETH Trace by lucas 20181124
  */
object OdsETHTrace {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.odsETHTrace.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.odsETHTrace.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHTrace.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHTrace.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.odsETHTrace.transactionDate")

    getOdsETHTrace(spark)

    spark.stop()

  }

  def getOdsETHTrace(spark: SparkSession): Unit = {

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
      .select("trace_type", "trace_from", "trace_to", "trace_value", "trace_gas", "trace_gas_used", "trace_input", "trace_output",
        "trace_time", "trace_error", "block_number", "trace_transaction_index", "trace_transaction_hash", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
