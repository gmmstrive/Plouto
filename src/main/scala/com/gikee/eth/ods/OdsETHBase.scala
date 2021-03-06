package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 每天更新 ODS ETH Base by lucas 20181124
  */
object OdsETHBase {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.odsETHBase.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.odsETHBase.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHBase.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHBase.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.odsETHBase.transactionDate")

    getOdsETHBase(spark)

    spark.stop()

  }

  def getOdsETHBase(spark: SparkSession): Unit = {

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
      .select("block_hash", "block_number", "base_from", "base_gas_limit", "base_gas_used", "base_gas_price", "base_input",
        "base_nonce", "base_to", "base_transaction_index", "base_transaction_hash", "base_value", "base_v", "base_r", "base_s",
        "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
