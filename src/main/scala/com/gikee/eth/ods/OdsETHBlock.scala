package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 每天更新 ODS ETH Block by lucas 20181124
  */
object OdsETHBlock {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.odsETHBlock.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.odsETHBlock.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHBlock.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHBlock.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.odsETHBlock.transactionDate")

    getOdsETHBlock(spark)

    spark.stop()

  }

  def getOdsETHBlock(spark: SparkSession): Unit = {

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
      .select("difficulty", "extra_data", "gas_limit", "gas_used", "block_hash", "logs_bloom", "miner", "mix_hash", "nonce",
        "parent_hash", "receipts_root", "sha3_uncles", "blocks_size", "state_root", "total_difficulty", "transactions_root", "block_number",
        "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
