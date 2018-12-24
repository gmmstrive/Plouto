package com.gikee.eth.token.dwd

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * token transaction info by lucas 20181124
  */
object DwdETHTokenTransaction {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()


    readStageDataBase = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.transactionDate")

    getDwdETHTokenTransaction(spark)

    spark.stop()

  }

  def getDwdETHTokenTransaction(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)
    val beforeDate = DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -3)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val targetDF = spark.read.table(s"${readStageDataBase}.${readStageTableName}")
      .where(s" transaction_date >= '${beforeDate}' ")
      .select("block_number", "from_address", "to_address", "value", "price_us", "amount", "logs_transaction_index",
        "logs_transaction_hash", "date_time", "token_id", "token_symbol", "token_address", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
