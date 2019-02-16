package com.gikee.eth.dwd

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 以太币交易表 by lucas 20181210
  */
object DwdETHTransaction {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()


    readStageDataBase = spark.sparkContext.getConf.get("spark.dwdETHTransaction.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.dwdETHTransaction.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwdETHTransaction.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwdETHTransaction.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwdETHTransaction.transactionDate")

    getDwdETHTransaction(spark)

    spark.stop()

  }

  def getDwdETHTransaction(spark: SparkSession): Unit = {

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
      .select("block_number", "from_address", "to_address", "value", "price_us", "amount",
        "transaction_index", "transaction_hash", "date_time", "transaction_type", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
