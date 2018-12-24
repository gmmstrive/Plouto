package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * 每天更新 ODS ETH Source
  */
object OdsETHSource {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.odsETHSource.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.odsETHSource.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHSource.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHSource.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.odsETHSource.transactionDate")

    getOdsETHSource(spark)

    spark.stop()

  }

  def getOdsETHSource(spark: SparkSession): Unit = {

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
      .select("info","block_number","date_time","transaction_date")

    TableUtil.writeDataStreams(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
