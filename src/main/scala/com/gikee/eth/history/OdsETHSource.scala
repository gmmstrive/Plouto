package com.gikee.eth.ods

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.SparkSession

/**
  * eth data source by lucas 20181114
  */
object OdsETHSource {

  var readOdsDataBase, readOdsTableName, writeDataBase, writeTableName, dateMonthly, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDataBase = spark.sparkContext.getConf.get("spark.odsETHSource.readOdsDataBase")
    readOdsTableName = spark.sparkContext.getConf.get("spark.odsETHSource.readOdsTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHSource.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHSource.writeTableName")
    dateMonthly = spark.sparkContext.getConf.get("spark.odsETHSource.transactionMonthly")
    dateTime = spark.sparkContext.getConf.get("spark.odsETHSource.transactionDate")

    getOdsETHSource(spark)

    spark.stop()

  }

  def getOdsETHSource(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val query_sql = if (dateMonthly != "") s" transaction_date rlike '${dateMonthly}' " else s" transaction_date = '${dateTime}' "

    val targetDF = spark.read.table(s"${readOdsDataBase}.${readOdsTableName}").where(query_sql)
      .select("info", "block_number", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
