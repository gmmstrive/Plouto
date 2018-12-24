package com.gikee.eth.stage

import com.alibaba.fastjson.JSON
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, ParsingJson, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * ETH 数据的抽取工作, UTC 时间作为分区
  */
object StageETHSourceStream {

  val pathPrefix: String = "/gikee/stage/eth_source_stream"
  var writeDataBase, writeTableName: String = ""

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    writeDataBase = spark.sparkContext.getConf.get("spark.stageETHSourceStream.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.stageETHSourceStream.writeTableName")

    getStageETHSourceStream(spark)

    spark.stop()

  }

  def getStageETHSourceStream(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    val beforeDate = DateTransform.getBeforeDate(DateTransform.getUTCDate(CommonConstant.FormatDay), CommonConstant.FormatDay, -3)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    import spark.implicits._

    val targetDF = spark.read.textFile(s"${pathPrefix}/*/*/*/*.log")
      .map(x => {
        val infoJson = JSON.parseObject(x.toString)
        val block_number = ParsingJson.getStrTrim(infoJson, "number")
        val timeTuple = ParsingJson.getStrDate(infoJson, "timestamp")
        val dh = timeTuple._6
        val date_time = timeTuple._1
        val transaction_date = timeTuple._5
        (infoJson.toJSONString, block_number, dh, date_time, transaction_date)
      }).toDF("info", "block_number", "dh", "date_time", "transaction_date")
      .sortWithinPartitions("transaction_date").where(s" transaction_date >= '${beforeDate}' ")

    TableUtil.writeDataStreams(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
