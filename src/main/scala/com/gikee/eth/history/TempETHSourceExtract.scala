package com.gikee.eth.ods

import com.alibaba.fastjson.JSON
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{ParsingJson, TableUtil}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 从 eth 原始文件中提取日期，
  * 因为原始文件是按照块号分目录并没有按照日期，hive 分区要按照日期插入
  * 所以多了一步刷新日期操作
  */
object TempETHSourceExtract {

  var readTempDataBase, readTempTableName, writeDataBase, writeTableName, partitionDir: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readTempDataBase = spark.sparkContext.getConf.get("spark.tempETHSourceExtract.readTempDataBase")
    readTempTableName = spark.sparkContext.getConf.get("spark.tempETHSourceExtract.readTempTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.tempETHSourceExtract.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.tempETHSourceExtract.writeTableName")
    partitionDir = spark.sparkContext.getConf.get("spark.tempETHSourceExtract.partitionDir")

    getTempETHSourceExtract(spark)

    spark.stop()

  }

  def getTempETHSourceExtract(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    import spark.implicits._

    val targetDF = spark.read.table(s"${readTempDataBase}.${readTempTableName}").where(s" dir = '${partitionDir}' ").rdd.map(x => {
      val info = x.get(0).toString
      val dir = x.get(1).toString
      val infoJson = JSON.parseObject(info)
      val block_number = ParsingJson.getStrTrim(infoJson, "number")
      val timeTuple = ParsingJson.getStrDate(infoJson, "timestamp")
      val date_time = timeTuple._1
      val transaction_date = timeTuple._5
      (infoJson.toJSONString, block_number, date_time, dir, transaction_date)
    }).toDF("info", "block_number", "date_time", "dir", "transaction_date").repartition(10)

//    targetDF.repartition(1).write.mode(SaveMode.Append).format("parquet")
//      .insertInto(s"${writeDataBase}.${writeTableName}")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "dir", "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "dir", "transaction_date")

  }

}
