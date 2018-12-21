package com.gikee.eth.ods

import com.alibaba.fastjson.JSON
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{ParsingJson, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * eth block info by lucas 20181114
  */
object OdsETHBlock {

  var readOdsDataBase, readOdsTableName, writeDataBase, writeTableName, dateMonthly, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDataBase = spark.sparkContext.getConf.get("spark.odsETHBlock.readOdsDataBase")
    readOdsTableName = spark.sparkContext.getConf.get("spark.odsETHBlock.readOdsTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHBlock.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHBlock.writeTableName")
    dateMonthly = spark.sparkContext.getConf.get("spark.odsETHBlock.transactionMonthly")
    dateTime = spark.sparkContext.getConf.get("spark.odsETHBlock.transactionDate")

    getOdsETHBlock(spark)

    spark.stop()

  }

  def getOdsETHBlock(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val query_sql = if (dateMonthly != "") s" transaction_date rlike '${dateMonthly}' " else s" transaction_date = '${dateTime}' "

    import spark.implicits._

    val targetDF = spark.read.table(s"${readOdsDataBase}.${readOdsTableName}").where(query_sql).rdd.map(x => {
      val info = x.get(0).toString
      val infoJson = JSON.parseObject(info)
      val difficulty = ParsingJson.getStrTrim(infoJson, "difficulty").toLowerCase
      val extra_data = ParsingJson.getStrTrim(infoJson, "extraData").toLowerCase
      val gas_limit = ParsingJson.getStrTrim(infoJson, "gasLimit").toLowerCase
      val gas_used = ParsingJson.getStrTrim(infoJson, "gasUsed").toLowerCase
      val block_hash = ParsingJson.getStrTrim(infoJson, "hash").toLowerCase
      val logs_bloom = ParsingJson.getStrTrim(infoJson, "logsBloom").toLowerCase
      val miner = ParsingJson.getStrTrim(infoJson, "miner").toLowerCase
      val mix_hash = ParsingJson.getStrTrim(infoJson, "mixHash").toLowerCase
      val nonce = ParsingJson.getStrTrim(infoJson, "nonce").toLowerCase
      val parent_hash = ParsingJson.getStrTrim(infoJson, "parentHash").toLowerCase
      val receipts_root = ParsingJson.getStrTrim(infoJson, "receiptsRoot").toLowerCase
      val sha3_uncles = ParsingJson.getStrTrim(infoJson, "sha3Uncles").toLowerCase
      val blocks_size = ParsingJson.getStrTrim(infoJson, "size").toLowerCase
      val state_root = ParsingJson.getStrTrim(infoJson, "stateRoot").toLowerCase
      val total_difficulty = ParsingJson.getStrTrim(infoJson, "totalDifficulty").toLowerCase
      val transactions_root = ParsingJson.getStrTrim(infoJson, "transactionsRoot").toLowerCase
      val block_number = x.get(1).toString
      val date_time = x.get(2).toString
      val transaction_date = x.get(3).toString
      (difficulty, extra_data, gas_limit, gas_used, block_hash, logs_bloom, miner, mix_hash, nonce, parent_hash, receipts_root, sha3_uncles,
        blocks_size, state_root, total_difficulty, transactions_root, block_number, date_time, transaction_date)
    }).toDF("difficulty", "extra_data", "gas_limit", "gas_used", "block_hash", "logs_bloom", "miner", "mix_hash", "nonce", "parent_hash", "receipts_root", "sha3_uncles",
      "blocks_size", "state_root", "total_difficulty", "transactions_root", "block_number", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
