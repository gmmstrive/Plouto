package com.gikee.eth.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{ParsingJson, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * eth base info by lucas 20181114
  */
object OdsETHBase {

  var readOdsDataBase, readOdsTableName, writeDataBase, writeTableName, dateMonthly, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDataBase = spark.sparkContext.getConf.get("spark.odsETHBase.readOdsDataBase")
    readOdsTableName = spark.sparkContext.getConf.get("spark.odsETHBase.readOdsTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHBase.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHBase.writeTableName")
    dateMonthly = spark.sparkContext.getConf.get("spark.odsETHBase.transactionMonthly")
    dateTime = spark.sparkContext.getConf.get("spark.odsETHBase.transactionDate")

    getOdsETHBase(spark)

    spark.stop()

  }

  def getOdsETHBase(spark: SparkSession): Unit = {

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
      val block_number = x.get(1).toString
      val date_time = x.get(2).toString
      val transaction_date = x.get(3).toString
      val infoJson = JSON.parseObject(info)
      val transactions: JSONArray = ParsingJson.getStrArray(infoJson, "transactions")
      if (transactions != null) {
        for (i <- 0 until transactions.size()) {
          transactions.getJSONObject(i).put("block_number", block_number)
          transactions.getJSONObject(i).put("date_time", date_time)
          transactions.getJSONObject(i).put("transaction_date", transaction_date)
        }
      }
      transactions
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val base = x.getJSONObject("base")
      val receipt = x.getJSONObject("receipt")
      val base_gas_used = ParsingJson.getStrTrim(receipt, "gasUsed").toLowerCase
      val block_hash = ParsingJson.getStrTrim(base, "blockHash").toLowerCase
      val base_from = ParsingJson.getStrTrim(base, "from").toLowerCase
      val base_gas_limit = ParsingJson.getStrTrim(base, "gas").toLowerCase
      val base_gas_price = ParsingJson.getStrTrim(base, "gasPrice").toLowerCase
      val base_transaction_hash = ParsingJson.getStrTrim(base, "hash").toLowerCase
      val base_input = ParsingJson.getStrTrim(base, "input").toLowerCase
      val base_nonce = ParsingJson.getStrTrim(base, "nonce").toLowerCase
      val base_to = ParsingJson.getStrTrim(base, "to").toLowerCase
      val base_transaction_index = ParsingJson.getStrTrim(base, "transactionIndex").toLowerCase
      val base_value = ParsingJson.getStrTrim(base, "value").toLowerCase
      val base_v = ParsingJson.getStrTrim(base, "v").toLowerCase
      val base_r = ParsingJson.getStrTrim(base, "r").toLowerCase
      val base_s = ParsingJson.getStrTrim(base, "s").toLowerCase
      val block_number = ParsingJson.getStrTrim(x, "block_number")
      val date_time = ParsingJson.getStrTrim(x, "date_time")
      val transaction_date = ParsingJson.getStrTrim(x, "transaction_date")

      (block_hash, block_number, base_from, base_gas_limit, base_gas_used, base_gas_price, base_input, base_nonce, base_to,
        base_transaction_index, base_transaction_hash, base_value, base_v, base_r, base_s, date_time, transaction_date)
    }).toDF("block_hash", "block_number", "base_from", "base_gas_limit", "base_gas_used", "base_gas_price", "base_input",
      "base_nonce", "base_to", "base_transaction_index", "base_transaction_hash", "base_value", "base_v", "base_r", "base_s",
      "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }


}
