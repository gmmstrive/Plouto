package com.gikee.eth.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{ParsingJson, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  *  eth trace info by lucas 20181114
  */
object OdsETHTrace {

  var readOdsDataBase, readOdsTableName, writeDataBase, writeTableName,dateMonthly, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDataBase = spark.sparkContext.getConf.get("spark.odsETHTrace.readOdsDataBase")
    readOdsTableName = spark.sparkContext.getConf.get("spark.odsETHTrace.readOdsTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.odsETHTrace.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.odsETHTrace.writeTableName")
    dateMonthly = spark.sparkContext.getConf.get("spark.odsETHTrace.transactionMonthly")
    dateTime = spark.sparkContext.getConf.get("spark.odsETHTrace.transactionDate")

    getOdsETHTrace(spark)

    spark.stop()

  }

  def getOdsETHTrace(spark: SparkSession): Unit = {

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
      val transactions = ParsingJson.getStrArray(infoJson, "transactions")
      if (transactions != null) {
        for (i <- 0 until transactions.size()) {
          transactions.getJSONObject(i).put("block_number", block_number)
          transactions.getJSONObject(i).put("date_time", date_time)
          transactions.getJSONObject(i).put("transaction_date", transaction_date)
        }
      }
      transactions
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val trace = x.getJSONObject("trace")
      val base = x.getJSONObject("base")
      val trace_type = ParsingJson.getStrTrim(trace, "type").toLowerCase
      val trace_from = ParsingJson.getStrTrim(trace, "from").toLowerCase
      val trace_to = ParsingJson.getStrTrim(trace, "to").toLowerCase
      val trace_value = ParsingJson.getStrTrim(trace, "value").toLowerCase
      val trace_gas = ParsingJson.getStrTrim(trace, "gas").toLowerCase
      val trace_gas_used = ParsingJson.getStrTrim(trace, "gasUsed").toLowerCase
      val trace_input = ParsingJson.getStrTrim(trace, "input").toLowerCase
      val trace_output = ParsingJson.getStrTrim(trace, "output").toLowerCase
      val trace_time = ParsingJson.getStrTrim(trace, "time").toLowerCase
      val trace_error = ParsingJson.getStrTrim(trace, "error").toLowerCase
      val block_number = ParsingJson.getStrTrim(x, "block_number").toLowerCase
      val trace_transaction_index = ParsingJson.getStrTrim(base, "transactionIndex").toLowerCase
      val trace_transaction_hash = ParsingJson.getStrTrim(base, "hash").toLowerCase
      val date_time = ParsingJson.getStrTrim(x, "date_time")
      val transaction_date = ParsingJson.getStrTrim(x, "transaction_date")
      (trace_type, trace_from, trace_to, trace_value, trace_gas, trace_gas_used, trace_input, trace_output, trace_time, trace_error, block_number,
        trace_transaction_index, trace_transaction_hash, date_time, transaction_date)
    }).toDF("trace_type", "trace_from", "trace_to", "trace_value", "trace_gas", "trace_gas_used", "trace_input", "trace_output", "trace_time",
      "trace_error", "block_number", "trace_transaction_index", "trace_transaction_hash", "date_time", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
