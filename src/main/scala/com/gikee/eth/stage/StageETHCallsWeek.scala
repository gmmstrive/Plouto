package com.gikee.eth.stage

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, ParsingJson, TableUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * eth calls info by lucas 20181122
  */
object StageETHCallsWeek {

  var readStageDataBase, readStageTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readStageDataBase = spark.sparkContext.getConf.get("spark.stageETHCallsWeek.readStageDataBase")
    readStageTableName = spark.sparkContext.getConf.get("spark.stageETHCallsWeek.readStageTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.stageETHCallsWeek.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.stageETHCallsWeek.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.stageETHCallsWeek.transactionDate")

    getStageETHCallsWeek(spark)

    spark.stop()

  }

  def getStageETHCallsWeek(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)
    var tempDF: DataFrame = null

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    if (transactionDate != "") {
      val beforeDate = DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -1)
      tempDF = spark.read.table(s"${readStageDataBase}.${readStageTableName}").where(s" transaction_date >= '${beforeDate}' ")
    } else {
      tempDF = spark.read.table(s"${readStageDataBase}.${readStageTableName}")
    }


    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val info = x.get(0).toString
      val block_number = x.get(1).toString
      val dh = x.get(2).toString
      val date_time = x.get(3).toString
      val transaction_date = x.get(4).toString
      val infoJson = JSON.parseObject(info)
      val transactions = ParsingJson.getStrArray(infoJson, "transactions")
      if (transactions != null) {
        for (i <- 0 until transactions.size()) {
          transactions.getJSONObject(i).put("block_number", block_number)
          transactions.getJSONObject(i).put("dh", dh)
          transactions.getJSONObject(i).put("date_time", date_time)
          transactions.getJSONObject(i).put("transaction_date", transaction_date)
        }
      }
      transactions
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val trace = x.getJSONObject("trace")
      val base = x.getJSONObject("base")
      val calls_transaction_index = ParsingJson.getStrTrim(base, "transactionIndex")
      val calls_transaction_hash = ParsingJson.getStrTrim(base, "hash")
      val block_number = ParsingJson.getStrTrim(x, "block_number")
      val dh = ParsingJson.getStrTrim(x, "dh")
      val date_time = ParsingJson.getStrTrim(x, "date_time")
      val transaction_date = ParsingJson.getStrTrim(x, "transaction_date")
      val callsArray = ParsingJson.getStrArray(trace, "calls")

      def getAllCallsArray(jsonArray: JSONArray, calls_parent_id: String): Unit = {
        if (jsonArray != null) {
          for (i <- 0 until jsonArray.size) {
            val calls_element = jsonArray.getJSONObject(i)
            val calls_id = s"${if (calls_element.getString("from") != null) calls_element.getString("from") else CommonConstant.getUUID()}_${CommonConstant.getUUID()}"
            calls_element.put("calls_id", calls_id)
            calls_element.put("calls_index", i)
            calls_element.put("calls_transaction_index", calls_transaction_index)
            calls_element.put("calls_transaction_hash", calls_transaction_hash)
            calls_element.put("block_number", block_number)
            calls_element.put("dh", dh)
            calls_element.put("date_time", date_time)
            calls_element.put("transaction_date", transaction_date)
            if (calls_parent_id == "") {
              calls_element.put("calls_parent_id", calls_id)
            } else {
              calls_element.put("calls_parent_id", calls_parent_id)
              callsArray.add(calls_element) // 把子节点的数组，追加到父节点数组，后续不解析子数组，把所有 calls 拉平处理
            }
            val calls = ParsingJson.getStrArray(calls_element, "calls")
            if (calls != null) {
              getAllCallsArray(calls, calls_id)
            }
          }
        }
      }

      if (callsArray != null) {
        getAllCallsArray(callsArray, "")
      }
      callsArray
    }).filter(_ != null).flatMap(_.toArray).map(_.asInstanceOf[JSONObject]).map(x => {
      val calls_id = ParsingJson.getStrTrim(x, "calls_id").toLowerCase
      val calls_parent_id = ParsingJson.getStrTrim(x, "calls_parent_id").toLowerCase
      val calls_index = ParsingJson.getStrTrim(x, "calls_index").toLowerCase
      val calls_type = ParsingJson.getStrTrim(x, "type").toLowerCase
      val calls_from = ParsingJson.getStrTrim(x, "from").toLowerCase
      val calls_gas = ParsingJson.getStrTrim(x, "gas").toLowerCase
      val calls_gas_used = ParsingJson.getStrTrim(x, "gasUsed").toLowerCase
      val calls_to = ParsingJson.getStrTrim(x, "to").toLowerCase
      val calls_value = ParsingJson.getStrTrim(x, "value").toLowerCase
      val calls_input = ParsingJson.getStrTrim(x, "input").toLowerCase
      val calls_output = ParsingJson.getStrTrim(x, "output").toLowerCase
      val calls_error = ParsingJson.getStrTrim(x, "error").toLowerCase
      val block_number = ParsingJson.getStrTrim(x, "block_number").toLowerCase
      val calls_transaction_index = ParsingJson.getStrTrim(x, "calls_transaction_index").toLowerCase
      val calls_transaction_hash = ParsingJson.getStrTrim(x, "calls_transaction_hash").toLowerCase
      val dh = ParsingJson.getStrTrim(x, "dh")
      val date_time = ParsingJson.getStrTrim(x, "date_time")
      val transaction_date = ParsingJson.getStrTrim(x, "transaction_date")
      (calls_id, calls_parent_id, calls_index, calls_type, calls_from, calls_gas, calls_gas_used, calls_to, calls_value, calls_input, calls_output, calls_error,
        block_number, calls_transaction_index, calls_transaction_hash, dh, date_time, transaction_date)
    }).toDF("calls_id", "calls_parent_id", "calls_index", "calls_type", "calls_from", "calls_gas", "calls_gas_used", "calls_to", "calls_value",
      "calls_input", "calls_output", "calls_error", "block_number", "calls_transaction_index", "calls_transaction_hash", "dh", "date_time", "transaction_date")
    //.where(" calls_from not in ('0xa43ebd8939d8328f5858119a3fb65f65c864c6dd','0x0e879ae28cdddeb31a405a9db354505a5560b0bd','0x5974e295986768967edfd8fbb66d4ad47f174029') ")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
