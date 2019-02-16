package com.gikee.eth.history

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, ProcessingString, TableUtil}
import org.apache.spark.sql.SparkSession

/**
  * ETH Token Everyday transaction by lucas 20181124
  */
object DwdETHTokenTransaction {

  var readOdsDatabase, readLogsTableName, readReceiptTableName, readTraceTableName, readDmDatabase,
  readDmTokenAddressTableName, readDmTokenPriceTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDatabase = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readOdsDatabase")
    readLogsTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readLogsTableName")
    readReceiptTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readReceiptTableName")
    readTraceTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readTraceTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readDmDatabase")
    readDmTokenAddressTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readDmTokenTableName")
    readDmTokenPriceTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readDmTokenPriceTableName")
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

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    import spark.implicits._

    /**
      * 解析 logs 中 token 的交易数据
      *
      */
    spark.read.table(s"${readOdsDatabase}.${readLogsTableName}")
      .where(s" logs_address != '' and logs_topics_one = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' " +
        s" and size(split(logs_topics,',')) > 2 and logs_address in (select address from ${readDmDatabase}.${readDmTokenAddressTableName}) ")
      .select("block_number", "logs_address", "logs_topics", "logs_data",
        "logs_transaction_index", "logs_transaction_hash", "date_time", "transaction_date")
      .rdd.map(x => {
      val block_number = x.get(0).toString
      val logs_address = x.get(1).toString
      val logs_topics = x.get(2).toString.replaceAll("\"|\\[|\\]", "").split(",")
      val from_address = "0x" + logs_topics(1).reverse.substring(0, 40).reverse
      val to_address = "0x" + logs_topics(2).reverse.substring(0, 40).reverse
      val logs_data = x.get(3).toString
      val value = if (logs_data.startsWith("0x")) ProcessingString.dataHexadecimalTransform(logs_data.substring(2), 16) else logs_data
      val logs_transaction_index = x.get(4).toString
      val logs_transaction_hash = x.get(5).toString
      val date_time = x.get(6).toString
      val transaction_date = x.get(7).toString
      (block_number, logs_address, from_address, to_address, value, logs_transaction_index, logs_transaction_hash, date_time, transaction_date)
    }).toDF("block_number", "logs_address", "from_address", "to_address", "value",
      "logs_transaction_index", "logs_transaction_hash", "date_time", "transaction_date")
      .createTempView("logs")

    spark.read.table(s"${readOdsDatabase}.${readReceiptTableName}")
      .where(s" receipt_status != 'false' ")
      .select("block_number", "receipt_transaction_index", "receipt_transaction_hash", "transaction_date")
      .createTempView("receipt")

    spark.read.table(s"${readOdsDatabase}.${readTraceTableName}")
      .where(s" trace_error = '' ")
      .select("block_number", "trace_transaction_index", "trace_transaction_hash", "transaction_date")
      .rdd.map(x => {
      val block_number = x.get(0).toString
      val index = x.get(1).toString
      val trace_transaction_index = if (index.startsWith("0x")) ProcessingString.dataHexadecimalTransform(index.substring(2), 16) else index
      val trace_transaction_hash = x.get(2).toString
      val transaction_date = x.get(3).toString
      (block_number, trace_transaction_index, trace_transaction_hash, transaction_date)
    }).toDF("block_number", "trace_transaction_index", "trace_transaction_hash", "transaction_date")
      .createTempView("trace")

    val targetDF = spark.sql(
      s"""
         |
         |select
         |    t1.block_number, t1.from_address, t1.to_address, t4.decimals, t1.value, nvl(t5.price_us,'') as price_us,
         |    t1.logs_transaction_index, t1.logs_transaction_hash, t1.date_time, t4.id as token_id, t4.token_symbol,
         |    t1.logs_address as token_address, t1.transaction_date
         |from logs t1
         |left join
         |    receipt t2
         |on
         |    t1.block_number = t2.block_number and t1.transaction_date = t2.transaction_date and t1.logs_transaction_index = t2.receipt_transaction_index and t1.logs_transaction_hash = t2.receipt_transaction_hash
         |left join
         |    trace t3
         |on
         |    t1.block_number = t3.block_number and t1.transaction_date = t3.transaction_date and t1.logs_transaction_index = t3.trace_transaction_index and t1.logs_transaction_hash = t3.trace_transaction_hash
         |left join
         |    (select id, address, decimals, token_symbol from ${readDmDatabase}.${readDmTokenAddressTableName}) t4
         |on
         |    t1.logs_address = t4.address
         |left join
         |    (select id, price_us, transaction_date from ${readDmDatabase}.${readDmTokenPriceTableName} ) t5
         |on
         |    t1.transaction_date = t5.transaction_date and t4.id = t5.id
         |where
         |    t2.block_number is not null and t3.block_number is not null
         |
      """.stripMargin).rdd.map(x => {
      val block_number = x.get(0).toString
      val from_address = x.get(1).toString
      val to_address = x.get(2).toString
      val decimals = x.get(3).toString.toInt
      val value = if (decimals == 0) BigDecimal(x.get(4).toString).bigDecimal.toPlainString else (BigDecimal(x.get(4).toString) / Math.pow(10, decimals)).bigDecimal.toPlainString
      var price_us = x.get(5).toString
      var amount = ""
      if (price_us != "") {
        price_us = BigDecimal(x.get(5).toString).bigDecimal.toPlainString
        amount = (BigDecimal(value) * BigDecimal(price_us)).bigDecimal.toPlainString
      }
      val logs_transaction_index = x.get(6).toString
      val logs_transaction_hash = x.get(7).toString
      val date_time = x.get(8).toString
      val token_id = x.get(9).toString
      val token_symbol = x.get(10).toString
      val token_address = x.get(11).toString
      val transaction_date = x.get(12).toString
      (block_number, from_address, to_address, value, price_us, amount, logs_transaction_index,
        logs_transaction_hash, date_time, token_id, token_symbol, token_address, transaction_date)
    }).toDF("block_number", "from_address", "to_address", "value", "price_us", "amount", "logs_transaction_index",
      "logs_transaction_hash", "date_time", "token_id", "token_symbol", "token_address", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
