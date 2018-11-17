package com.gikee.eth

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{ProcessingString, TableUtil}
import org.apache.spark.sql.SparkSession

object DwdETHTokenTransaction {

  var readOdsDatabase, readLogsTableName, readReceiptTableName, readTraceTableName, readDimDatabase, readDimTokenTableName,
  writeDataBase, writeTableName, partitionAnnual, partitionMonthly, partitionEveryday: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDatabase = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readOdsDatabase")
    readLogsTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readLogsTableName")
    readReceiptTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readReceiptTableName")
    readTraceTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readTraceTableName")
    readDimDatabase = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readDimDatabase")
    readDimTokenTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.readDimTokenTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.writeTableName")
    partitionAnnual = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.partitionAnnual")
    partitionMonthly = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.partitionMonthly")
    partitionEveryday = spark.sparkContext.getConf.get("spark.dwdETHTokenTransaction.partitionEveryday")

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

    spark.read.table(s"${readOdsDatabase}.${readLogsTableName}")
      .where(" logs_address != '' and lower(logs_topics_one) = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' " +
        s" and size(split(logs_topics,',')) > 2 and lower(logs_address) in (select lower(address) from ${readDimDatabase}.${readDimTokenTableName}) ")
      .select("block_number", "logs_address", "logs_topics", "logs_data", "logs_transaction_index", "logs_transaction_hash",
        "date_time", "annual", "monthly", "everyday")
      .rdd.map(x => {
      val block_number = x.get(0).toString
      val logs_address = x.get(1).toString.toLowerCase
      val logs_topics = x.get(2).toString.replaceAll("\"|\\[|\\]", "").split(",")
      val from_address = "0x" + logs_topics(1).reverse.substring(0, 40).reverse.toLowerCase
      val to_address = "0x" + logs_topics(2).reverse.substring(0, 40).reverse.toLowerCase
      val logs_data = x.get(3).toString
      val value = if (logs_data.startsWith("0x")) ProcessingString.dataHexadecimalTransform(logs_data.substring(2), 16) else logs_data
      val logs_transaction_index = x.get(4).toString.toLowerCase
      val logs_transaction_hash = x.get(5).toString.toLowerCase
      val date_time = x.get(6).toString
      val annual = x.get(7).toString
      val monthly = x.get(8).toString
      val everyday = x.get(9).toString
      (block_number, logs_address, from_address, to_address, value, logs_transaction_index, logs_transaction_hash, date_time, annual, monthly, everyday)
    }).toDF("block_number", "logs_address", "from_address", "to_address", "value", "logs_transaction_index", "logs_transaction_hash",
      "date_time", "annual", "monthly", "everyday").createTempView("logs")

    spark.read.table(s"${readOdsDatabase}.${readReceiptTableName}").where(" receipt_status != 'false' ")
      .select("block_number", "receipt_transaction_index", "receipt_transaction_hash", "annual", "monthly", "everyday")
      .createTempView("receipt")

    spark.read.table(s"${readOdsDatabase}.${readTraceTableName}").where(" trace_error = '' ")
      .select("block_number", "trace_transaction_index", "trace_transaction_hash", "annual", "monthly", "everyday")
      .rdd.map(x => {
      val block_number = x.get(0).toString
      val index = x.get(1).toString
      val trace_transaction_index = if (index.startsWith("0x")) ProcessingString.dataHexadecimalTransform(index.substring(2), 16) else index
      val trace_transaction_hash = x.get(2).toString
      val annual = x.get(3).toString
      val monthly = x.get(4).toString
      val everyday = x.get(5).toString
      (block_number, trace_transaction_index, trace_transaction_hash, annual, monthly, everyday)
    }).toDF("block_number", "trace_transaction_index", "trace_transaction_hash", "annual", "monthly", "everyday")
      .createTempView("trace")

    val targetDF =
      spark.sql(
        s"""
           |select
           |    t1.block_number,t1.from_address,t1.to_address,
           |    cast(if(t4.decimals = '0',cast(t1.value as double),cast(t1.value as double)/pow(10,cast(t4.decimals as double))) as string) as value,
           |    t1.logs_transaction_index,t1.logs_transaction_hash,t1.date_time,t4.token_symbol,
           |    t1.logs_address as token_address,t1.annual,t1.monthly,t1.everyday
           |from logs t1
           |left join
           |(select block_number,receipt_transaction_index,receipt_transaction_hash,annual,monthly,everyday from receipt) t2
           |on
           |    t1.annual = t2.annual and t1.monthly = t2.monthly and t1.everyday = t2.everyday and t1.block_number= t2.block_number
           |    and t1.logs_transaction_index = t2.receipt_transaction_index and t1.logs_transaction_hash = t2.receipt_transaction_hash
           |left join
           |(select block_number,trace_transaction_index,trace_transaction_hash,annual,monthly,everyday from trace ) t3
           |on
           |    t1.annual = t3.annual and t1.monthly = t3.monthly and t1.everyday = t3.everyday and t1.block_number= t3.block_number
           |    and t1.logs_transaction_index = t3.trace_transaction_index and t1.logs_transaction_hash = t3.trace_transaction_hash
           |left join
           |(select lower(address) as address,decimals,token_symbol from ${readDimDatabase}.${readDimTokenTableName} )t4
           |on
           |    t1.logs_address = lower(t4.address)
           |where
           |    t2.block_number is not null and t3.block_number is not null and t4.address is not null
           |
      """.stripMargin).rdd.map(x => {
        val block_number = x.get(0).toString
        val from_address = x.get(1).toString
        val to_address = x.get(2).toString
        val value = BigDecimal(x.get(3).toString).bigDecimal.toPlainString
        val logs_transaction_index = x.get(4).toString
        val logs_transaction_hash = x.get(5).toString
        val date_time = x.get(6).toString
        val token_symbol = x.get(7).toString
        val token_address = x.get(8).toString
        val annual = x.get(9).toString
        val monthly = x.get(10).toString
        val everyday = x.get(11).toString
        (block_number, from_address, to_address, value, logs_transaction_index, logs_transaction_hash,
          date_time, token_symbol, token_address, annual, monthly, everyday)
      }).toDF("block_number", "from_address", "to_address", "value", "logs_transaction_index", "logs_transaction_hash",
        "date_time", "token_symbol", "token_address", "annual", "monthly", "everyday")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "annual", "monthly", "everyday")

    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "annual", "monthly", "everyday")

  }

}
