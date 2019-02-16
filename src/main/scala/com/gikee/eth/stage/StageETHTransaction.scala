package com.gikee.eth.stage

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, ProcessingString, TableUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 以太币交易表 by lucas 20181210
  */
object StageETHTransaction {

  var readOdsDataBase, readBlockTableName, readBaseTableName, readTraceTableName, readCallsTableName, readUnclesTableName, readDmDataBase, readPriceTableName,
  writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readOdsDataBase = spark.sparkContext.getConf.get("spark.stageETHTransaction.readOdsDataBase")
    readBlockTableName = spark.sparkContext.getConf.get("spark.stageETHTransaction.readBlockTableName")
    readBaseTableName = spark.sparkContext.getConf.get("spark.stageETHTransaction.readBaseTableName")
    readTraceTableName = spark.sparkContext.getConf.get("spark.stageETHTransaction.readTraceTableName")
    readCallsTableName = spark.sparkContext.getConf.get("spark.stageETHTransaction.readCallsTableName")
    readUnclesTableName = spark.sparkContext.getConf.get("spark.stageETHTransaction.readUnclesTableName")
    readDmDataBase = spark.sparkContext.getConf.get("spark.stageETHTransaction.readDmDataBase")
    readPriceTableName = spark.sparkContext.getConf.get("spark.stageETHTransaction.readPriceTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.stageETHTransaction.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.stageETHTransaction.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.stageETHTransaction.transactionDate")

    getStageETHTransaction(spark)

    spark.stop()

  }

  def getStageETHTransaction(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    import spark.implicits._

    /**
      * 计算手续费
      */
    spark.read.table(s"${readOdsDataBase}.${readBaseTableName}").where(s" transaction_date = '${transactionDate}' ")
      .select("block_number", "base_gas_used", "base_gas_price").rdd.map(x => {
      var service_charge = "0"
      val block_number = x.get(0).toString
      val base_gas_used = x.get(1).toString
      val base_gas_price = x.get(2).toString
      val price = if (base_gas_price.startsWith("0x"))
        ProcessingString.dataHexadecimalTransform(base_gas_price.substring(2), 16) else base_gas_price
      if (price != null && price != "" && base_gas_used != null && base_gas_used != "")
        service_charge = (BigDecimal(base_gas_used) * BigDecimal(price) / Math.pow(10, 18)).bigDecimal.toPlainString
      (block_number, service_charge)
    }).toDF("block_number", "service_charge")
      .groupBy($"block_number").agg(sum($"service_charge").alias("service_charge"))
      .rdd.map(x => {
      (x.get(0).toString, BigDecimal(x.get(1).toString).bigDecimal.toPlainString)
    }).toDF("block_number", "service_charge").createTempView("base_charge")

    /**
      * 叔块挖出者奖励 //if (BigDecimal(block_number) >= 4370000) 3 else 5
      */
    spark.read.table(s"${readOdsDataBase}.${readUnclesTableName}").where(s" transaction_date = '${transactionDate}' ")
      .select("block_number", "uncles_miner", "uncles_number", "date_time", "transaction_date").map(x => {
      val block_number = x.get(0).toString
      val to_address = x.get(1).toString
      val uncles_number = x.get(2).toString
      val date_time = x.get(3).toString
      val transaction_date = x.get(4).toString
      val from_address = "0x^0"
      val transaction_index = ""
      val transaction_hash = ""
      val transaction_type = "uncles"
      val value = ((BigDecimal(uncles_number) + 8 - BigDecimal(block_number)) *
        (if (BigDecimal(block_number) <= 7080000) { if (BigDecimal(block_number) >= 4370000) 3 else 5 } else { 2 }) / 8).bigDecimal.toPlainString
      (block_number, from_address, to_address, value, transaction_index, transaction_hash, date_time, transaction_type, transaction_date)
    }).toDF("block_number", "from_address", "to_address", "value", "transaction_index", "transaction_hash", "date_time",
      "transaction_type", "transaction_date").createTempView("uncles_reward")

    // if(cast(t1.block_number as double) <= 7080000,if (cast(t1.block_number as double) >= 4370000,3, 5),2)
    // if(cast(t1.block_number as double) >= 4370000,3,5)
    val rewardDF = spark.sql(
      s"""
         |select
         |    t1.block_number, '0x^0' as from_address, t1.to_address,
         |    if(cast(t1.block_number as double) <= 7080000,if (cast(t1.block_number as double) >= 4370000,3, 5),2) + if(t2.block_number is null,0,if(cast(t1.block_number as double) <= 7080000,if (cast(t1.block_number as double) >= 4370000,3, 5),2) * 0.03125 * t2.block_count) + if(t3.block_number is null,0,t3.service_charge) as value,
         |    '' as transaction_index, '' as transaction_hash, t1.date_time, 'reward_charge' as transaction_type, t1.transaction_date
         |from
         |(select block_number, miner as to_address, date_time, transaction_date from ${readOdsDataBase}.${readBlockTableName} where transaction_date = '${transactionDate}' ) t1
         |left join
         |    (select count(1) as block_count, block_number from ${readOdsDataBase}.${readUnclesTableName}  where transaction_date = '${transactionDate}' group by block_number) t2
         |on
         |    t1.block_number= t2.block_number
         |left join
         |    base_charge t3
         |on
         |    t1.block_number = t3.block_number
         |union all
         |    select * from uncles_reward
      """.stripMargin)

    /**
      * 外部交易
      */
    val traceDF = spark.read.table(s"${readOdsDataBase}.${readTraceTableName}")
      .where(s"trace_error = '' and trace_from != '' and trace_to != '' and transaction_date = '${transactionDate}' ")
      .select("block_number", "trace_from", "trace_to", "trace_value", "trace_transaction_index", "trace_transaction_hash",
        "date_time", "transaction_date").rdd.map(x => {
      val block_number = x.get(0).toString
      val from_address = x.get(1).toString
      val to_address = x.get(2).toString
      val trace_value = x.get(3).toString
      val value = if (trace_value.startsWith("0x")) {
        (BigDecimal(ProcessingString.dataHexadecimalTransform(trace_value.substring(2), 16)) / Math.pow(10, 18)).bigDecimal.toPlainString
      } else {
        trace_value
      }
      val transaction_index = x.get(4).toString
      val transaction_hash = x.get(5).toString
      val date_time = x.get(6).toString
      val transaction_date = x.get(7).toString
      (block_number, from_address, to_address, value, transaction_index, transaction_hash, date_time, "trace", transaction_date)
    }).toDF("block_number", "from_address", "to_address", "value", "transaction_index", "transaction_hash",
      "date_time", "transaction_type", "transaction_date")

    /**
      * 内部交易
      */
    val callsDF = spark.read.table(s"${readOdsDataBase}.${readCallsTableName}")
      .where(s" calls_error = '' and calls_from != '' and calls_to != '' and transaction_date = '${transactionDate}' ")
      .select("block_number", "calls_from", "calls_to", "calls_value", "calls_transaction_index", "calls_transaction_hash",
        "date_time", "transaction_date").rdd.map(x => {
      val block_number = x.get(0).toString
      val from_address = x.get(1).toString
      val to_address = x.get(2).toString
      val calls_value = x.get(3).toString
      val value = if (calls_value.startsWith("0x")) {
        (BigDecimal(ProcessingString.dataHexadecimalTransform(calls_value.substring(2), 16)) / Math.pow(10, 18)).bigDecimal.toPlainString
      } else {
        calls_value
      }
      val transaction_index = x.get(4).toString
      val transaction_hash = x.get(5).toString
      val date_time = x.get(6).toString
      val transaction_date = x.get(7).toString
      (block_number, from_address, to_address, value, transaction_index, transaction_hash, date_time, "calls", transaction_date)
    }).toDF("block_number", "from_address", "to_address", "value", "transaction_index", "transaction_hash",
      "date_time", "transaction_type", "transaction_date")

    rewardDF.union(traceDF).union(callsDF).createTempView("target")

    /**
      * 把每日 eth 价格加入进去
      */
    val targetDF = spark.sql(
      s"""
         |
        |select
         |    block_number, from_address, to_address, value, if(t2.price_us is null,'0',t2.price_us) as price_us, transaction_index, transaction_hash, date_time, transaction_type, t1.transaction_date
         |from  target t1
         |left join
         |    (select price_us, transaction_date from ${readDmDataBase}.${readPriceTableName} where id = 'ethereum' and transaction_date = '${transactionDate}' ) t2
         |on
         |    t1.transaction_date = t2.transaction_date
         |
      """.stripMargin).rdd.map(x => {
      val block_number = x.get(0).toString
      val from_address = x.get(1).toString
      val to_address = x.get(2).toString
      val value = x.get(3).toString
      val price_us = BigDecimal(x.get(4).toString).bigDecimal.toPlainString
      val transaction_index = x.get(5).toString
      val transaction_hash = x.get(6).toString
      val date_time = x.get(7).toString
      val dh = DateTransform.getDate2Detailed(date_time)._5
      val transaction_type = x.get(8).toString
      val transaction_date = x.get(9).toString
      var amount = ""
      if (price_us != "" && value != "") amount = (BigDecimal(value) * BigDecimal(price_us)).bigDecimal.toPlainString
      (block_number, from_address, to_address, value, price_us, amount, transaction_index, transaction_hash, dh, date_time, transaction_type, transaction_date)
    }).toDF("block_number", "from_address", "to_address", "value", "price_us", "amount", "transaction_index", "transaction_hash",
      "dh", "date_time", "transaction_type", "transaction_date").sortWithinPartitions("transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
