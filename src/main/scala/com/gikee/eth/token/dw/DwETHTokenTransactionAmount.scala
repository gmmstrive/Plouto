package com.gikee.eth.token.dw

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.SparkSession

/**
  * 交易金额 by 晓静 2018117
  */
object DwETHTokenTransactionAmount {

  var readDwdDatabase, readDwdETHTokenTransactionTableName,
  writeDataBase, writeTableName, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    readDwdDatabase = sc.getConf.get("spark.DwdETHTokenTransactionAmount.readDwdDatabase")
    readDwdETHTokenTransactionTableName = sc.getConf.get("spark.DwdETHTokenTransactionAmount.readDwdETHTokenTransactionTableName")
    writeDataBase = sc.getConf.get("spark.DwdETHTokenTransactionAmount.writeDataBase")
    writeTableName = sc.getConf.get("spark.DwdETHTokenTransactionAmount.writeTableName")
    dateTime = sc.getConf.get("spark.DwdETHTokenTransactionAmount.dateTime")

    getDwdETHTokenTransactionAmount(spark)

    spark.stop()

  }

  def getDwdETHTokenTransactionAmount(spark:SparkSession): Unit ={

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val query_sql =
      s"""
        |select
        |    sum(if(amount = '',0,cast(amount as double))) as amount,
        |    token_symbol,token_address,substr(date_time,12,2) as dh,
        |    substr(transaction_date,1,7) as monthly,transaction_date
        |from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
        |group by
        |    token_symbol,token_address,substr(transaction_date,1,7),transaction_date,substr(date_time,12,2)
      """.stripMargin

    val query_everyday_sql =
      s"""
         |select
         |    sum(if(amount = '',0,cast(amount as double))) as amount,
         |    token_symbol,token_address,substr(date_time,12,2) as dh,
         |    substr(transaction_date,1,7) as monthly,transaction_date
         |from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |where
         |    transaction_date = '${dateTime}'
         |group by
         |    token_symbol,token_address,substr(transaction_date,1,7),transaction_date,substr(date_time,12,2)
      """.stripMargin

    import spark.implicits._

    val tempDF = if (dateTime != "") spark.sql(query_everyday_sql) else spark.sql(query_sql)

    val targetDF = tempDF.rdd.map(x=>{
      val amount = BigDecimal(x.get(0).toString).bigDecimal.toPlainString
      val token_symbol = x.get(1).toString
      val token_address = x.get(2).toString
      val dh = x.get(3).toString
      val monthly = x.get(4).toString
      val transaction_date = x.get(5).toString
      (amount,token_symbol,token_address,dh,monthly,transaction_date)
    }).toDF("amount","token_symbol","token_address","dh","monthly","transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")

    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
