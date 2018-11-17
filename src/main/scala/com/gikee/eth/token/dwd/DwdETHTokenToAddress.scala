package com.gikee.eth.token.dwd

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.SparkSession

/**
  * token 新增地址 by lucas 20181114
  */
object DwdETHTokenToAddress {

  var readDwdDatabase, readDwdETHTokenTransactionTableName, writeDataBase, writeTableName, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    readDwdDatabase = sc.getConf.get("spark.dwdETHTokenToAddress.readDwdDatabase")
    readDwdETHTokenTransactionTableName = sc.getConf.get("spark.dwdETHTokenToAddress.readDwdETHTokenTransactionTableName")
    writeDataBase = sc.getConf.get("spark.dwdETHTokenToAddress.writeDataBase")
    writeTableName = sc.getConf.get("spark.dwdETHTokenToAddress.writeTableName")
    dateTime = spark.sparkContext.getConf.get("spark.dwdETHTokenToAddress.transactionDate")

    getDwdETHTokenToAddress(spark)

    spark.stop()

  }

  def getDwdETHTokenToAddress(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val query_everyday_sql =
      s"""
         |select
         |    t1.address,t1.token_symbol,t1.token_address,t1.dh,t1.date_time,t1.transaction_date
         |from
         |(
         |    select
         |        address,token_symbol,token_address,dh,date_time,transaction_date
         |    from(
         |        select
         |            address,
         |            token_symbol,
         |            token_address,
         |            substr(date_time,12,2) as dh,
         |            date_time,
         |            transaction_date,
         |            row_number() over (partition by address,token_address,token_symbol order by transaction_date asc ) as rk
         |        from (
         |            select
         |                from_address as address, token_address, token_symbol, date_time, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |                transaction_date = '${dateTime}'
         |            union all
         |            select
         |                to_address   as address, token_address, token_symbol, date_time, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |                transaction_date = '${dateTime}'
         |        ) t
         |    ) t1
         |    where
         |        t1.rk = 1
         |) t1
         |left join
         |    (select address,token_symbol,token_address from ${writeDataBase}.${writeTableName}) t2
         |on
         |    t1.address = t2.address and t1.token_symbol = t2.token_symbol and t1.token_address = t2.token_address
         |where
         |    t2.address is null
      """.stripMargin

    val query_sql =
      s"""
         |select
         |    t1.address,t1.token_symbol,t1.token_address,t1.dh,t1.date_time,t1.transaction_date
         |from
         |(
         |    select
         |        address,token_symbol,token_address,dh,date_time,transaction_date
         |    from(
         |        select
         |            address,
         |            token_symbol,
         |            token_address,
         |            substr(date_time,12,2) as dh,
         |            date_time,
         |            transaction_date,
         |            row_number() over (partition by address,token_address,token_symbol order by transaction_date asc ) as rk
         |        from (
         |            select
         |                from_address as address, token_address, token_symbol, date_time, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            union all
         |            select
         |                to_address   as address, token_address, token_symbol, date_time, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |        ) t
         |    ) t1
         |    where
         |        t1.rk = 1
         |) t1
         |left join
         |    (select address,token_symbol,token_address from ${writeDataBase}.${writeTableName}) t2
         |on
         |    t1.address = t2.address and t1.token_symbol = t2.token_symbol and t1.token_address = t2.token_address
         |where
         |    t2.address is null
      """.stripMargin

    val targetDF = if (dateTime != "") spark.sql(query_everyday_sql) else spark.sql(query_sql)

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")

    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
