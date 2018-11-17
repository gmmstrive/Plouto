package com.gikee.eth.token.dwd

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.SparkSession

object DwdETHTokenFromAddress {

  var readDwdDatabase, readDwdETHTokenTransactionTableName, writeDataBase, writeTableName, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    readDwdDatabase = sc.getConf.get("spark.dwdETHTokenFromAddress.readDwdDatabase")
    readDwdETHTokenTransactionTableName = sc.getConf.get("spark.dwdETHTokenFromAddress.readDwdETHTokenTransactionTableName")
    writeDataBase = sc.getConf.get("spark.dwdETHTokenFromAddress.writeDataBase")
    writeTableName = sc.getConf.get("spark.dwdETHTokenFromAddress.writeTableName")
    dateTime = spark.sparkContext.getConf.get("spark.dwdETHTokenFromAddress.transactionDate")

    getDwdETHTokenFromAddress(spark)

    spark.stop()

  }

  def getDwdETHTokenFromAddress(spark: SparkSession): Unit = {

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
         |            from dw.dwd_eth_token_transaction
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
         |            from dw.dwd_eth_token_transaction
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
