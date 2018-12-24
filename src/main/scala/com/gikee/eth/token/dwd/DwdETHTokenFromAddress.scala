package com.gikee.eth.token.dwd

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.SparkSession

object DwdETHTokenFromAddress {

  var readDwdDatabase, readETHTokenTransactionTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDwdDatabase = spark.sparkContext.getConf.get("spark.dwdETHTokenFromAddress.readDwdDatabase")
    readETHTokenTransactionTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenFromAddress.readETHTokenTransactionTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwdETHTokenFromAddress.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenFromAddress.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwdETHTokenFromAddress.transactionDate")

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
         |
         |select
         |    t1.customer_address,t1.token_address,t1.dh,t1.date_time,t1.transaction_date
         |from
         |(
         |    select
         |        customer_address,token_address,dh,date_time,transaction_date
         |    from(
         |        select
         |            customer_address,
         |            token_address,
         |            dh,
         |            date_time,
         |            transaction_date,
         |            row_number() over (partition by customer_address,token_address order by transaction_date asc ) as rk
         |        from (
         |            select
         |                from_address as customer_address, token_address, dh, date_time, transaction_date
         |            from ${readDwdDatabase}.${readETHTokenTransactionTableName}
         |            where
         |                 transaction_date = '${transactionDate}'
         |        ) t
         |    ) t1
         |    where
         |        t1.rk = 1
         |) t1
         |left join
         |    (select customer_address,token_address from ${writeDataBase}.${writeTableName}) t2
         |on
         |    t1.customer_address = t2.customer_address and t1.token_address = t2.token_address
         |where
         |    t2.customer_address is null
         |
      """.stripMargin

    val query_sql =
      s"""
         |
         |select
         |    customer_address,token_address,dh,date_time,transaction_date
         |from(
         |    select
         |        customer_address,
         |        token_address,
         |        substr(date_time,12,2) as dh,
         |        date_time,
         |        transaction_date,
         |        row_number() over (partition by customer_address,token_address order by transaction_date asc ) as rk
         |    from (
         |        select
         |            from_address as customer_address, token_address, date_time, transaction_date
         |        from ${readDwdDatabase}.${readETHTokenTransactionTableName}
         |    ) t
         |) t1
         |where
         |    t1.rk = 1
         |
      """.stripMargin

    val targetDF = if (transactionDate != "") spark.sql(query_everyday_sql) else spark.sql(query_sql)

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")

    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
