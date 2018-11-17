package com.gikee.eth.token.dw

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * 总交易地址
  */
object DwETHTokenTotalAddress {

  var readDwdDatabase, readDwdETHTokenToAddressTableName, writeDataBase, writeTableName, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    readDwdDatabase = sc.getConf.get("spark.dwdETHTokenTotalAddress.readDwdDatabase")
    readDwdETHTokenToAddressTableName = sc.getConf.get("spark.dwdETHTokenTotalAddress.readDwdETHTokenToAddressTableName")
    writeDataBase = sc.getConf.get("spark.dwdETHTokenTotalAddress.writeDataBase")
    writeTableName = sc.getConf.get("spark.dwdETHTokenTotalAddress.writeTableName")
    dateTime = sc.getConf.get("spark.dwdETHTokenTotalAddress.dateTime")

    spark.stop()

  }

  def getDwdETHTokenTotalAddress(spark: SparkSession): Unit = {

    val query_sql =
      s"""
         |select
         |    sum(t1.customer_number) as customer_number, t2.transaction_date
         |from
         |    (select count(1) as customer_number, transaction_date from ${readDwdDatabase}.${readDwdETHTokenToAddressTableName} group by transaction_date ) t1,
         |    (select count(1) as customer_number, transaction_date from ${readDwdDatabase}.${readDwdETHTokenToAddressTableName} group by transaction_date ) t2
         |where
         |    t1.transaction_date <= t2.transaction_date
         |group by
         |    t2.transaction_date
         |order by
         |    t2.transaction_date
         |
      """.stripMargin

    val query_everyday_sql =
      s"""
        |select count(1) as customer_number, '${dateTime}' as transaction_date from ${readDwdDatabase}.${readDwdETHTokenToAddressTableName}
      """.stripMargin

    val targetDF = if (dateTime != "") spark.sql(query_everyday_sql) else spark.sql(query_sql)

    targetDF.coalesce(1).write.mode(SaveMode.Overwrite).insertInto(s"${writeDataBase}.${writeTableName}")

  }

}
