package com.gikee.eth.dw

import org.apache.spark.sql.SparkSession

object DwETHToAddressSum {

  var readDatabase, readTableName, writeDataBase, writeEverydayTableName,
  writeWeekTableName, writeMonthTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDatabase = spark.sparkContext.getConf.get("spark.dwETHToAddressSum.readDatabase")
    readTableName = spark.sparkContext.getConf.get("spark.dwETHToAddressSum.readTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHToAddressSum.writeDataBase")
    writeEverydayTableName = spark.sparkContext.getConf.get("spark.dwETHToAddressSum.writeEverydayTableName")
    writeWeekTableName = spark.sparkContext.getConf.get("spark.dwETHToAddressSum.writeWeekTableName")
    writeMonthTableName = spark.sparkContext.getConf.get("spark.dwETHToAddressSum.writeMonthTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHToAddressSum.transactionDate")

    getDwETHToAddressSum(spark)

    spark.stop()

  }

  def getDwETHToAddressSum(spark: SparkSession): Unit = {

    if (transactionDate != "") {

      val query_every_day =
        s"""
           |
           |select
           |    count(1) as value_num,transaction_date
           |from ${readDatabase}.${readTableName}
           |where
           |    transaction_date = '${transactionDate}'
           |group by
           |    transaction_date
           |
        """.stripMargin

    } else {

      val query_all_every_day =
        s"""
           |
           |select
           |    count(1) as value_num,transaction_date
           |from ${readDatabase}.${readTableName}
           |group by
           |    transaction_date
           |
        """.stripMargin

    }


  }

}
