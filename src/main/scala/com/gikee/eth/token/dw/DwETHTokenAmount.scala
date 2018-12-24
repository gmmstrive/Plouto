package com.gikee.eth.token.dw

import org.apache.spark.sql.SparkSession

object DwETHTokenAmount {

  var readDwdDatabase, readDwdETHTokenToAddressTableName, readDmDatabase, readDmTokenListTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    getDwETHTokenCount(spark)

    spark.stop()

  }

  def getDwETHTokenCount(spark:SparkSession): Unit ={





  }

}
