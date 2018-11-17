package com.gikee.eth.token.dw

import com.gikee.common.CommonConstant
import com.gikee.util.DateTransform
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 休眠、唤醒
  */
object DwETHTokenDormantAwakenNumber {

  var readDwdDatabase, readDwdETHTokenTransactionTableName, readDwdETHTokenFromAddressTableName,
  writeDataBase, writeDormantTableName, writeAwakenTableName, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    readDwdDatabase = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDwdDatabase")
    readDwdETHTokenTransactionTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDwdETHTokenTransactionTableName")
    readDwdETHTokenFromAddressTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDwdETHTokenFromAddressTableName")
    writeDataBase = sc.getConf.get("spark.dwdETHDormantAwakenNumber.writeDataBase")
    writeDormantTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.writeDormantTableName")
    writeAwakenTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.writeAwakenTableName")
    dateTime = sc.getConf.get("spark.dwdETHDormantAwakenNumber.dateTime")

    getDwdETHDormantAwakenNumber(spark)

    spark.stop()

  }

  def getDwdETHDormantAwakenNumber(spark: SparkSession): Unit = {

    val beforeDate = DateTransform.getBeforeDate(dateTime, CommonConstant.FormatDay, -60)
    val afterDate = DateTransform.getBeforeDate(dateTime, CommonConstant.FormatDay, 1)

    val query_dormant =
      s"""
         |select
         |   t1.address,t1.token_symbol,t1.token_address,'${dateTime}' as transaction_date
         |from
         |(
         |    select
         |        address,
         |        token_symbol,
         |        token_address
         |    from ${readDwdDatabase}.${readDwdETHTokenFromAddressTableName}
         |    where
         |        transaction_date <= '${dateTime}'
         |)t1
         |left join
         |(
         |    select
         |        from_address as address,
         |        token_symbol,
         |        token_address
         |    from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |    where
         |        transaction_date between '${beforeDate}' and '${dateTime}'
         |)t2
         |on
         |    t1.address = t2.address and t1.token_symbol = t2.token_symbol and t1.token_address = t2.token_address
         |where
         |    t2.address is null
      """.stripMargin

    spark.sql(query_dormant).createTempView("dormant")

    spark.sql(
      """
        |select
        |    count(1) as address_number,token_symbol,token_address,transaction_date
        |from dormant
        |group by
        |    token_symbol,token_address,transaction_date
      """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(s"${writeDataBase}.${writeDormantTableName}")

    spark.sql(
      s"""
         |select
         |    count(1) as address_number,t1.token_symbol,t1.token_address,t1.transaction_date
         |from dormant t1
         |left join
         |(
         |    select
         |        from_address as address,token_symbol,token_address
         |    from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |    where
         |        transaction_date = '${afterDate}'
         |    group by
         |        from_address,token_symbol,token_address
         |) t2
         |on
         |    t1.address = t2.address and t1.token_symbol = t2.token_symbol and t1.token_address = t2.token_address
         |where
         |    t2.address is not null
         |group by
         |    t1.token_symbol,t1.token_address,t1.transaction_date
      """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite)
      .insertInto(s"${writeDataBase}.${writeAwakenTableName}")

  }

}
