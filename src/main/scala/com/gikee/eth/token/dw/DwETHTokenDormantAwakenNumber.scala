package com.gikee.eth.token.dw

import com.gikee.common.CommonConstant
import com.gikee.util.DateTransform
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 休眠、唤醒 by lucas 20181114
  */
object DwETHTokenDormantAwakenNumber {

  var readDwdDatabase, readDwdETHTokenTransactionTableName, readDwdETHTokenFromAddressTableName,
  readDmDatabase,readDmTokenListTableName, writeDataBase, writeDormantTableName, writeAwakenTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    readDwdDatabase = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDwdDatabase")
    readDwdETHTokenTransactionTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDwdETHTokenTransactionTableName")
    readDwdETHTokenFromAddressTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDwdETHTokenFromAddressTableName")
    readDmDatabase = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDmDatabase")
    readDmTokenListTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.readDmTokenListTableName")
    writeDataBase = sc.getConf.get("spark.dwdETHDormantAwakenNumber.writeDataBase")
    writeDormantTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.writeDormantTableName")
    writeAwakenTableName = sc.getConf.get("spark.dwdETHDormantAwakenNumber.writeAwakenTableName")
    transactionDate = sc.getConf.get("spark.dwdETHDormantAwakenNumber.transactionDate")

    getDwdETHDormantAwakenNumber(spark)

    spark.stop()

  }

  def getDwdETHDormantAwakenNumber(spark: SparkSession): Unit = {

    val beforeDate_60 = DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -60)
    val beforeDate_61 = DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -61)
    val yesterday = DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -1)

    spark.sql(
      s"""
         |
         |select
         |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
         |from (select * from  ${readDmDatabase}.${readDmTokenListTableName} where transaction_date = '${transactionDate}' ) t1
         |left join
         |(
         |    select
         |       count(1) as value_num, t1.token_address, '${transactionDate}' as transaction_date
         |    from
         |    (
         |        select
         |            customer_address,
         |            token_address
         |        from ${readDwdDatabase}.${readDwdETHTokenFromAddressTableName}
         |        where
         |            transaction_date <= '${transactionDate}'
         |    )t1
         |    left join
         |    (
         |        select
         |            from_address as customer_address,
         |            token_address
         |        from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |        where
         |            transaction_date between '${beforeDate_60}' and '${transactionDate}'
         |    )t2
         |    on
         |        t1.customer_address = t2.customer_address and t1.token_address = t2.token_address
         |    where
         |        t2.customer_address is null
         |    group by
         |        t1.token_address
         |) t2
         |on
         |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
         |
      """.stripMargin).repartition(1).write.mode(SaveMode.Overwrite)
      .insertInto(s"${writeDataBase}.${writeDormantTableName}")

    val query_yesterday_dormant =
      s"""
         |select
         |   t1.customer_address,t1.token_address,'${yesterday}' as transaction_date
         |from
         |(
         |    select
         |        customer_address,
         |        token_address
         |    from ${readDwdDatabase}.${readDwdETHTokenFromAddressTableName}
         |    where
         |        transaction_date <= '${yesterday}'
         |)t1
         |left join
         |(
         |    select
         |        from_address as customer_address,
         |        token_address
         |    from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |    where
         |        transaction_date between '${beforeDate_61}' and '${yesterday}'
         |)t2
         |on
         |    t1.customer_address = t2.customer_address and t1.token_address = t2.token_address
         |where
         |    t2.customer_address is null
      """.stripMargin

    spark.sql(query_yesterday_dormant).createTempView("query_yesterday_dormant")

    spark.sql(
      s"""
         |select
         |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
         |from (select * from  ${readDmDatabase}.${readDmTokenListTableName} where transaction_date = '${transactionDate}' ) t1
         |left join
         |(
         |    select
         |        count(1) as value_num , t1.token_address, '${transactionDate}' as transaction_date
         |    from query_yesterday_dormant t1
         |    left join
         |    (
         |        select
         |            from_address as customer_address,token_address
         |        from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |        where
         |            transaction_date = '${transactionDate}'
         |        group by
         |            from_address,token_address
         |    ) t2
         |    on
         |        t1.customer_address = t2.customer_address and t1.token_address = t2.token_address
         |    where
         |        t2.customer_address is not null
         |    group by
         |        t1.token_address,t1.transaction_date
         |)t2
         |on
         |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
      """.stripMargin).repartition(1).write.mode(SaveMode.Overwrite)
      .insertInto(s"${writeDataBase}.${writeAwakenTableName}")

  }

}
