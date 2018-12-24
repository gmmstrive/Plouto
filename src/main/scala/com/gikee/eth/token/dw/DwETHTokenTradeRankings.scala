package com.gikee.eth.token.dw

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 地址交易频次排名 by lucas 20181120
  */
object DwETHTokenTradeRankings {

  var readDwdDatabase, readDwdETHTokenTransactionTableName, writeDataBase, writeTradeRankingsMonthlyTableName,
  writeTradeRankingsEverydayTableName, writeTradeRankingsWeekTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDwdDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenTradeRankings.readDwdDatabase")
    readDwdETHTokenTransactionTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeRankings.readDwdETHTokenToAddressTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTokenTradeRankings.writeDataBase")
    writeTradeRankingsMonthlyTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeRankings.writeTradeRankingsMonthlyTableName")
    writeTradeRankingsWeekTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeRankings.writeTradeRankingsWeekTableName")
    writeTradeRankingsEverydayTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeRankings.writeTradeRankingsEverydayTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTokenTradeRankings.transactionDate")

    getDwETHTokenTradeRankingsTotalMonthly(spark)

    spark.stop()

  }

  def getDwETHTokenTradeRankingsTotalMonthly(spark: SparkSession): Unit = {

    val query_monthly =
      s"""
         |
         |select
         |    value_num, customer_address, token_address, '${transactionDate}' as transaction_date
         |from(
         |    select
         |        value_num, customer_address,  token_address,
         |        row_number() over (partition by  token_address order by value_num desc) as ank
         |    from
         |    (
         |        select
         |            count(1) as value_num, customer_address,  token_address
         |        from
         |        (
         |            select
         |             from_address as customer_address, token_address
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |             transaction_date between date_add('${transactionDate}', -30) and '${transactionDate}'
         |            union all
         |            select
         |             to_address  as customer_address, token_address
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |             transaction_date between date_add('${transactionDate}', -30) and '${transactionDate}'
         |        ) t
         |        group by
         |            customer_address,  token_address
         |    ) tt
         |) ttt
         |where
         |    ank <=100
         |
      """.stripMargin

    val query_everyday =
      s"""
         |
         |select
         |    value_num, customer_address, token_address, transaction_date
         |from(
         |    select
         |        value_num, customer_address, token_address, transaction_date,
         |        row_number() over (partition by token_address, transaction_date order by value_num desc) as ank
         |    from
         |    (
         |        select
         |            count(1) as value_num, customer_address, token_address, transaction_date
         |        from
         |        (
         |            select
         |                from_address as customer_address, token_address, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |                transaction_date = '${transactionDate}'
         |            union all
         |            select
         |                to_address   as customer_address, token_address, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |                transaction_date = '${transactionDate}'
         |        ) t
         |        group by
         |            customer_address, token_address, transaction_date
         |    ) tt
         |) ttt
         |where
         |    ank <=100
         |
       """.stripMargin

    val query_week =
      s"""
         |
         |select
         |    value_num, customer_address, token_address, '${transactionDate}' as transaction_date
         |from(
         |    select
         |        value_num, customer_address,  token_address,
         |        row_number() over (partition by  token_address order by value_num desc) as ank
         |    from
         |    (
         |        select
         |            count(1) as value_num, customer_address,  token_address
         |        from
         |        (
         |            select
         |             from_address as customer_address, token_address
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |             transaction_date between date_add('${transactionDate}', -7) and '${transactionDate}'
         |            union all
         |            select
         |             to_address  as customer_address, token_address
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |             transaction_date between date_add('${transactionDate}', -7) and '${transactionDate}'
         |        ) t
         |        group by
         |            customer_address,  token_address
         |    ) tt
         |) ttt
         |where
         |    ank <=100
         |
      """.stripMargin

    spark.sql(query_monthly)
      .coalesce(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTradeRankingsMonthlyTableName}")

    spark.sql(query_week)
      .coalesce(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTradeRankingsWeekTableName}")

    spark.sql(query_everyday)
      .coalesce(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTradeRankingsEverydayTableName}")

  }

}
