package com.gikee.eth.token.dw

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 地址交易频次排名
  */
object DwETHTokenTradeRankings {

  var readDwdDatabase, readDwdETHTokenTransactionTableName, writeDataBase, writeTradeRankingsTotalMonthlyTableName,
  writeTradeRankingsMonthlyTableName, writeTradeRankingsWeekTableName, dateTime: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    readDwdDatabase = sc.getConf.get("spark.DwETHTokenTradeRankings.readDwdDatabase")
    readDwdETHTokenTransactionTableName = sc.getConf.get("spark.DwETHTokenTradeRankings.readDwdETHTokenToAddressTableName")
    writeDataBase = sc.getConf.get("spark.DwETHTokenTradeRankings.writeDataBase")
    writeTradeRankingsTotalMonthlyTableName = sc.getConf.get("spark.DwETHTokenTradeRankings.writeTradeRankingsTotalMonthlyTableName")
    writeTradeRankingsMonthlyTableName = sc.getConf.get("spark.DwETHTokenTradeRankings.writeTradeRankingsMonthlyTableName")
    writeTradeRankingsWeekTableName = sc.getConf.get("spark.DwETHTokenTradeRankings.writeTradeRankingsWeekTableName")
    dateTime = sc.getConf.get("spark.DwETHTokenTradeRankings.dateTime")

    getDwETHTokenTradeRankingsTotalMonthly(spark)

    spark.stop()

  }

  def getDwETHTokenTradeRankingsTotalMonthly(spark: SparkSession): Unit = {

    val query_total_monthly =
      s"""
        |
        |select
        |    count_number, customer_address, token_symbol, token_address
        |from(
        |    select
        |        count_number, customer_address, token_symbol, token_address,
        |        row_number() over (partition by token_symbol, token_address order by count_number desc) as ank
        |    from
        |    (
        |        select
        |            count(1) as count_number, customer_address, token_symbol, token_address
        |        from
        |        (
        |            select
        |             from_address as customer_address, token_symbol, token_address
        |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
        |            where
        |             transaction_date rlike date_format('${dateTime}','yyyy-MM')
        |            union all
        |            select
        |             to_address   as customer_address, token_symbol, token_address
        |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
        |            where
        |             transaction_date rlike date_format('${dateTime}','yyyy-MM')
        |        ) t
        |        group by
        |            customer_address, token_symbol, token_address
        |    ) tt
        |) ttt
        |where
        |    ank <=100
        |
      """.stripMargin

    val query_monthly =
      s"""
         |
         |select
         |    count_number, customer_address, token_symbol, token_address, transaction_date
         |from(
         |    select
         |        count_number, customer_address, token_symbol, token_address, transaction_date,
         |        row_number() over (partition by token_symbol, token_address, transaction_date order by count_number desc) as ank
         |    from
         |    (
         |        select
         |            count(1) as count_number, customer_address, token_symbol, token_address, transaction_date
         |        from
         |        (
         |            select
         |                from_address as customer_address, token_symbol, token_address, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |                transaction_date rlike date_format('${dateTime}','yyyy-MM')
         |            union all
         |            select
         |                to_address   as customer_address, token_symbol, token_address, transaction_date
         |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
         |            where
         |                transaction_date rlike date_format('${dateTime}','yyyy-MM')
         |        ) t
         |        group by
         |            customer_address, token_symbol, token_address, transaction_date
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
        |    count_number, customer_address, token_symbol, token_address, transaction_date
        |from(
        |    select
        |        count_number, customer_address, token_symbol, token_address, transaction_date,
        |        row_number() over (partition by token_symbol, token_address, transaction_date order by count_number desc) as ank
        |    from
        |    (
        |        select
        |            count(1) as count_number, customer_address, token_symbol, token_address, transaction_date
        |        from
        |        (
        |            select
        |                from_address as customer_address, token_symbol, token_address, transaction_date
        |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
        |            where
        |                transaction_date between to_date(date_add('${dateTime}',-7)) and '${dateTime}'
        |            union all
        |            select
        |                to_address   as customer_address, token_symbol, token_address, transaction_date
        |            from ${readDwdDatabase}.${readDwdETHTokenTransactionTableName}
        |            where
        |                transaction_date between to_date(date_add('${dateTime}',-7)) and '${dateTime}'
        |        ) t
        |        group by
        |            customer_address, token_symbol, token_address, transaction_date
        |    ) tt
        |) ttt
        |where
        |    ank <=100
        |
      """.stripMargin

    spark.sql(query_total_monthly)
        .coalesce(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTradeRankingsTotalMonthlyTableName}")
    spark.sql(query_monthly)
      .coalesce(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTradeRankingsMonthlyTableName}")
    spark.sql(query_week)
      .coalesce(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTradeRankingsWeekTableName}")

  }

}
