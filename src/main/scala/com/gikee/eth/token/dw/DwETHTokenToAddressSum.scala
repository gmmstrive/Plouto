package com.gikee.eth.token.dw

import com.gikee.common.CommonConstant
import com.gikee.util.DateTransform
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 新增地址 每日数量、每周数量、每月数量 by lucas 20181125
  */
object DwETHTokenToAddressSum {

  var readDatabase, readTableName,readDmDatabase,readDmTokenListTableName,
  writeDataBase, writeEverydayTableName, writeWeekTableName, writeMonthTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.readDatabase")
    readTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.readTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.readDmDatabase")
    readDmTokenListTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.readDmTokenListTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.writeDataBase")
    writeEverydayTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.writeEverydayTableName")
    writeWeekTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.writeWeekTableName")
    writeMonthTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.writeMonthTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSum.transactionDate")

    getDwETHTokenToAddressSum(spark)

    spark.stop()

  }

  def getDwETHTokenToAddressSum(spark: SparkSession): Unit = {

    if (transactionDate != "") {

      val query_every_day =
        s"""
           |
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
           |from (select * from  ${readDmDatabase}.${readDmTokenListTableName} where transaction_date = '${transactionDate}' ) t1
           |left join
           |(
           |    select
           |        count(1) as value_num,token_address,transaction_date
           |    from ${readDatabase}.${readTableName}
           |    where
           |        transaction_date = '${transactionDate}'
           |    group by
           |        token_address,transaction_date
           |) t2
           |on
           |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
           |
      """.stripMargin

      val query_week_day =
        s"""
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_week_mo as transaction_date
           |from (
           |    select
           |        token_address,
           |        transaction_date,
           |        next_day(date_sub(from_unixtime(unix_timestamp(),transaction_date),7),'MO') as transaction_week_mo
           |    from ${readDmDatabase}.${readDmTokenListTableName}
           |    where
           |        transaction_date = '${transactionDate}'
           |) t1
           |left join
           |(
           |    select
           |        count(1) as value_num,
           |        token_address,
           |        transaction_date,
           |        transaction_week_mo
           |    from
           |    (
           |        select
           |            token_address,
           |            next_day(date_sub(from_unixtime(unix_timestamp(),transaction_date),7),'MO') as transaction_week_mo,
           |            transaction_date
           |        from ${readDatabase}.${readTableName}
           |        where
           |            transaction_date >= '${DateTransform.getMonday(transactionDate, CommonConstant.FormatDay)}'
           |    )t
           |    group by token_address,transaction_date,transaction_week_mo
           |) t2
           |on
           |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
           |
      """.stripMargin

      val query_month_day =
        s"""
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,substr(cast(date_sub(t1.last_date,dayofmonth(t1.last_date)-1) as string),1,10) as transaction_date
           |from (
           |    select
           |        token_address,
           |        substr(cast(last_day(transaction_date) as string),1,10) as last_date
           |    from ${readDmDatabase}.${readDmTokenListTableName}
           |    where
           |        transaction_date rlike '${transactionDate.substring(0, 7)}'
           |    group by
           |        token_address,substr(cast(last_day(transaction_date) as string),1,10)
           |) t1
           |left join
           |(
           |    select
           |        count(1) as value_num,token_address,substr(cast(last_day(transaction_date) as string) ,1,10) as last_date
           |    from ${readDatabase}.${readTableName}
           |    where
           |        transaction_date rlike '${transactionDate.substring(0, 7)}'
           |    group by
           |        token_address,last_day(transaction_date)
           |) t2
           |on
           |    t1.last_date = t2.last_date and t1.token_address = t2.token_address
      """.stripMargin

      spark.sql(query_every_day).repartition(1).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeEverydayTableName}")
      spark.sql(query_week_day).repartition(1).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeWeekTableName}")
      spark.sql(query_month_day).repartition(1).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeMonthTableName}")

    } else {

      val query_all_every_day =
        s"""
           |
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
           |from ${readDmDatabase}.${readDmTokenListTableName} t1
           |left join
           |(
           |    select
           |        count(1) as value_num,token_address,transaction_date
           |    from ${readDatabase}.${readTableName}
           |    group by
           |        token_address,transaction_date
           |) t2
           |on
           |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
           |
      """.stripMargin

      val query_all_week_day =
        s"""
           |
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_week_mo as transaction_date
           |from (
           |    select
           |        token_address,
           |        transaction_week_mo,
           |        date_add(next_day(date_sub(from_unixtime(unix_timestamp(),transaction_week_mo),7),'SU'),7) as transaction_week_su
           |    from
           |    (
           |        select
           |            token_address,transaction_date,next_day(date_sub(from_unixtime(unix_timestamp(),transaction_date),7),'MO') as transaction_week_mo
           |        from ${readDmDatabase}.${readDmTokenListTableName}
           |    )t
           |    group by
           |        token_address,transaction_week_mo
           |) t1
           |left join
           |(
           |    select
           |        count(1) as value_num,
           |        token_address,
           |        transaction_week_mo,
           |        date_add(next_day(date_sub(from_unixtime(unix_timestamp(),transaction_week_mo),7),'SU'),7) as transaction_week_su
           |    from
           |    (
           |        select
           |            token_address,
           |            next_day(date_sub(from_unixtime(unix_timestamp(),transaction_date),7),'MO') as transaction_week_mo,
           |            transaction_date
           |        from ${readDatabase}.${readTableName}
           |    )t
           |    group by token_address,transaction_week_mo
           |) t2
           |on
           |    t1.transaction_week_su = t2.transaction_week_su and t1.token_address = t2.token_address
           |
      """.stripMargin

      val query_all_month_day =
        s"""
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,substr(cast(date_sub(t1.last_date,dayofmonth(t1.last_date)-1) as string),1,10) as transaction_date
           |from (
           |    select
           |        token_address,
           |        substr(cast(last_day(transaction_date) as string),1,10) as last_date
           |    from ${readDmDatabase}.${readDmTokenListTableName}
           |    group by
           |        token_address,substr(cast(last_day(transaction_date) as string),1,10)
           |) t1
           |left join
           |(
           |    select
           |        count(1) as value_num,token_address,substr(cast(last_day(transaction_date) as string) ,1,10) as last_date
           |    from ${readDatabase}.${readTableName}
           |    group by
           |        token_address,last_day(transaction_date)
           |) t2
           |on
           |    t1.last_date = t2.last_date and t1.token_address = t2.token_address
      """.stripMargin

      spark.sql(query_all_every_day).repartition(1).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeEverydayTableName}")
      spark.sql(query_all_week_day).repartition(1).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeWeekTableName}")
      spark.sql(query_all_month_day).repartition(1).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeMonthTableName}")

    }

  }

}
