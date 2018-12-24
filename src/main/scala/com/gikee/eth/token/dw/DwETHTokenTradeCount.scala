package com.gikee.eth.token.dw

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

object DwETHTokenTradeCount {

  var readDatabase, readTableName, readDmDatabase, readDmTableName,
  writeDataBase, writeEverydayTableName, writeWeekTableName, writeMonthTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.readDatabase")
    readTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.readTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.readDmDatabase")
    readDmTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.readDmTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.writeDataBase")
    writeEverydayTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.writeEverydayTableName")
    writeWeekTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.writeWeekTableName")
    writeMonthTableName = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.writeMonthTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTokenTradeCount.transactionDate")

    getDwETHTokenTradeCount(spark)

    spark.stop()

  }

  def getDwETHTokenTradeCount(spark: SparkSession): Unit = {
    getDwETHTokenTradeCountEveryday(spark)
    getDwETHTokenTradeCountWeek(spark)
    getDwETHTokenTradeCountMonth(spark)
  }

  def getDwETHTokenTradeCountEveryday(spark: SparkSession): Unit = {
    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeEverydayTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeEverydayTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val tempDF = if (transactionDate != "") {
      val query_everyday_sql =
        s"""
           |
        |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
           |from
           |(select token_address,transaction_date from ${readDmDatabase}.${readDmTableName} where transaction_date = '${transactionDate}') t1
           |left join(
           |select
           |    count(1) as value_num,token_address,transaction_date
           |from ${readDatabase}.${readTableName} where transaction_date = '${transactionDate}'
           |group by
           |    token_address,transaction_date
           |) t2
           |on
           |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
           |
      """.stripMargin
      spark.sql(query_everyday_sql)
    } else {
      val query_all_everyday_sql =
        s"""
           |
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
           |from
           |(select token_address,transaction_date from ${readDmDatabase}.${readDmTableName} ) t1
           |left join(
           |select
           |    count(1) as value_num,token_address,transaction_date
           |from ${readDatabase}.${readTableName}
           |group by
           |    token_address,transaction_date
           |) t2
           |on
           |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
           |
      """.stripMargin
      spark.sql(query_all_everyday_sql)
    }
    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val value_num = BigDecimal(x.get(0).toString).bigDecimal.toPlainString
      val token_address = x.get(1).toString
      val transaction_date = x.get(2).toString
      (value_num, token_address, transaction_date)
    }).toDF("value_num", "token_address", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeEverydayTableName, "transaction_date")

  }

  def getDwETHTokenTradeCountWeek(spark: SparkSession): Unit = {
    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeWeekTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeWeekTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val tempDF = if (transactionDate != "") {
      val query_week_sql =
        s"""
           |
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_week_mo as transaction_date
           |from (
           |    select
           |        token_address,
           |        transaction_date,
           |        next_day(date_sub(from_unixtime(unix_timestamp(),transaction_date),7),'MO') as transaction_week_mo
           |    from ${readDmDatabase}.${readDmTableName}
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
      spark.sql(query_week_sql)
    } else {
      val query_all_week_sql =
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
           |        from ${readDmDatabase}.${readDmTableName}
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
           |            value,
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
      spark.sql(query_all_week_sql)
    }
    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val value_num = BigDecimal(x.get(0).toString).bigDecimal.toPlainString
      val token_address = x.get(1).toString
      val transaction_date = x.get(2).toString
      (value_num, token_address, transaction_date)
    }).toDF("value_num", "token_address", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeWeekTableName, "transaction_date")

  }

  def getDwETHTokenTradeCountMonth(spark: SparkSession): Unit = {
    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeMonthTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeMonthTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val tempDF = if (transactionDate != "") {
      val query_month_sql =
        s"""
           |
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,substr(cast(date_sub(t1.last_date,dayofmonth(t1.last_date)-1) as string),1,10) as transaction_date
           |from (
           |    select
           |        token_address,
           |        substr(cast(last_day(transaction_date) as string),1,10) as last_date
           |    from ${readDmDatabase}.${readDmTableName}
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
           |
      """.stripMargin
      spark.sql(query_month_sql)
    } else {
      val query_all_month_sql =
        s"""
           |
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,substr(cast(date_sub(t1.last_date,dayofmonth(t1.last_date)-1) as string),1,10) as transaction_date
           |from (
           |    select
           |        token_address,
           |        substr(cast(last_day(transaction_date) as string),1,10) as last_date
           |    from ${readDmDatabase}.${readDmTableName}
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
           |
      """.stripMargin
      spark.sql(query_all_month_sql)
    }
    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val value_num = BigDecimal(x.get(0).toString).bigDecimal.toPlainString
      val token_address = x.get(1).toString
      val transaction_date = x.get(2).toString
      (value_num, token_address, transaction_date)
    }).toDF("value_num", "token_address", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeMonthTableName, "transaction_date")

  }

}
