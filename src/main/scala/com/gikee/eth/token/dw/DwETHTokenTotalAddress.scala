package com.gikee.eth.token.dw

import com.gikee.common.CommonConstant
import com.gikee.util.DateTransform
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * 总交易地址
  */
object DwETHTokenTotalAddress {

  var readDwdDatabase, readDwdETHTokenToAddressTableName, readDmDatabase, readDmTokenListTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDwdDatabase = spark.sparkContext.getConf.get("spark.dwdETHTokenTotalAddress.readDwdDatabase")
    readDwdETHTokenToAddressTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTotalAddress.readDwdETHTokenToAddressTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwdETHTokenTotalAddress.readDmDatabase")
    readDmTokenListTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTotalAddress.readDmTokenListTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwdETHTokenTotalAddress.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwdETHTokenTotalAddress.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwdETHTokenTotalAddress.transactionDate")

    getDwdETHTokenTotalAddress(spark)

    spark.stop()

  }

  def getDwdETHTokenTotalAddress(spark: SparkSession): Unit = {

    if (transactionDate != "") {
      val query_everyday_sql =
        s"""
           |select
           |    sum(value_num) as value_num,token_address,max(transaction_date) as value_num
           |from(
           |    select
           |        if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
           |    from (select * from  ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}' ) t1
           |    left join
           |    (
           |        select
           |            sum(value_num) over ( partition by token_address order by transaction_date )as value_num,token_address,transaction_date
           |        from (select count(1) as value_num,token_address, transaction_date from ${readDwdDatabase}.${readDwdETHTokenToAddressTableName} where transaction_date = '${transactionDate}' group by transaction_date,token_address) t
           |    ) t2
           |    on
           |        t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
           |    union all
           |    select cast(value_num as bigint) as value_num,token_address,transaction_date from ${writeDataBase}.${writeTableName} where transaction_date = '${DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -1)}'
           |)t
           |group by
           |    token_address
      """.stripMargin
      spark.sql(query_everyday_sql).repartition(1).write.mode(SaveMode.Overwrite).insertInto(s"${writeDataBase}.${writeTableName}")
    } else {
      val query_sql =
        s"""
           |select
           |    max(value_num) over ( partition by token_address order by transaction_date )as value_num,token_address,transaction_date
           |from
           |(
           |    select
           |        if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
           |    from (select * from  ${readDmDatabase}.${readDmTokenListTableName} ) t1
           |    left join
           |    (
           |        select
           |            sum(value_num) over ( partition by token_address order by transaction_date )as value_num,token_address,transaction_date
           |        from (select count(1) as value_num,token_address, transaction_date from ${readDwdDatabase}.${readDwdETHTokenToAddressTableName} group by transaction_date,token_address) t
           |    ) t2
           |    on
           |        t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
           |)t
           |
      """.stripMargin
      spark.sql(query_sql).sortWithinPartitions("transaction_date").repartition(1).write.mode(SaveMode.Overwrite).insertInto(s"${writeDataBase}.${writeTableName}")
    }

  }

}
