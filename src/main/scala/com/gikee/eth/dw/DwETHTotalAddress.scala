package com.gikee.eth.dw

import com.gikee.common.CommonConstant
import com.gikee.util.DateTransform
import org.apache.spark.sql.{SaveMode, SparkSession}

object DwETHTotalAddress {

  var readDwdDatabase, readDwdETHToAddressTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDwdDatabase = spark.sparkContext.getConf.get("spark.dwETHTotalAddress.readDwdDatabase")
    readDwdETHToAddressTableName = spark.sparkContext.getConf.get("spark.dwETHTotalAddress.readDwdETHToAddressTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTotalAddress.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwETHTotalAddress.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTotalAddress.transactionDate")

    getDwETHTotalAddress(spark)

    spark.stop()

  }

  def getDwETHTotalAddress(spark: SparkSession): Unit = {

    if (transactionDate != "") {
      val query_everyday_sql =
        s"""
           |
           |select
           |    sum(cast(value_num as bigint)),max(transaction_date) as transaction_date
           |from
           |(
           |    select
           |        count(1) as value_num,transaction_date
           |    from ${readDwdDatabase}.${readDwdETHToAddressTableName}
           |    where
           |        transaction_date = '${transactionDate}'
           |    group by transaction_date
           |    union all
           |    select
           |        cast(value_num as bigint) as value_num, transaction_date
           |    from ${writeDataBase}.${writeTableName}
           |    where transaction_date = '${DateTransform.getBeforeDate(transactionDate, CommonConstant.FormatDay, -1)}'
           |)t
           |
      """.stripMargin
      spark.sql(query_everyday_sql).repartition(1).write.mode(SaveMode.Append).insertInto(s"${writeDataBase}.${writeTableName}")
    } else {
      val query_sql =
        s"""
           |
           |select
           |    sum(count(1)) over (order by transaction_date ) as value_num,transaction_date
           |from ${readDwdDatabase}.${readDwdETHToAddressTableName}
           |group by transaction_date
           |
      """.stripMargin
      spark.sql(query_sql).sortWithinPartitions("transaction_date")
        .repartition(1).write.mode(SaveMode.Overwrite).insertInto(s"${writeDataBase}.${writeTableName}")
    }

  }

}
