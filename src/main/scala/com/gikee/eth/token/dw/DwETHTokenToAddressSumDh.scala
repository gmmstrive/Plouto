package com.gikee.eth.token.dw

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 新增地址 每小时 by lucas 20181219
  */
object DwETHTokenToAddressSumDh {

  var readDatabase, readTableName, readDmDatabase, readDmTokenListTableName,
  writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSumDh.readDatabase")
    readTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSumDh.readTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSumDh.readDmDatabase")
    readDmTokenListTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSumDh.readDmTokenListTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSumDh.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSumDh.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTokenToAddressSumDh.transactionDate")

    getDwETHTokenToAddressSumDh(spark)

    spark.stop()
  }

  def getDwETHTokenToAddressSumDh(spark: SparkSession): Unit = {

    if (transactionDate != "") {
      val token_list_sql =
        s"""
          |select token_address,'00' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'01' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'02' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'03' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'04' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'05' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'06' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'07' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'08' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'09' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'10' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'11' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'12' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'13' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'14' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'15' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'16' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'17' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'18' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'19' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'20' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'21' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'22' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
          |union all
          |select token_address,'23' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}  where transaction_date = '${transactionDate}'
        """.stripMargin

      spark.sql(token_list_sql).createTempView("token_list")

      spark.sql(
        s"""
           |select
           |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,concat(t1.transaction_date,' ',if(t2.token_address is null,t1.dh,t2.dh),':00:00') as dh,
           |    t1.transaction_date
           |from (
           |    select
           |        token_address,dh,transaction_date
           |    from token_list
           |) t1
           |left join(
           |    select
           |        count(1) as value_num,token_address,dh,transaction_date
           |    from ${readDatabase}.${readTableName} where transaction_date = '${transactionDate}'
           |    group by token_address,dh,transaction_date
           |)t2
           |on
           |    t1.token_address = t2.token_address and t1.dh = t2.dh and t1.transaction_date = t2.transaction_date
           |where
           |    t1.dh <= (select max(dh) from ${readDatabase}.${readTableName} where transaction_date = '${transactionDate}')
        """.stripMargin).repartition(1).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeTableName}")

    } else {

      val token_list_sql =
        s"""
          |select token_address,'00' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'01' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'02' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'03' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'04' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'05' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'06' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'07' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'08' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'09' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'10' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'11' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'12' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'13' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'14' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'15' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'16' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'17' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'18' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'19' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'20' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'21' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'22' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
          |union all
          |select token_address,'23' as dh,transaction_date from ${readDmDatabase}.${readDmTokenListTableName}
        """.stripMargin

      spark.sql(token_list_sql).createTempView("token_list")

      spark.sql(
        s"""
          |select
          |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,concat(t1.transaction_date,' ',if(t2.token_address is null,t1.dh,t2.dh),':00:00') as dh,
          |    t1.transaction_date
          |from (
          |    select
          |        token_address,dh,transaction_date
          |    from token_list
          |    group by token_address,dh,transaction_date
          |) t1
          |left join(
          |    select
          |        count(1) as value_num,token_address,dh,transaction_date
          |    from ${readDatabase}.${readTableName}
          |    group by token_address,dh,transaction_date
          |)t2
          |on
          |    t1.token_address = t2.token_address and t1.dh = t2.dh and t1.transaction_date = t2.transaction_date
        """.stripMargin).write.mode(SaveMode.Overwrite)
        .insertInto(s"${writeDataBase}.${writeTableName}")
    }

  }

}
