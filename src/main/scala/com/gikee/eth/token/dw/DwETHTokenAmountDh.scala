package com.gikee.eth.token.dw

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.SparkSession

object DwETHTokenAmountDh {

  var readDatabase, readTableName, readDmDatabase, readDmTableName,
  writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenAmountDh.readDatabase")
    readTableName = spark.sparkContext.getConf.get("spark.dwETHTokenAmountDh.readTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenAmountDh.readDmDatabase")
    readDmTableName = spark.sparkContext.getConf.get("spark.dwETHTokenAmountDh.readDmTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTokenAmountDh.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwETHTokenAmountDh.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTokenAmountDh.transactionDate")

    getDwETHTokenAmountDh(spark)

    spark.stop()

  }

  def getDwETHTokenAmountDh(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val token_list_sql =
      s"""
         |select token_address,'00' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'01' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'02' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'03' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'04' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'05' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'06' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'07' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'08' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'09' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'10' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'11' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'12' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'13' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'14' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'15' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'16' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'17' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'18' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'19' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'20' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'21' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'22' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
         |union all
         |select token_address,'23' as dh,transaction_date from ${readDmDatabase}.${readDmTableName}  where transaction_date = '${transactionDate}'
        """.stripMargin

    spark.sql(token_list_sql).createTempView("token_list")

    val tempDF = spark.sql(
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
         |        sum(nvl(cast(amount as double),0)) as value_num,token_address,dh,transaction_date
         |    from ${readDatabase}.${readTableName} where transaction_date = '${transactionDate}'
         |    group by token_address,dh,transaction_date
         |)t2
         |on
         |    t1.token_address = t2.token_address and t1.dh = t2.dh and t1.transaction_date = t2.transaction_date
         |where
         |    t1.dh <= (select max(dh) from ${readDatabase}.${readTableName} where transaction_date = '${transactionDate}')
        """.stripMargin)

    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val value_num = BigDecimal(x.get(0).toString).bigDecimal.toPlainString
      val token_address = x.get(1).toString
      val transaction_date = x.get(2).toString
      val trade_date = x.get(3).toString
      (value_num, token_address, transaction_date, trade_date)
    }).toDF("value_num", "token_address", "transaction_date", "trade_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "trade_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "trade_date")

  }

}
