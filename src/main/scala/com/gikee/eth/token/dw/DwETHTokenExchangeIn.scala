package com.gikee.eth.token.dw

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.TableUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 交易所流入 天
  */
object DwETHTokenExchangeIn {

  var readDatabase, readTableName, readDmDatabase, readDmTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenExchangeIn.readDatabase")
    readTableName = spark.sparkContext.getConf.get("spark.dwETHTokenExchangeIn.readTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenExchangeIn.readDmDatabase")
    readDmTableName = spark.sparkContext.getConf.get("spark.dwETHTokenExchangeIn.readDmTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTokenExchangeIn.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwETHTokenExchangeIn.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTokenExchangeIn.transactionDate")

    getDwETHTokenExchangeIn(spark)

    spark.stop()

  }

  def getDwETHTokenExchangeIn(spark: SparkSession): Unit = {

    val prefixPath = CommonConstant.outputRootDir
    val tmpPath = CommonConstant.getTmpPath(writeDataBase, writeTableName, System.currentTimeMillis().toString)
    val targetPath = CommonConstant.getTargetPath(writeDataBase, writeTableName)

    if (tmpPath == null || targetPath == null) {
      PerfLogging.error("临时目录或者目标目录为 Null")
      throw new IllegalArgumentException("tmpPath or targetPath is null")
    }

    val query_everyday_sql =
      s"""
         |
        |select
         |    t1.value_num,t1.to_address,t1.token_address,t1.transaction_date
         |from (
         |    select
         |        sum(cast(value as double)) as value_num,to_address,token_address,transaction_date
         |    from ${readDatabase}.${readTableName} where transaction_date = '${transactionDate}'
         |    group by
         |        to_address,token_address,transaction_date
         |) t1
         |left join
         |    ${readDmDatabase}.${readDmTableName} t2
         |on
         |    t1.to_address = t2.address
         |where
         |    t2.address is not null
         |
        """.stripMargin

    val query_all_sql =
      s"""
         |
        |select
         |    t1.value_num,t1.to_address,t1.token_address,t1.transaction_date
         |from (
         |    select
         |        sum(cast(value as double)) as value_num,to_address,token_address,transaction_date
         |    from ${readDatabase}.${readTableName}
         |    group by
         |        to_address,token_address,transaction_date
         |) t1
         |left join
         |    ${readDmDatabase}.${readDmTableName} t2
         |on
         |    t1.to_address = t2.address
         |where
         |    t2.address is not null
         |
      """.stripMargin

    val tempDF = if (transactionDate != "") spark.sql(query_everyday_sql) else spark.sql(query_all_sql)

    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val value_num = BigDecimal(x.get(0).toString).bigDecimal.toPlainString
      val address = x.get(1).toString
      val token_address = x.get(2).toString
      val transaction_date = x.get(3).toString
      (value_num, address, token_address, transaction_date)
    }).toDF("value_num", "address", "token_address", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
