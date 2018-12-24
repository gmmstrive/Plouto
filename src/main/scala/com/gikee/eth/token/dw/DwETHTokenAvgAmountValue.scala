package com.gikee.eth.token.dw

import com.gikee.common.{CommonConstant, PerfLogging}
import com.gikee.util.{DateTransform, TableUtil}
import org.apache.spark.sql.SparkSession

object DwETHTokenAvgAmountValue {

  var readDatabase, readTableName, readDmDatabase, readDmTableName, writeDataBase, writeTableName, transactionDate: String = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    readDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenAvgAmountValue.readDatabase")
    readTableName = spark.sparkContext.getConf.get("spark.dwETHTokenAvgAmountValue.readTableName")
    readDmDatabase = spark.sparkContext.getConf.get("spark.dwETHTokenAvgAmountValue.readDmDatabase")
    readDmTableName = spark.sparkContext.getConf.get("spark.dwETHTokenAvgAmountValue.readDmTableName")
    writeDataBase = spark.sparkContext.getConf.get("spark.dwETHTokenAvgAmountValue.writeDataBase")
    writeTableName = spark.sparkContext.getConf.get("spark.dwETHTokenAvgAmountValue.writeTableName")
    transactionDate = spark.sparkContext.getConf.get("spark.dwETHTokenAvgAmountValue.transactionDate")

    getDwETHTokenAvgAmountValue(spark)

    spark.stop()

  }


  def getDwETHTokenAvgAmountValue(spark: SparkSession): Unit = {

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
         |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
         |from
         |(select token_address,transaction_date from ${readDmDatabase}.${readDmTableName} where transaction_date = '${transactionDate}' ) t1
         |left join(
         |    select
         |        (sum(cast(value as double))/count(1)) as value_num, token_address,transaction_date
         |    from ${readDatabase}.${readTableName} where amount > '0' and transaction_date = '${transactionDate}'
         |    group by
         |        token_address,transaction_date
         |) t2
         |on
         |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
         |
      """.stripMargin

    val query_all_everyday_sql =
      s"""
         |
        |select
         |    if(t2.token_address is null,0,t2.value_num) as value_num,t1.token_address,t1.transaction_date
         |from
         |(select token_address,transaction_date from ${readDmDatabase}.${readDmTableName} ) t1
         |left join(
         |    select
         |        (sum(cast(value as double))/count(1)) as value_num, token_address,transaction_date
         |    from ${readDatabase}.${readTableName} where amount > '0'
         |    group by
         |        token_address,transaction_date
         |) t2
         |on
         |    t1.transaction_date = t2.transaction_date and t1.token_address = t2.token_address
         |
      """.stripMargin

    val tempDF = if (transactionDate != "") spark.sql(query_everyday_sql) else spark.sql(query_all_everyday_sql)

    import spark.implicits._

    val targetDF = tempDF.rdd.map(x => {
      val value_num = BigDecimal(x.get(0).toString).bigDecimal.toPlainString
      val token_address = x.get(1).toString
      val transaction_date = x.get(2).toString
      (value_num, token_address, transaction_date)
    }).toDF("value_num", "token_address", "transaction_date")

    TableUtil.writeDataStream(spark, targetDF, prefixPath, tmpPath, targetPath, "transaction_date")
    TableUtil.refreshPartition(spark, targetDF, writeDataBase, writeTableName, "transaction_date")

  }

}
