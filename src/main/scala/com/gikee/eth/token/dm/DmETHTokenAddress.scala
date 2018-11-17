package com.gikee.eth.token.dm

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 每天更新 token address
  */
object DmETHTokenAddress {

  var mysqlDataBase, mysqlTableName, writeDataBase, writeTableName: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    mysqlDataBase = sc.getConf.get("spark.dmETHTokenAddress.mysqlDataBase")
    mysqlTableName = sc.getConf.get("spark.dmETHTokenAddress.mysqlTableName")
    writeDataBase = sc.getConf.get("spark.dmETHTokenAddress.writeDatabase")
    writeTableName = sc.getConf.get("spark.dmETHTokenAddress.writeTableName")

    getDmETHTokenAddress(spark)

    spark.stop()

  }

  def getDmETHTokenAddress(spark: SparkSession): Unit = {

    import spark.implicits._

    spark.read.format("jdbc")
      .options(Map(
        "url" -> s"jdbc:mysql://106.14.200.2:3306/${mysqlDataBase}",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> s"${mysqlTableName}",
        "user" -> "lyjm_data",
        "password" -> "lyjm_python"
      )).load().selectExpr(
      "if(id is null,'',id) as id", "address", "decimals", "symbol as token_symbol", "totalSuply as total_suply", "date_time"
    ).rdd.map(x => {
      val id = x.get(0).toString
      val address = x.get(1).toString.toLowerCase
      val decimals = x.get(2).toString
      val token_symbol = x.get(3).toString
      val total_suply = BigDecimal(x.get(4).toString).bigDecimal.toPlainString
      val date_time = x.get(5).toString
      (id, address, decimals, token_symbol, total_suply, date_time)
    }).toDF("id", "address", "decimals", "token_symbol", "total_suply", "date_time")
      .coalesce(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTableName}")

  }

}
