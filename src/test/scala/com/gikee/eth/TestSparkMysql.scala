package com.gikee.eth

import org.apache.spark.sql.{SaveMode, SparkSession}

object TestSparkMysql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().master("local[*]").getOrCreate()

    spark.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://106.14.200.2:3306/gikee_api2?user=lyjm_data&password=lyjm_python",
      "dbtable"->"gikee_api2.coinBaseInfo"
    )).load().createTempView("coinBaseInfo")

    spark.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://106.14.200.2:3306/gikee_api2?user=lyjm_data&password=lyjm_python",
      "dbtable"->"gikee_api2.ethTokenAddress"
    )).load().createTempView("ethTokenAddress")

    spark.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://106.14.200.2:3306/gikee_api2?user=lyjm_data&password=lyjm_python",
      "dbtable"->"gikee_api2.coinEverydayInfo"
    )).load().createTempView("coinEverydayInfo")
//
//    spark.sql("select * from coinBaseInfo").show(false)
//
//    spark.sql("select * from ethTokenAddress").show(false)

    val query_str =
      """
        |select
        |    tt.symbol as token_symbol,cast(t3.priceUs as string) price_us,cast(t3.time as string) as date_time
        |from
        |(
        |    select
        |            t2.id , t1.symbol
        |    from ethTokenAddress t1
        |    left join
        |        (select id, symbol from coinBaseInfo ) t2
        |    on
        |        t1.symbol = t2.symbol
        |    where
        |        t2.symbol is not null
        |) tt
        |left join
        |    ( select id,priceUs,time from coinEverydayInfo ) t3
        |on
        |   tt.id = t3.id
        |where
        |    t3.id is not null
      """.stripMargin

    println()

    spark.sql(query_str).repartition(1).write.mode(SaveMode.Overwrite).format("parquet").save("/Users/lucas/File/tmp")

//    sc.read.format("jdbc").options(
//      Map("url" -> "jdbc:mysql://www.iteblog.com:3306/iteblog?user=iteblog&password=iteblog",
//        "dbtable" -> "iteblog")).load() //"dbtable"->"gikee_api2.ethTokenAddress"

  }

}
