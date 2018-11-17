package com.gikee.eth

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{SaveMode, SparkSession}

object TestArray {

  val str = "[\"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef\",\"0x009f837f1feddc3de305fab200310a83d2871686078dab617c02b44360c9e236\",\"0x009f837f1feddc3de305fab200310a83d2871686078dab617c02b44360c9e236\"]"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().master("local[*]").getOrCreate()

    import spark.implicits._

    spark.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://106.14.200.2:3306/gikee_api2?user=lyjm_data&password=lyjm_python",
      "dbtable"->"gikee_api2.coinBaseInfo"
    )).load()//.show(false)
      .createTempView("coinBaseInfo")

    spark.read.format("jdbc").options(Map(
      "url"->"jdbc:mysql://106.14.200.2:3306/gikee_api2?user=lyjm_data&password=lyjm_python",
      "dbtable"->"gikee_api2.coinEverydayInfo"
    )).load()
      //.show(false)
      //.createTempView("coinEverydayInfo")

    spark.read.textFile("/Users/lucas/File/无标题.csv").rdd.map(x=>{
     val arr = x.split(",")
      (arr(1),arr(2),arr(3),BigDecimal(arr(4)).bigDecimal.toPlainString)
    }).toDF("address","decimals","token_symbol","total_suply").createTempView("ethTokenAddress")
      //.write.mode(SaveMode.Overwrite).format("parquet").save("/Users/lucas/File/tmp")

    spark.sql(
      """
        |select
        |    *
        |from ethTokenAddress t1
        |left join
        |    (select id,symbol,if(timing == 'null',timing,'2018-11-13') as timing from coinBaseInfo) t2
        |on
        |    t1.token_symbol = t2.symbol
        |where t1.address = '0x78a73b6cbc5d183ce56e786f6e905cadec63547b'
      """.stripMargin).show(false)

    //if(t2.id is null,'',t2.id) as id,t1.*,if(t2.timing is null,'',t2.timing) as date_time


    spark.stop()

  }

}
