package com.gikee.common

import java.util.UUID

object CommonConstant {

  val outputRootDir = "/gikee"

  // get tmpPath /tmp/temp/temp_eth_source_refresh_time
  def getTmpPath(databaseName: String, tableName: String, timesTamp: String): String = {
    if (databaseName != null && databaseName != "" && tableName != null && tableName != "") s"/tmp/${databaseName}/${tableName}/dt=${timesTamp}" else null
  }

  // get tarGetPath   /gikee/temp/temp_eth_source_refresh_time
  // '/gikee/dw/dwd_eth_token_transaction'
  def getTargetPath(databaseName: String, tableName: String): String = {
    if (databaseName != null && databaseName != "" && tableName != null && tableName != "") s"/${databaseName}/${tableName}" else null
  }

  /**
    * 日期格式
    */
  val FormatDay = "yyyy-MM-dd"
  val FormatDate = "yyyy-MM-dd HH:mm:ss"
  val FormatUTCDate = "yyyy-MM-dd'T'HH:mm:ss'Z'"
  val FormatAnnual = "yyyy"
  val FormatMonthly = "MM"
  val FormatEveryday = "dd"


  /**
    * get UUID
    *
    * @return
    */
  def getUUID(): String = {
    UUID.randomUUID.toString.split("-")(0)
  }

}
