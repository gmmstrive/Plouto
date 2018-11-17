package com.gikee.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.gikee.common.CommonConstant

/**
  * 解析 Json by lucas 20181114
  */
object ParsingJson {

  /**
    * 解析 JSONObject 获取 String
    *
    * @param json
    * @param colName
    * @return
    */
  def getStrTrim(json: JSONObject, colName: String): String = {
    var str = ""
    if (json != null) {
      val value = json.getString(colName)
      if (value != null) str = value.trim
    }
    str
  }

  /**
    * 解析 JSONObject 获取 JSONArray ，如果为 null || size < 0 return null 否则返回 JSONArray
    *
    * @param json
    * @param colName
    * @return
    */
  def getStrArray(json: JSONObject, colName: String): JSONArray = {
    var jsonArray: JSONArray = null
    if (json != null) {
      val value = json.getJSONArray(colName)
      if (value != null && value.size() > 0) jsonArray = value
    }
    jsonArray
  }

  /**
    * 解析 JSONObject 获取 UTC 日期，返回年、月、日、time
    *
    * @param json
    * @param colName
    * @return
    */
  def getStrDate(json: JSONObject, colName: String): (String, String, String, String, String) = {
    var dateTime = ""
    var annual = ""
    var monthly = ""
    var everyday = ""
    var transaction_date = ""
    val date = getStrTrim(json, colName)
    if (date != "") {
      dateTime = DateTransform.getLong2date(date, CommonConstant.FormatDate)
      annual = DateTransform.getDate2Detailed(dateTime)._1
      monthly = DateTransform.getDate2Detailed(dateTime)._2
      everyday = DateTransform.getDate2Detailed(dateTime)._3
      transaction_date = DateTransform.getDate2Detailed(dateTime)._4
    }
    (dateTime, annual, monthly, everyday, transaction_date)
  }

}
