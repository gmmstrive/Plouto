package com.gikee.util

import java.time.{Instant, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.gikee.common.CommonConstant

/**
  * date utils by lucas 20180824
  */
object DateTransform {

  /**
    * 输入时间戳返回 UTC DATE
    *
    * @param str 时间戳
    * @return date
    */
  def getLong2date(str: String, format: String): String = {
    if (ProcessingString.verify(str, "Long")) {
      LocalDateTime.parse(Instant.ofEpochSecond(str.toLong).toString, DateTimeFormatter.ofPattern(CommonConstant.FormatUTCDate))
        .format(DateTimeFormatter.ofPattern(format))
    } else {
      ""
    }
  }

  /**
    * 截取日期，返回年、月、日
    *
    * @param date
    * @return
    */
  def getDate2Detailed(date: String): (String, String, String, String) = {
    (date.substring(0, 4), date.substring(5, 7), date.substring(8, 10), date.substring(0, 10))
  }

  /**
    * 获取 num 天之前的日期，返回 time
    *
    * @param date
    * @param num
    * @return
    */
  def getBeforeDate(date: String, format: String, num: Int): String = {
    val date_time = LocalDate.parse(date, DateTimeFormatter.ofPattern(format)).plusDays(num).toString
    (date_time)
  }

}
