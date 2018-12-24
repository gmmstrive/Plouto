package com.gikee.util

import java.time._
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

  def getUTCDate(format: String): String = {
    LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format))
  }

  /**
    * 截取日期，返回年、月、日
    *
    * @param date
    * @return
    */
  def getDate2Detailed(date: String): (String, String, String, String, String) = {
    (date.substring(0, 4), date.substring(5, 7), date.substring(8, 10), date.substring(0, 10), date.substring(11, 13))
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

  /**
    * 获取当前时间的周一
    *
    * @param date
    * @param format
    * @return
    */
  def getMonday(date: String, format: String): String = {
    getBeforeDate(date, format, (LocalDate.parse(date).getDayOfWeek.getValue - DayOfWeek.MONDAY.getValue) * -1)
  }

  def main(args: Array[String]): Unit = {

    println(
      if (BigDecimal("7080001") <= 7080000) { if (BigDecimal("7080001") >= 4370000) 3 else 5 } else { 2 }
    )

  }

}
