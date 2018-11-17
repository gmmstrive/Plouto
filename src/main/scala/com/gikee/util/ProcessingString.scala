package com.gikee.util

import java.math.BigInteger

import scala.util.{Success, Try}

/**
  * 字符串处理 by lucas 2018-11-08
  */
object ProcessingString {

  /**
    * 16 进制转换到 10 进制,包含 "0x" 则需要删除 0x 前缀
    *
    * @param str
    */
  def dataHexadecimalTransform(str: String, hex: Int): String = {
    var value: String = null
    if (str != "" && str != null) {
      value = new BigInteger(str, hex).toString()
    }
    value
  }

  /**
    * 判断是否可转指定类型数据
    *
    * @param str
    * @param msg
    * @return
    */
  def verify(str: String, msg: String): Boolean = {
    var c: Try[Any] = null
    if ("Long".equals(msg)) {
      c = scala.util.Try(str.toLong)
    } else if ("Double".equals(msg)) {
      c = scala.util.Try(str.toDouble)
    }
    c match {
      case Success(_) => true
      case _ => false
    }
  }

}
