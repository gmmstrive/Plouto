package com.gikee.common

import org.slf4j.LoggerFactory

object PerfLogging {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def trace(msg: String): Unit = {
    logger.trace(msg)
  }

}
