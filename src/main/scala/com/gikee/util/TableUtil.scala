package com.gikee.util

import com.gikee.common.CommonConstant
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * hive 分区表写入 and 刷新分区 by lucas 20180821
  */
object TableUtil {

  /**
    * 数据写入 hive 分区表
    *
    * @param spark
    * @param df
    * @param writeDataBase
    * @param writeTableName
    */
  def writeDataStream(spark: SparkSession, df: DataFrame, writeDataBase: String, writeTableName: String): Unit = {
    df.repartition(1).write.mode(SaveMode.Overwrite).format("parquet")
      .insertInto(s"${writeDataBase}.${writeTableName}")
  }

  def writeDataStream(spark: SparkSession, df: DataFrame, prefixPath: String, tmpPath: String, targetPath: String,
                      partition_1: String): Unit = {
    df.repartition(1).write.partitionBy(partition_1)
      .mode(SaveMode.Overwrite).format("parquet").save(prefixPath + tmpPath)
    FileExchange.dataMovement(spark, prefixPath, tmpPath, targetPath)
  }

  def writeDataStreams(spark: SparkSession, df: DataFrame, prefixPath: String, tmpPath: String, targetPath: String,
                      partition_1: String): Unit = {
    df.repartition(3).write.partitionBy(partition_1)
      .mode(SaveMode.Overwrite).format("parquet").save(prefixPath + tmpPath)
    FileExchange.dataMovement(spark, prefixPath, tmpPath, targetPath)
  }

  def writeDataStream(spark: SparkSession, df: DataFrame, prefixPath: String, tmpPath: String, targetPath: String,
                      partition_1: String, partition_2: String): Unit = {
    df.repartition(1).write.partitionBy(partition_1, partition_2)
      .mode(SaveMode.Overwrite).format("parquet").save(prefixPath + tmpPath)
    FileExchange.dataMovement(spark, prefixPath, tmpPath, targetPath)
  }

  def writeDataStream(spark: SparkSession, df: DataFrame, prefixPath: String, tmpPath: String, targetPath: String,
                      partition_1: String, partition_2: String, partition_3: String): Unit = {
    df.repartition(1).write.partitionBy(partition_1, partition_2, partition_3)
      .mode(SaveMode.Overwrite).format("parquet").save(prefixPath + tmpPath)
    FileExchange.dataMovement(spark, prefixPath, tmpPath, targetPath)
  }

  def writeDataStream(spark: SparkSession, df: DataFrame, prefixPath: String, tmpPath: String, targetPath: String,
                       partition_1: String, partition_2: String, partition_3: String, partition_4: String): Unit = {
    df.repartition(1).write.partitionBy(partition_1, partition_2, partition_3, partition_4)
      .mode(SaveMode.Overwrite).format("parquet").save(prefixPath + tmpPath)
    FileExchange.dataMovement(spark, prefixPath, tmpPath, targetPath)
  }


  /**
    * 刷新分区
    *
    * @param spark     SparkSession
    * @param df        DataFrame
    * @param dataBases 库
    * @param tableName 表
    * @param partition 分区
    *                  collect().foreach(spark.sql(_))
    */
  def refreshPartition(spark: SparkSession, df: DataFrame, databaseName: String, tableName: String, partition: String*): Unit = {

    if (partition.size == 2) {
      df.groupBy(partition(0), partition(1)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}',${partition(1)} = '${x(1)}')"
        (value)
      }).collect().foreach(spark.sql(_))
    } else if (partition.size == 3) {
      df.groupBy(partition(0), partition(1), partition(2)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}',${partition(1)} = '${x(1)}',${partition(2)} = '${x(2)}')"
        (value)
      }).collect().foreach(spark.sql(_))
    } else if (partition.size == 4) {
      df.groupBy(partition(0), partition(1), partition(2), partition(3)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}',${partition(1)} = '${x(1)}',${partition(2)} = '${x(2)}',${partition(3)} = '${x(3)}')"
        (value)
      }).collect().foreach(spark.sql(_))
    } else {
      df.groupBy(partition(0)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}')"
        (value)
      }).collect().foreach(spark.sql(_))
    }

  }

  def refreshPartitions(spark: SparkSession, df: DataFrame, databaseName: String, tableName: String, partition: String*): Unit = {

    val partitionFilePath = CommonConstant.outputRootDir + s"/tmp/partition/${databaseName}/${tableName}"

    import spark.implicits._

    if (partition.size == 2) {
      df.groupBy(partition(0), partition(1)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}',${partition(1)} = '${x(1)}')"
        (value)
      }).toDF("value").write.mode(SaveMode.Overwrite).format("textfile").save(partitionFilePath)
    } else if (partition.size == 3) {
      df.groupBy(partition(0), partition(1), partition(2)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}',${partition(1)} = '${x(1)}',${partition(2)} = '${x(2)}')"
        (value)
      }).toDF("value").write.mode(SaveMode.Overwrite).format("textfile").save(partitionFilePath)
    } else if (partition.size == 4) {
      df.groupBy(partition(0), partition(1), partition(2), partition(3)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}',${partition(1)} = '${x(1)}',${partition(2)} = '${x(2)}',${partition(3)} = '${x(3)}')"
        (value)
      }).toDF("value").write.mode(SaveMode.Overwrite).format("textfile").save(partitionFilePath)
    } else {
      df.groupBy(partition(0)).count().rdd.map(x => {
        val value = s"alter table ${databaseName}.${tableName} add if not exists partition (${partition(0)} = '${x(0)}')"
        (value)
      }).toDF("value").write.mode(SaveMode.Overwrite).format("textfile").save(partitionFilePath)
    }

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val filePathArray = fileSystem.listStatus(new Path(partitionFilePath))

    for (file <- filePathArray) {
      if (!"_SUCCESS".equals(file.getPath.getName)) {
        fileSystem.rename(file.getPath, new Path(s"${file.getPath.getParent.toString}/partition.sh"))
      }
    }

  }

}
