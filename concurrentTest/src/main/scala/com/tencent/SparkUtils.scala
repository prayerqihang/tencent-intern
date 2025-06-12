package com.tencent

import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.SparkSession

import java.util

object SparkUtils {
  def getLaunch(appName: String, appClass: String, args: Array[String]): SparkLauncher = {
    /**
     * appName Spark 应用名称
     * appClass Spark 应用的主类（包含 main 方法的类）
     * args 传递给 Spark 应用的命令行参数数组
     */
    // 通过反射获取当前类 SparkConcurrentTest 所在的 jar 包路径
    val jarPath = SparkConcurrentTest.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val env = new java.util.HashMap[String, String]()
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1")  // 打印启动命令
    env.put("SPARK_HOME", "/usr/local/service/spark")
    env.put("HADOOP_HOME", "/usr/local/service/hadoop")
    new SparkLauncher(env)
      .setAppResource(jarPath)
      .setMainClass(appClass)
      .setMaster("yarn")
      .setDeployMode("cluster")
      .setConf("spark.yarn.maxAppAttempts", "1")
      .setConf("spark.app.name", appName)
      .setConf("spark.driver.memory", "1g")
      .setConf("spark.ui.port", "0")  // UI 端口
      .setConf("spark.executor.memory", "1g")
      .setConf("spark.executor.instances", "8")
      .setConf("spark.executor.cores", "6")
      .setConf("spark.dynamicAllocation.enabled", "false")  // 禁止动态资源分配
      .setConf("spark.sql.files.openCostInBytes", "134217728")  // Spark SQL 文件打开成本参数
      .setConf("spark.task.maxFailures", "2")
      .addAppArgs(args: _*)
  }

  def sparkInit(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("app1")
      .config("spark.sql.hive.convertMetastoreOrc", "true")  // 采用 Spark 内置的 ORC 读取器代替 Hive
      .config("spark.sql.hive.convertMetastoreParquet", "true")  // 使用 Spark 内置 Parquet 读取器代替 Hive
      .config("spark.sql.hive.convertInsertingPartitionedTable", "true")  // 优化分区表插入
      .config("spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")  // 使用 Hadoop MapReduce 风格的输入输出提交协议
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.cosn",
        "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitterFactory")
      .config("spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("set spark.sql.sources.partitionOverwriteMode=DYNAMIC")  // 只覆盖实际写入的分区，而不是整个表
    spark.sql("set mapreduce.fileoutputcommitter.algorithm.version=2")  // 关键参数 1
    spark.sql("set mapreduce.fileoutputcommitter.task.cleanup.enabled=true")  // 关键参数 2
    spark.sql("set mapreduce.fileoutputcommitter.cleanup.skipped=true")  // 关键参数 3
    spark.sql("set spark.sql.orc.compression.codec= zlib")
    spark
  }

  def cleanupProcesses(processes: util.ArrayList[Process]): Unit = {
    processes.forEach(process => {
      if (process != null) {
        try {
          process.destroyForcibly()  // 强制终止线程
        } catch {
          case e: Throwable =>
            println(s"Error destroying process")
        }
      }
    })
    processes.clear()  // 清理集合
  }

}
