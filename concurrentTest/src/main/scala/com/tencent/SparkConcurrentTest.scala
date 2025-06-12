package com.tencent

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.util
import java.util.concurrent.{ConcurrentHashMap, Future, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.UUID

object SparkConcurrentTest extends Logging {

  def main(args: Array[String]): Unit = {
    val sourcePath = args(0)  // 造数的路径
    val recordCount = args(1).toInt  // 造数的记录数量
    val appCount = args(2).toInt  // 并发的 App 数量
    val tableLocation = args(3)  // 表路径

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .appName("sparkConcurrentTest")
      .getOrCreate()

    val tempView = "temp" + UUID.randomUUID().toString.replaceAll("-", "_")
    val tblName = "person" + UUID.randomUUID().toString.replaceAll("-", "_")

    var finish = false
    var runCount: Int = 0
    // 创建线程池
    val executor = new ThreadPoolExecutor(
      appCount,  // 核心线程数
      appCount,  // 最大线程数
      0L,  // 空闲线程存活时间
      TimeUnit.MILLISECONDS,  // 时间单位
      new LinkedBlockingQueue[Runnable](appCount)  // 等待队列，最多存放 appCount 个线程
    )
    // 给线程池设置一个线程工厂，后续创建线程时使用 - 这个线程工厂会创建守护线程。
    executor.setThreadFactory(new ThreadFactoryBuilder().setDaemon(true).build())

    val processes = new util.ArrayList[Process]()
    try {
      /**
       * ShutdownHookManager 是 Hadoop 中管理 JVM 关闭钩子的一个工具类
       * get().addShutdownHook 获取一个实例，并注册一个关闭钩子（会在 JVM 进程正常退出时执行）
       */
      ShutdownHookManager.get().addShutdownHook (new Runnable() {
        override def run(): Unit = {
          // 关闭钩子的具体逻辑
          println("shutdown hook 执行")
          // 清理表
          spark.sql(s"drop table if exists ${tblName}")
          // 清理表对应的物理文件
          val path = new Path(tableLocation)
          val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
          if (fs.exists(path) && tableLocation.contains("/person_") ) {
            fs.delete(path, true)
          }
          // 自定义工具类方法：清理存放在 process 列表中的所有外部进程（强制终止）
          SparkUtils.cleanupProcesses(processes)
          println("shutdown hook 执行完毕")
        }
      }, 10)

      while (!finish) {
        runCount += 1
        logWarning(s"第${runCount}次测试")
        spark.sql(s"drop table if exists ${tblName} PURGE")
        spark.sql(s"create table ${tblName}(id int, key string, user string, location string, " +
          "country string, product string) stored as orc partitioned by(pt string)" +
          s" location '${tableLocation}'")

        // 提交一个任务到线程池，线程池分配工作线程执行 run()
        val task1 = executor.submit(new Runnable {
          override def run(): Unit = {
            // 自定义工具类创建 Spark 应用程序启动器
            val launcher = SparkUtils.getLaunch("APP1", "com.tencent.App1",
              Array(sourcePath, tempView, tblName))
              .addAppArgs(sourcePath, tempView, tblName)
            val process = launcher.launch()  // 创建操作系统进程执行 Spark 应用
            processes.add(process)
            val exitCode = process.waitFor()  // 阻塞当前线程，直到 Spark 进程结束
            if (exitCode == 0) {
              logWarning(s"APP1成功执行")
            } else {
              logWarning(s"APP1执行失败，退出码: $exitCode")
            }
          }
        })

        val taskMap = new util.HashMap[String, Future[_]]()
        val resultMap = new ConcurrentHashMap[String, Long]()

        for (i <- 1 until appCount) {
          val appIndex = i + 1
          val appKey = "APP" + appIndex
          val task = executor.submit(new Runnable {
            override def run(): Unit = {
              val launcher = SparkUtils.getLaunch(appKey, "com.tencent.App2",
                Array(sourcePath, tempView, tblName, "pt" + appIndex))  // 额外指定了分区
              val process = launcher.launch()
              processes.add(process)
              val exitCode = process.waitFor()
              var ptCount: Long = 0L
              if (exitCode == 0) {
                ptCount = spark.table(tblName).where(s"pt='pt${appIndex}'").count()  // 查找目标表特定分区的数据量
                logWarning(s"APP${appIndex}成功执行，写入数据条数: ${ptCount}")
              } else {
                logWarning(s"${appKey}执行失败，退出码: $exitCode")
              }
              resultMap.put(appKey, ptCount)  // 结果储存
            }
          })
          taskMap.put(appKey, task)  // 保存每个 Spark 应用的 Future 对象
        }

        task1.get()  // 阻塞当前线程直到 task1 执行结束
        // 遍历等待所有任务执行结束
        taskMap.forEach(
          (_, task) => task.get()
        )

        resultMap.forEach(
          (appKey, ptCount) => {
            if (ptCount == recordCount) {
              // 完全写入数据：App2 的写入、提交流程均在 App1 发生异常前结束。
              logWarning(s"第${runCount}次：${appKey}执行成功，分区写入条数：$ptCount")
            } else if (ptCount == 0) {
              // 完全没有写入数据：在 App2 的所有 Task 提交之前，App1 发生失败

              // 情况一：App2 的 Task 在写入数据的过程中，App1 发生失败，导致顶级临时目录被清理。此时 App2 写入路径不存在，失败
              // 情况二：App2 写入数据成功，但在所有 Task 提交开始前，App1 发生失败，导致所有待提交的数据路径被清理，失败
              logWarning(s"第${runCount}次：${appKey}执行失败，数据未写入")
            } else {
              // 部分数据写入：在 App2 的部分 Task 完成提交之后，App1 发生失败，导致部分数据提交部分数据未提交
              finish = true
              logWarning(s"问题在第${runCount}次复现，${appKey}写入条数：$ptCount")
            }
          }
        )
        taskMap.clear()
        resultMap.clear()
        SparkUtils.cleanupProcesses(processes)
      }
    } finally {
      spark.sql(s"drop table if exists ${tblName}")
    }
    spark.stop()
  }
}
