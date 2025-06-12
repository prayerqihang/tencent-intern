package com.tencent

import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.udf

object App1 {
  def main(args: Array[String]): Unit = {
    val sourcePath = args(0)
    val tempView = args(1)
    val tblName = args(2)
    val spark = SparkUtils.sparkInit()

    // 从指定路径加载 ORC 格式的源数据，将数据注册为 SQL 临时视图
    spark.read.orc(sourcePath).createOrReplaceTempView(tempView)

    // 定义了一个自定义的 UDF（用户定义函数）并将其注册到 Spark SQL 中
    val tidUdf = udf(() => {
      val taskContext = TaskContext.get()  // 获取当前执行的任务上下文
      if (taskContext != null) {
        val taskId = taskContext.taskAttemptId()  // 获取当前任务的唯一尝试 ID
        // 如果 ID 超过 80，模拟内存溢出错误（ID 对应文件数量，也对应 RDD 分区，也对应 Task ID）
        if (taskId > 80) {
          throw new OutOfMemoryError(s"Task ID $taskId exceeds maximum allowed value 100")
        }
        // 返回任务 ID 字符串
        s"tid ${taskId}"
      } else {
        null
      }
    })
    spark.udf.register("tidUdf", tidUdf)

    /**
     * 数据处理流程：
     * 1. 读取 tempView 中的所有数据（tempView 为造数的拷贝版本）
     * 2. 为每条记录调用 tidUdf()，并将 select 的结果写入目标表的 pt1 分区
     *  - 如果 taskAttemptId 大于 80，抛出 OOM
     *  - 其他情况则正常返回 "tid ${taskId}"
     */
    spark.sql(s"insert overwrite table ${tblName} partition(pt='pt1') select id, tidUdf(), user," +
      s" location, country, product from ${tempView}")

    spark.stop()
  }

}
