package com.tencent

object App2 {
  def main(args: Array[String]): Unit = {
    val sourcePath = args(0)
    val tempView = args(1)
    val tblName = args(2)
    val pt = args(3)
    val spark = SparkUtils.sparkInit()
    spark.read.orc(sourcePath).createOrReplaceTempView(tempView)
    spark.sql(s"insert overwrite table ${tblName} partition(pt='${pt}') select * from ${tempView}")
    spark.stop()
  }

}
