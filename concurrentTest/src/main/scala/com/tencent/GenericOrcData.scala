package com.tencent

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.util.UUID

/**
 * usage: 在指定目录下生成orc测试数据，参数中2000000是生成数据条数，128是生成文件个数
 *
spark-submit --master yarn \
--class com.tencent.GenericOrcData \
--driver-memory 1g \
--executor-cores 4 \
--num-executors 6 \
--executor-memory 2g \
/tmp/spark-test-0.1.0.jar \
2000000 128 hdfs:/orcTest/genericdata
 */

object GenericOrcData {

  def main(args: Array[String]): Unit = {
    val recordCount = args(0).toInt
    val fileCount = args(1).toInt
    val savePath = args(2)
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("generic orc data")
      .config("spark.sql.orc.compression.codec", "zlib")
      .config("hive.exec.orc.default.stripe.size", 4 * 1024 * 1024)
      .getOrCreate()
    genericOrcData(spark, recordCount, fileCount, savePath)
  }

  case class Person(id: Int,
                    key: String,
                    user: String,
                    location: String,
                    country: String,
                    product: String)

  def genericOrcData(spark: SparkSession, recordCount: Int, fileSize: Int, savePath: String): Unit = {
    val path: Path = new Path(savePath)  // 解析并创建 Hadoop 的 Path 对象
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)  // 获取对应路径所在的文件系统对象
    // 如果路径存在，先删除
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    import spark.implicits._  // 导入 Spark SQL 的隐式转换，允许将 RDD、Seq 等转换成 DataFrame 或 Dataset

    /**
     * parallelize(0 until recordCount, fileSize) 生成一个 0 ～ recordCount - 1 的整数序列，并将这个序列并行为分区数为 fileSize 的 RDD
     * mapPartitions(itr => {}) 对每个分区的元素进行批量转换
     * itr.map() 映射函数，将 id 转变（映射）为一个 Person 对象
     */
    spark.sparkContext.parallelize(0 until recordCount, fileSize).mapPartitions(itr => {
      itr.map(id => {
        new Person(id, UUID.randomUUID().toString,
          "user" + UUID.randomUUID(),
          "地址：随机地址" + UUID.randomUUID(),
          "国籍" + UUID.randomUUID(),
          "省份" + UUID.randomUUID())
      })
    }).toDF().as[Person].write.orc(savePath)
  }

}
