package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class WordCountOnCos {
    public static void main(String[] args) {
        // Spark 应用程序配置对象
        SparkConf sc = new SparkConf().setAppName("spark on cos");

        // Spark 功能的入口点，负责连接集群、创建 RDD
        JavaSparkContext context = new JavaSparkContext(sc);

        // 从输入路径读取文本文件，生成 JavaRDD<String>，其中每个元素对应文件中的一行
        // 示例：输入文件内容
        // hello world
        // hello spark
        // lines: ["hello world", "hello spark"]
        JavaRDD<String> lines = context.textFile(args[0]);

        // flatMap: 将每行文本拆分为单词，并将所有单词展平为单个 RDD
        // 示例：["hello world", "hello spark"] → ["hello", "world", "hello", "spark"]
        // mapToPair：将每个单词转换为 (单词, 1) 的键值对
        // reduceByKey：对相同键（单词）的值进行累加。
        // 示例：("hello", 1) 和 ("hello", 1) → ("hello", 2）；("world", 1) 和 ("spark", 1) 保持不变。
        // saveAsTextFile：将最终结果以文本文件形式保存到输出路径
        lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey((x, y) -> x + y)
                .saveAsTextFile(args[1]);
    }
}
