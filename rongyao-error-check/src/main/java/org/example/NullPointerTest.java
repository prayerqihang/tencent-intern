package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.apache.spark.sql.functions.*;

public class NullPointerTest {

    // 创建一个指定长度（字节长度）的字符串，用 ASCII 码填充
    private static String createLargeString(int targetLength, int itemIndex) {
        byte[] bytes = new byte[targetLength];
        new Random().nextBytes(bytes);  // 采用随机字符填充数数组

        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .config("spark.driver.memory", "16g")
//                .config("spark.sql.orc.columnarWriterBatchSize", "64")  // 这个配置项经过测试无法解决问题
                .enableHiveSupport()
                .getOrCreate();

        // 输出位置
        String warehouseDir = sparkSession.sessionState().conf().warehousePath();

        try (JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext())) {
            // 准备数据
            List<Integer> initialData = new ArrayList<>(1024);
            for (int i = 0; i < 1024; i++) {
                initialData.add(i);
            }

            JavaRDD<Integer> initialRDD = sc.parallelize(initialData, 1);  // 指定 1 个分区
            JavaRDD<String> largeStringRDD = initialRDD.map(item -> {
                int length = Integer.MAX_VALUE / 1024 + 1024;
                return createLargeString(length, item);
            });

            // 转换为 dataset
            Dataset<String> ds = sparkSession.createDataset(largeStringRDD.rdd(), Encoders.STRING());
            Dataset<Row> dsWithValue = ds.toDF("value");
            Dataset<Row> df = dsWithValue.select(
                    map(lit("key"), col("value")).alias("a")
            );

            // 写入 ORC 文件
            String outputPath = "/Users/qihanglv/output_orc";

            df.write()
                    .mode(SaveMode.Overwrite)
//                    .option("orc.dictionary.key.threshold", "0")  // （独立生效）解决方案一
//                    .option("orc.column.encoding.direct", "a")  // （独立生效）解决方案二
                    .orc(outputPath);

            System.out.println("[SUCCESS] Inserted rows into tmp");

        } finally {
            sparkSession.stop();
        }
    }
}