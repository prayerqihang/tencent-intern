package org.example;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 第一步：创建数据，模拟 CTAS 发生错误的场景
 spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --class org.example.CTASIssueReproducer \
 application-name.jar first-ctas 1000000
 * 第二步：重新执行 CTAS
 spark-submit \
 --master yarn \
 --deploy-mode cluster \
 --class org.example.CTASIssueReproducer \
 application-name.jar second-ctas
 */

public class CTASIssueReproducer {

    private static final String TABLE_NAME = "test_ctas_issue";
    private static final String DATA_PATH = "hdfs:/futuTest/rawData";

    public static void main(String[] args) throws Exception {
        if (args[0].equals("first-ctas")) {
            int dataCount = Integer.parseInt(args[1]);
            // 创建测试数据
            createTestData(dataCount);
            // 第一次运行：模拟失败场景
            simulateFailure();
        } else if (args[0].equals("second-ctas")) {
            // 第二次运行：尝试再次创建表，应该会失败
            retryCreateTable();
        } else {
            System.out.println("输入参数有误！");
        }
    }

    private static void createTestData(int dataCount) {
        SparkSession spark = SparkSession.builder()
                .appName("CTAS Issue Reproducer")
                .enableHiveSupport()
                .getOrCreate();

        // 创建测试数据
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataCount; i++) {
            rows.add(RowFactory.create(i, "name_" + i, new Random().nextDouble()));
        }

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, false)
        });

        Dataset<Row> df = spark.createDataFrame(rows, schema);

        // 保存为 ORC 文件
        df.write().mode(SaveMode.Overwrite).orc(DATA_PATH);

        spark.stop();
    }

    private static void simulateFailure() {
        SparkSession spark = SparkSession.builder()
                .appName("CTAS Issue Reproducer")
                .enableHiveSupport()
                .getOrCreate();

        // 从 ORC 文件加载数据
        Dataset<Row> df = spark.read().orc(DATA_PATH);

        // 注册为临时表
        df.createOrReplaceTempView("source_data");

        // 添加监听器
        final AtomicBoolean writeStarted = new AtomicBoolean(false);
        spark.sparkContext().addSparkListener(new SparkListener() {
            @Override
            public void onJobStart(SparkListenerJobStart jobStart) {
                writeStarted.set(true);
                System.out.println("监测到数据写入作业开始！");
            }
        });

        // 使用一个线程来模拟在 CTAS 执行过程中 Driver 被 kill 的情况
        Thread killerThread = new Thread(() -> {
            try {
                int waitCount = 0;
                // 等待最多 60 秒，直到检测到写入开始
                while (!writeStarted.get() && waitCount < 60) {
                    System.out.println("等待数据写入开始(" + (waitCount + 1) + "/60)...");
                    Thread.sleep(1000);
                    waitCount++;
                }

                // 等待一段时间，确保数据开始写入但尚未完成
                System.out.println("等待 0.5 秒确保写入进行中...");
                Thread.sleep(500);

                System.out.println("模拟 Driver 被 kill...");
                Runtime.getRuntime().halt(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        killerThread.setDaemon(true);
        killerThread.start();

        // 执行 CTAS，这个操作会被中断
        spark.sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        System.out.println("开始执行 CTAS 操作 ...");
        spark.sql("CREATE TABLE " + TABLE_NAME +
                " USING orc AS SELECT * FROM source_data");

        // 这里应该不会执行到，前面已经 exit 了
        spark.stop();
    }

    private static void retryCreateTable() {
        SparkSession spark = SparkSession.builder()
                .appName("CTAS Issue Reproducer")
                .enableHiveSupport()
                .getOrCreate();

        // 从 ORC 文件加载数据
        Dataset<Row> df = spark.read().orc(DATA_PATH);

        // 注册为临时表
        df.createOrReplaceTempView("source_data");

        try {
            // 再次执行 CTAS 操作，应该会失败，因为数据目录已经存在
            spark.sql("DROP TABLE IF EXISTS " + TABLE_NAME);
            System.out.println("尝试再次执行CTAS操作...");
            spark.sql("CREATE TABLE " + TABLE_NAME +
                    " USING orc AS SELECT * FROM source_data");
            System.out.println("CTAS操作成功完成，这是不应该发生的！");
        } catch (Exception e) {
            System.out.println("复现成功！捕获到预期的异常：");
            e.printStackTrace();
        }

        spark.stop();
    }
}