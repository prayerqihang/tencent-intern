import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Demo {
    public static void main(String[] args) {
        // Spark 应用程序入口
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")  // 设置应用名称（会在集群资源管理界面显示）
                .enableHiveSupport()  // 启动 Hive 支持，可访问 Hive 表
                .getOrCreate();  // 获取或创建 SparkSession 实例

        // 从 args[0] 指定的路径读取 JSON 文件，转换为 DataFrame
        Dataset<Row> df = spark.read().json(args[0]);

        // 将 DataFrame 转换为 RDD（spark 底层数据处理核心）
        RDD<Row> test = df.rdd();

        // 将 RDD 内容保存为文本文件到 args[1] 指定的路径
        test.saveAsTextFile(args[1]);
    }
}
