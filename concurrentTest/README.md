# 测试步骤
1. 造数
```text
执行命令：根据需求调整最后3个参数
args[1]: 数据行数
args[2]: 文件个数
args[3]: 保存路径
spark-submit --master yarn \
--class com.tencent.GenericOrcData \
--driver-memory 1g \
--executor-cores 4 \
--num-executors 6 \
--executor-memory 2g \
/tmp/spark-test-0.1.0.jar \
2000000 128 hdfs:/orcTest/genericdata
```
2. 并发测试
```text
args[1]: 前面造数写入的路径
args[2]: 前面造数写入的行数
args[3]: 多少个APP并发
args[4]: 表路径:为了安全，会校验路径必须包含/person_
spark-submit --master yarn \
--class com.tencent.SparkConcurrentTest \
--driver-memory 1g \
--executor-cores 2 \
--num-executors 1 \
--executor-memory 1g \
/tmp/spark-test-0.1.0.jar \
hdfs:/orcTest/genericdata 2000000 3 cosn://geoli-test-1308597516/warehouse/person_test
```