package structuredops

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SocketStreamDemo {
  def main(args: Array[String]): Unit = {
    // 构建SparkSession实例对象，相关配置进行设置
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      // 设置Shuffle时分区数目
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
//    spark.sparkContext.setLogLevel("INFO")
    import spark.implicits._

    // 从TCP Socket加载数据，读取数据列名称为value，类型是String
    val inputStreamDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 进行词频统计
    val resultStreamDF: DataFrame = inputStreamDF
      .as[String] // 将DataFrame转换为Dataset
      .filter(line => null != line && line.trim.length > 0 )
      .flatMap(line => line.trim.split("\\s+"))
      // 按照单词分组和聚合
      .groupBy($"value").count()
    resultStreamDF.printSchema()

    // 将结果输出（ResultTable结果输出，此时需要设置输出模式）
    val query: StreamingQuery = resultStreamDF.writeStream
      // TODO: a. 设置输出模式， 当数据更新时再进行输出
      .outputMode(OutputMode.Update())
      // TODO: b. 设置查询名称
      .queryName("query-wordcount")
      // TODO: c. 设置触发时间间隔
      .trigger(Trigger.ProcessingTime("0 seconds"))
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      // TODO: d. 设置检查点目录
      .option("checkpointLocation", "./0002")
      .start()
    // 启动流式应用后，等待终止
    query.awaitTermination()
    query.stop()
  }

}
