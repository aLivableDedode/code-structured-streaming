package structuredops

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.ProcessingTimeTimeout
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{DataStreamReader, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KafkaSourceStreamDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("test")
      .master("local[2]")
      .getOrCreate()

//    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.17.255.39:9092,10.17.255.15:9092,10.17.255.88:9092")
//      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ndc_gid_doc_sync")
//      .option("subscribe", "test-struct-streaming")
      .option("startingOffsets", "earliest")
      // 这个参数很重要 本地开发的时候因数值过大回导致一直卡住没法输出结果
      .option("maxOffsetsPerTrigger", "200")
      .option("failOnDataLoss", "false")
      .load()

    val jsonSchema = "{\"type\":\"record\",\"name\":\"KafkaSourceTestSchema\",\"fields\":[{\"name\":\"op\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"index\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ts\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"content\",\"type\":{\"type\":\"record\",\"name\":\"content\",\"fields\":[{\"name\":\"md5_doc_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"doc_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"media_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"media_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"region_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"pub_time\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"folder_refs\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"folder_ref\",\"fields\":[{\"name\":\"folder_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"sentiment_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"tracking_time\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"keywords_highlight\",\"type\":{\"type\":\"array\",\"name\":\"keywords_highlight\",\"items\":[\"string\",\"null\"],\"default\":null}}]}}}]}}]}"
    val dataType = SchemaConverters.toSqlType(new Schema.Parser().parse(jsonSchema)).dataType
    val dFrame: DataFrame = transformFromJSON(df)

    var tempDF: DataFrame = dFrame
      .select(from_json(col("record"), dataType).as("value"), col("recordTimestamp"), col("recordOffset"))
      .select("value.*", "recordTimestamp", "recordOffset")

    tempDF.mapPartitions(partition=>{
      partition.map(row=>{
        println(row.toString())
        row
      })
    })(RowEncoder(tempDF.schema))

    val query = tempDF
      .writeStream
      .format("console")
      .start()
    query.awaitTermination()



//    val dataSet: Dataset[(String, String)] = streamReader.load()
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
//      .as[(String, String)]
//
//    dataSet.collect().foreach(println)
//      .formatted("console")
//
//    dataSet
//      .writeStream
//      .format("console")
////      .outputMode(OutputMode.Append())
//      .trigger(Trigger.ProcessingTime("0 seconds"))
//      .option("truncate","false")
////      .option("checkpointLocation", "./checkpoint/001")
//      .start()
//      .awaitTermination()



    /*
    val jsonSchema = "{\"type\":\"record\",\"name\":\"KafkaSourceTestSchema\",\"fields\":[{\"name\":\"op\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"index\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ts\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"content\",\"type\":{\"type\":\"record\",\"name\":\"content\",\"fields\":[{\"name\":\"md5_doc_id\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"doc_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"media_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"media_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"region_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"pub_time\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"folder_refs\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"folder_ref\",\"fields\":[{\"name\":\"folder_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"sentiment_type_id\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"tracking_time\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"keywords_highlight\",\"type\":{\"type\":\"array\",\"name\":\"keywords_highlight\",\"items\":[\"string\",\"null\"],\"default\":null}}]}}}]}}]}"
    val dataType = SchemaConverters.toSqlType(new Schema.Parser().parse(jsonSchema)).dataType
    val dFrame: DataFrame = transformFromJSON(dataFrame)

    var tempDF: DataFrame = dFrame
      .select(from_json(col("record"), dataType).as("value"), col("recordTimestamp"), col("recordOffset"))
      .select("value.*", "recordTimestamp", "recordOffset")

    tempDF.mapPartitions(partition=>{
      partition.map(row=>{
          println(row.toString())
        row
      })
    })(RowEncoder(tempDF.schema))

    val query: StreamingQuery = tempDF
      .writeStream
      .format("console")
      //      .outputMode("append")
      .queryName("sourceTable")
      .trigger(Trigger.ProcessingTime("0 seconds"))
//      .option("numRows", "10")
//      .option("truncate", "false")
      .start()

//    dataFrame.show()
    query.awaitTermination()
    query.stop()
  */
  }

  private[this] def transformFromJSON(dataFrame: DataFrame): DataFrame = {
    dataFrame.selectExpr("cast (value as string) as record", "cast(timestamp as long) as recordTimestamp",
      "concat(partition, '_', offset) as recordOffset")
  }
}
