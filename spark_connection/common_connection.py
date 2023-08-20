from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, from_unixtime, to_json, struct, avg
import traceback

spark = (
    SparkSession.builder
    .appName("CongestionSouelPreprocessing")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.executor.memory", "10g") 
    .config("spark.executor.cores", "4") 
    .config("spark.cores.max", "4") 
    .getOrCreate()
)
# spark.sparkContext.setLogLevel("INFO")  # 또는 "DEBUG", "ERROR", "WARN" 등의 다른 로깅 레벨을 선택할 수 있습니다.

def kafka_connection(topic_list: list[str], schema) -> DataFrame:
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092")
        .option("subscribe", topic_list)
        .load()
    )
    
    # # kafka 확인용
    # congestion_df = (
    #     kafka_stream
    #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset")
    #     .withColumn("data", from_json(col("value").cast("string"), schema=schema))
    #     .select("data.*", "topic", "partition", "offset")
    #     .withColumn("ppltn_time", col("ppltn_time").cast("timestamp"))
    #     .withWatermark("ppltn_time", "10 minute")
    # )
    return (
        kafka_stream
        .selectExpr("CAST(key as STRING)", "CAST(value as STRING)")
        .select(from_json(col("value"), schema=schema).alias("congestion"))  # Alias moved here
        .select("congestion.*")  # Now, this will extract fields from the "congestion" struct
        .withColumn("ppltn_time", col("ppltn_time").cast("timestamp"))
        .withWatermark("ppltn_time", "10 minute")
    )


def average_query(topic_list, schema, sql_expresstion, retrieve_topic) -> None:
    try:
        congestion_df: DataFrame = kafka_connection(topic_list, schema)    
        congestion_df.createOrReplaceTempView("congestion_data")
        congestion_df.printSchema()
        
        congestion_df = spark.sql(sql_expresstion)
        json_df: DataFrame = congestion_df.withColumn("value", to_json(struct("*")))
        
        
        checkpoint_dir: str = f"connection/.checkpoint_{topic_list}"
        # query: StreamingQuery = (
        #     json_df.writeStream
        #     .outputMode("append")
        #     .format("kafka")
        #     .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092")
        #     .option("topic", retrieve_topic)
        #     .option("checkpointLocation", checkpoint_dir)
        #     .option("startingOffsets", "earliest")
        #     .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        #     .start()
        # )
        query = json_df.writeStream.outputMode("append").format("console").option("truncate", "false").start()
        query.awaitTermination()

    except Exception as error:
        print(error)
        traceback.print_exc()
