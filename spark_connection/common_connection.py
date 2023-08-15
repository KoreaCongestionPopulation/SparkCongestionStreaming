import traceback
from pyspark.sql import types
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_json, struct

spark = (
    SparkSession.builder.appName("CongestionSouelPreprocessing").
    master("local[*]").
    config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0").
    config("spark.streaming.stopGracefullyOnShutdown", "true").
    getOrCreate()
)


def kafka_connection(topic_list: list[str], schema: types) -> None:
    kafka_connection = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092")
        .option("subscribe", ",".join(topic_list))
        .load()
    )
    
    
    # Kafka 스트림에서 value 칼럼의 JSON 데이터를 파싱하여 congestion 칼럼으로 변환
    congestion_df = (
        kafka_connection
        .select(
            from_json(col("value").cast("string"), schema).alias("congestion")
        )
        .select("congestion.*")
        .withColumn("ppltn_time", (col("ppltn_time") * 1000).cast("long")) # Convert to milliseconds
        .withColumn("ppltn_time", from_unixtime(col("ppltn_time") / 1000).cast("timestamp")) # Convert to timestamp
        .withWatermark("ppltn_time", "10 minutes") 
    )
    congestion_df.createOrReplaceTempView("congestion_data")
    congestion_df.printSchema()


def average_query(
    topic_list: list[str], 
    schema: types, 
    sql_expression: str, 
    retrieve_topic: str
) -> None:
    try:
        kafka_connection(topic_list, schema)
        processed_df = spark.sql(sql_expression)
        processed_df = processed_df.select(to_json(struct("*")).alias("value"))

        # 결과 출력
        query = (
            processed_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092")
            .option("topic", ",".join(retrieve_topic))
            .option("checkpointLocation", ".checkpoint_")
            .option(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            )
            .start()
        )
        # 결과 출력
        # query = processed_df.writeStream \
        #     .outputMode("complete") \
        #     .format("console") \
        #     .start()

        query.awaitTermination()
    except Exception as error:
        print(error)
        traceback.print_exc()
