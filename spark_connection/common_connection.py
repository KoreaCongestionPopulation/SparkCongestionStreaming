"""
Spark 
"""
from typing import Any
from properties import (
    KAFKA_BOOTSTRAP_SERVERS,
    MYSQL_PASSWORD,
    MYSQL_URL,
    MYSQL_USER,
)
from pyspark.sql import types as SparkDataTypeSchema
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, from_json, to_json, struct


class SparkCongestionProcessor:
    """
    스파크 실시간 인구 혼잡도 평균 쿼리 계산 
    """
    def __init__(self):
        self.spark = self.create_spark_session()

    @staticmethod
    def create_spark_session() -> SparkSession:
        """스파크 설정"""

        spark = (
            SparkSession.builder
            .appName("CongestionSouelPreprocessing")
            .master("spark://spark-master:7077")
            # .master("local[*]")
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.jars.packages", "com.google.guava:guava:27.0-jre,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:8.0.28,org.apache.hadoop:hadoop-aws:3.2.2") 
            # .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.kafka.consumer.config.auto.offset.reset", "latest")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.cores.max", "16")
            .config("spark.sql.adaptive.enabled", "false")
            # .config("spark.kafka.consumer.cache.capacity", "")
            .getOrCreate()
        )

        return spark

    def read_from_kafka(self, topic_list, schema) -> DataFrame:
        """카프카 연결 및 wateramark 설정"""
        kafka_stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("subscribe", ",".join(topic_list))
            .load()
        )

        return (
            kafka_stream
            .selectExpr("CAST(key as STRING)", "CAST(value as STRING)")
            .select(from_json(col("value"), schema=schema).alias("congestion"))
            .select("congestion.*")
            .withColumn("ppltn_time", col("ppltn_time").cast("timestamp"))
            .withWatermark("ppltn_time", "5 minute")
        )

    def write_to_kafka(self, data_format: DataFrame, topic: str) -> StreamingQuery:
        """처리된 결과값 다시 카프카 토픽으로 보내기"""
        # checkpoint_dir = f"{S3_LOCATION}/connection/.checkpoint_{topic}"
        checkpoint_dir = f"/connection/.checkpoint_{topic}"
        
        return (
            data_format.writeStream
            .outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("topic", topic)
            .option("checkpointLocation", checkpoint_dir)
            .option("startingOffsets", "earliest")
            .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            .start()
        )

    def write_to_mysql(self, data_format: DataFrame, table_name: str) -> StreamingQuery:
        """마이크로 배치고 mysql 적재하기"""
        # checkpoint_dir = f"{S3_LOCATION}/connection/.checkpoint_{table_name}"
        checkpoint_dir = f"/connection/.checkpoint_{table_name}"

        def write_batch_to_mysql(batch_df: Any, batch_id) -> None:
            """JDBC template 사용"""
            batch_df.write \
                .format("jdbc") \
                .option("url", MYSQL_URL) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", table_name) \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_PASSWORD) \
                .mode("append") \
                .save()
                
        return (
            data_format.writeStream
            .outputMode("update")
            .foreachBatch(write_batch_to_mysql)
            .option("checkpointLocation", checkpoint_dir)
            .trigger(processingTime ='1 minutes')
            .start()
        )


    def process(
        self,
        topic_list: list,
        schema: SparkDataTypeSchema,
        temp_view: str,
        sql_expression: str,
        retrieve_topic: str,
        mysql_table_name: str
    ) -> None:
        """
        최종으로 모든 처리 프로세스를 처리하는 프로세스 함수 시작점 


        Args:
            topic_list (list): 처리할 토픽
            schema (SparkDataTypeSchema): spark 타입
            temp_view (str): with절 SQL
            sql_expression (str): SQL절
            retrieve_topic (str): 처리한 결과값을 다시 카프카로 보낼 토픽 이름
            mysql_table_name (str): mysql 테이블 
        """
        try:
            congestion_df = self.read_from_kafka(topic_list, schema)

            congestion_df.createOrReplaceTempView(temp_view)
            congestion_df.printSchema()

            processed_df = self.spark.sql(sql_expression)
            json_df = processed_df.withColumn("value", to_json(struct("*")))
            table_injection = processed_df.select("*")

            # # Write to Kafka
            query_kafka = self.write_to_kafka(json_df, retrieve_topic)

            # Write to MySQL
            query_mysql = self.write_to_mysql(table_injection, mysql_table_name)

            # Await Termination for both Kafka and MySQL
            query_kafka.awaitTermination()
            query_mysql.awaitTermination()
            # query = table_injection.writeStream.outputMode("update").format("console").option("truncate", "false").start()
            # query.awaitTermination()
        except Exception as error:
            print(error)
