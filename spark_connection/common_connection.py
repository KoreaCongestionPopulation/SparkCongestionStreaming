from properties import (
    KAFKA_BOOTSTRAP_SERVERS, 
    MYSQL_PASSWORD,
    MYSQL_URL, 
    MYSQL_USER, 
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY,
    S3_LOCATION,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, from_json, to_json, struct


class SparkCongestionProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()

    @staticmethod
    def create_spark_session() -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("CongestionSouelPreprocessing")
            .master("spark://spark-master:7077")
            .config("spark.streaming.backpressure.enabled", "true") 
            .config("spark.jars.packages", "com.google.guava:guava:27.0-jre,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,mysql:mysql-connector-java:8.0.28,org.apache.hadoop:hadoop-aws:3.2.2") 
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.streaming.kafka.consumer.config.auto.offset.reset", "latest") 
            .config("spark.executor.memory", "10g")
            .config("spark.executor.cores", "4")
            .config("spark.cores.max", "4")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )

        # AWS Credentials 설정
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        return spark

    def read_from_kafka(self, topic_list, schema) -> DataFrame:
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

    def write_to_kafka(self, df: DataFrame, topic: str) -> StreamingQuery:
        checkpoint_dir = f"{S3_LOCATION}/connection/.checkpoint_{topic}"
        # checkpoint_dir = f"connection/.checkpoint_{topic}"
        
        return (
            df.writeStream
            .outputMode("update")
            .format("kafka")
            .option("kafka.bootstrap.servers", f"{KAFKA_BOOTSTRAP_SERVERS}")
            .option("topic", topic)
            .option("checkpointLocation", checkpoint_dir)
            .option("startingOffsets", "earliest")
            .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            .start()
        )

    # def write_to_mysql(self, df: DataFrame, table_name: str) -> StreamingQuery:
    #     # checkpoint_dir = f"{S3_LOCATION}/connection/.checkpoint_{table_name}"
    #     checkpoint_dir = f"connection/.checkpoint_{table_name}"

    #     def write_batch_to_mysql(batch_df, batch_id) -> None:
    #         batch_df.write \
    #             .format("jdbc") \
    #             .option("url", MYSQL_URL) \
    #             .option("driver", "com.mysql.cj.jdbc.Driver") \
    #             .option("dbtable", table_name) \
    #             .option("user", MYSQL_USER) \
    #             .option("password", MYSQL_PASSWORD) \
    #             .mode("append") \
    #             .save()

    #     return (
    #         df.writeStream
    #         .outputMode("update") 
    #         .foreachBatch(write_batch_to_mysql) 
    #         .option("checkpointLocation", checkpoint_dir)
    #         .trigger(processingTime='1 minutes') 
    #         .start()
    #     )


    def process(self, topic_list, schema, temp_view, sql_expression, retrieve_topic, mysql_table_name) -> None:
        try:
            congestion_df = self.read_from_kafka(topic_list, schema)

            congestion_df.createOrReplaceTempView(temp_view)
            congestion_df.printSchema()

            processed_df = self.spark.sql(sql_expression)
            json_df = processed_df.withColumn("value", to_json(struct("*")))
            # table_injection = processed_df.select("*")

            # # Write to Kafka
            query_kafka = self.write_to_kafka(json_df, retrieve_topic)

            # Write to MySQL
            # query_mysql = self.write_to_mysql(table_injection, mysql_table_name)

            # Await Termination for both Kafka and MySQL
            query_kafka.awaitTermination()
            # query_mysql.awaitTermination()
            # query = table_injection.writeStream.outputMode("update").format("console").option("truncate", "false").start()
            # query.awaitTermination()
        except Exception as error:
            print(error)
