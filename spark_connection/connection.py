from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg
from schema.congestion_type import n_gender_congestion_schema
from properties import (
    PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_GENDER,
    DEVELOPED_MARKET_NOT_FCST_GENDER,
    TOURIST_SPECIAL_ZONE_NOT_FCST_GENDER,
    PARK_NOT_FCST_GENDER,
    POPULATED_AREA_NOT_FCST_GENDER
)

topic_list = [
    POPULATED_AREA_NOT_FCST_GENDER, 
    PARK_NOT_FCST_GENDER, 
    TOURIST_SPECIAL_ZONE_NOT_FCST_GENDER, 
    DEVELOPED_MARKET_NOT_FCST_GENDER, 
    PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_GENDER
]

"""

{
  "area_name": "광화문·덕수궁",
  "area_congestion_lvl": 2,
  "ppltn_time": 1692001200.0,
  "area_congestion_msg": "사람이 몰려있을 수 있지만 크게 붐비지는 않아요. 도보 이동에 큰 제약이 없어요.",
  "area_ppltn_min": 2000,
  "area_ppltn_max": 2500,
  "fcst_yn": "N",
  "gender_rate": {
    "male_ppltn_rate": 46.2,
    "female_ppltn_rate": 53.8
  }
}
"""
spark = (
    SparkSession.builder.appName("CongestionSouelPreprocessing").
    master("local[*]").
    config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0").
    config("spark.streaming.stopGracefullyOnShutdown", "true").
    getOrCreate()
)

kafka_connection = (
    spark.readStream.
    format("kafka").
    option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092,kafka3:39092").
    option("subscribe", ",".join(topic_list)).
    load()
)

# Kafka 스트림에서 value 칼럼의 JSON 데이터를 파싱하여 congestion 칼럼으로 변환
average_congestion = (
    kafka_connection
    .select(
        from_json(col("value").cast("string"), n_gender_congestion_schema).alias("congestion")
    )
)

# 평균 계산을 위한 필드 추출
agg_data = average_congestion.select(
    col("congestion.area_name").alias("area_name"),
    col("congestion.ppltn_time").alias("ppltn_time"),
    col("congestion.area_congestion_lvl").alias("area_congestion_lvl"),
    col("congestion.area_ppltn_min").alias("area_ppltn_min"),
    col("congestion.area_ppltn_max").alias("area_ppltn_max"),
    col("congestion.gender_rate.male_ppltn_rate").alias("male_ppltn_rate"),
    col("congestion.gender_rate.female_ppltn_rate").alias("female_ppltn_rate")
)

# area_name 기준으로 평균 계산
aggregatedStream = agg_data.groupBy("area_name", "ppltn_time").agg(
    avg("area_congestion_lvl").alias("avg_congestion_lvl"),
    avg("area_ppltn_min").alias("avg_area_ppltn_min"),
    avg("area_ppltn_max").alias("avg_area_ppltn_max"),
    avg("male_ppltn_rate").alias("avg_male_ppltn_rate"),
    avg("female_ppltn_rate").alias("avg_female_ppltn_rate")
)

# 결과 출력
query = aggregatedStream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()