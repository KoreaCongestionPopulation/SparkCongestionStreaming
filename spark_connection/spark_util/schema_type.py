"""
-------------------------------------------------
|                                               |
|    seoul congestion rate schema register      |    
|                                               |
-------------------------------------------------
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ArrayType,
    IntegerType,
)

# 공통 스키마 정의
common_schema = StructType(
    [
        StructField("area_name", StringType(), True),
        StructField("area_congestion_lvl", IntegerType(), True),
        StructField("ppltn_time", FloatType(), True),
        StructField("area_congestion_msg", StringType(), True),
        StructField("area_ppltn_min", IntegerType(), True),
        StructField("area_ppltn_max", IntegerType(), True),
        StructField("fcst_yn", StringType(), True),
    ]
)

# fcst_yn 스키마 정의
fcst_yn_schema = StructField(
    "fcst_yn",
    StructType(
        [
            StructField(
                "fcst_ppltn",
                ArrayType(
                    StructType(
                        [
                            StructField("fcst_time", StringType(), True),
                            StructField("fcst_congest_lvl", StringType(), True),
                            StructField("fcst_ppltn_min", FloatType(), True),
                            StructField("fcst_ppltn_max", FloatType(), True),
                        ]
                    )
                ),
                True,
            )
        ]
    ),
    True,
)

# gender_rate 스키마 정의
gender_rate_schema = StructField(
    "gender_rate",
    StructType(
        [
            StructField("male_ppltn_rate", FloatType(), True),
            StructField("female_ppltn_rate", FloatType(), True),
        ]
    ),
    True,
)


# age_congestion_specific 스키마 정의
age_congestion_specific_schema = StructField(
    "age_congestion_specific",
    StructType(
        [
            StructField("ppltn_rate_0", FloatType(), True),
            StructField("ppltn_rate_10", FloatType(), True),
            StructField("ppltn_rate_20", FloatType(), True),
            StructField("ppltn_rate_30", FloatType(), True),
            StructField("ppltn_rate_40", FloatType(), True),
            StructField("ppltn_rate_50", FloatType(), True),
            StructField("ppltn_rate_60", FloatType(), True),
            StructField("ppltn_rate_70", FloatType(), True),
        ]
    ),
    True,
)


# 최종 스키마 합치기
final_schema = StructType(
    common_schema.fields + [gender_rate_schema, age_congestion_specific_schema]
)
