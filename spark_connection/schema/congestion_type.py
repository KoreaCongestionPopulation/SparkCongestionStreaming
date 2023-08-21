from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ArrayType,
    IntegerType,
    DoubleType, 
    TimestampType
)

"""
-------------------------------------------------
|        seoul congestion common schema         |    
-------------------------------------------------
schema ex)
{
  "area_name": "광화문·덕수궁",
  "area_congestion_lvl": 2,
  "ppltn_time": 1692001200.0,
  "area_congestion_msg": "사람이 몰려있을 수 있지만 크게 붐비지는 않아요. 도보 이동에 큰 제약이 없어요.",
  "area_ppltn_min": 28000,
  "area_ppltn_max": 30000,
}
"""
common_schema = StructType(
    [
        StructField("area_name", StringType(), True),
        StructField("area_congestion_lvl", IntegerType(), True),
        StructField("ppltn_time", FloatType(), True),
        StructField("area_congestion_msg", StringType(), True),
        StructField("area_ppltn_min", IntegerType(), True),
        StructField("area_ppltn_max", IntegerType(), True),
    ]
)



"""
---------------------------------------------------
|    seoul congestion fcst_yn register schema     |                                                 
---------------------------------------------------
schema ex)
"fcst_yn": {
    "fcst_ppltn": [
        {
        "fcst_time": utf float,
        "fcst_congest_lvl": "약간 붐빔",
        "fcst_ppltn_min": 30000.0,
        "fcst_ppltn_max": 32000.0
        },
        .....
    ]
}
"""
fcst_ppltn_schema = StructType([
    StructField("fcst_time", FloatType(), True),
    StructField("fcst_congest_lvl", StringType(), True),
    StructField("fcst_ppltn_min", FloatType(), True),
    StructField("fcst_ppltn_max", FloatType(), True),
])

fcst_yn_schema = StructType([
    StructField("fcst_ppltn", ArrayType(fcst_ppltn_schema), True),
])       



"""
------------------------------------------------------
|    seoul congestion gender rate schema register    |    
------------------------------------------------------
schema ex)
"gender_rate": {
    "male_ppltn_rate": 46.2,
    "female_ppltn_rate": 53.8
  }
"""
gender_rate_schema = StructField(
    "gender_rate", StructType(
        [
            StructField("male_ppltn_rate", DoubleType(), True),
            StructField("female_ppltn_rate", DoubleType(), True),
        ]
    ), True,
)


"""
------------------------------------------------------
|    seoul congestion age rate schema register       |    
------------------------------------------------------
schema ex)
"age_rate": {
    "ppltn_rate_0": 0.7,
    "ppltn_rate_10": 5.8,
    "ppltn_rate_20": 25.2,
    "ppltn_rate_30": 20.8,
    "ppltn_rate_40": 18.8,
    "ppltn_rate_50": 15.9,
    "ppltn_rate_60": 8.5,
    "ppltn_rate_70": 4.4
}
"""
age_congestion_specific_schema = StructField(
    "age_rate", StructType(
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
    ), True,
)

# -----------------------------------------------------------------------------------------

"""
---------------------------------------------
|    seoul congestion  schema register      |    
---------------------------------------------
"""

# 예측값 제공 안할때  fcst_yn: "n"
n_fcst_yn = StructField("fcst_yn", StringType(), True)

# common + fcst_yn_schema + age_rate
y_age_congestion_scheme = StructType(
    common_schema.fields + fcst_yn_schema.fields + [age_congestion_specific_schema]
)

# common + fcst_yn_schema + gender_rate
y_gender_congestion_schema = StructType(
    common_schema.fields + fcst_yn_schema.fields + [gender_rate_schema]
)

# common + fcst_yn_schema("n") + age_rate
n_age_congestion_scheme = StructType(
    common_schema.fields + [n_fcst_yn, age_congestion_specific_schema]
)

# common + fcst_yn_schema("n") + gender_rate
n_gender_congestion_schema = StructType(
    common_schema.fields + [n_fcst_yn, gender_rate_schema]
)


