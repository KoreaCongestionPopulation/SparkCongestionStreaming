from schema.congestion_type import (
    n_gender_congestion_schema, n_age_congestion_scheme
)
from common_connection import average_query
from concurrent.futures import ThreadPoolExecutor

from properties import (
    PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_GENDER,
    DEVELOPED_MARKET_NOT_FCST_GENDER,
    TOURIST_SPECIAL_ZONE_NOT_FCST_GENDER,
    PARK_NOT_FCST_GENDER,
    POPULATED_AREA_NOT_FCST_GENDER

)
from properties import (
    PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_AGE,
    DEVELOPED_MARKET_NOT_FCST_AGE,
    TOURIST_SPECIAL_ZONE_NOT_FCST_AGE,
    PARK_NOT_FCST_AGE,
    POPULATED_AREA_NOT_FCST_AGE
)


age_topic_list = [
    PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_AGE,
    DEVELOPED_MARKET_NOT_FCST_AGE,
    TOURIST_SPECIAL_ZONE_NOT_FCST_AGE,
    PARK_NOT_FCST_AGE,
    POPULATED_AREA_NOT_FCST_AGE
]
gender_topic_list = [
    POPULATED_AREA_NOT_FCST_GENDER, 
    PARK_NOT_FCST_GENDER, 
    TOURIST_SPECIAL_ZONE_NOT_FCST_GENDER, 
    DEVELOPED_MARKET_NOT_FCST_GENDER, 
    PALACE_AND_CULTURAL_HERITAGE_NOT_FCST_GENDER
]

# 일단 임시
def sql_for_congestion(fields: str) -> str:
    """
    congestion 데이터를 위한 SQL 쿼리를 생성
    
    :param fields: 집계하거나 선택할 추가 필드
    :return: 생성된 SQL 문자열.
    """
    if not fields:
        raise ValueError("Fields는 비워둘 수 없습니다.")
    
    return f"""
    SELECT 
        cg.congestion.area_name,
        cg.congestion.ppltn_time,
        AVG(cg.congestion.area_congestion_lvl) as avg_congestion_lvl,
        AVG(cg.congestion.area_ppltn_min) as avg_ppltn_min,
        AVG(cg.congestion.area_ppltn_max) as avg_ppltn_max,
        {fields}
    FROM 
        congestion_data as cg
    GROUP BY
        cg.congestion.area_name, cg.congestion.ppltn_time
    """


gender_rate = """
    AVG(cg.congestion.gender_rate.male_ppltn_rate) as avg_male_ppltn_rate,
    AVG(cg.congestion.gender_rate.female_ppltn_rate) as avg_female_ppltn_rate
"""

age_rate = """
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_0) as avg_ppltn_rate_0,
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_10) as avg_ppltn_rate_10,
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_20) as avg_ppltn_rate_20,
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_30) as avg_ppltn_rate_30,
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_40) as avg_ppltn_rate_40,
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_50) as avg_ppltn_rate_50,
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_60) as avg_ppltn_rate_60,
    AVG(cg.congestion.age_congestion_specific.ppltn_rate_70) as avg_ppltn_rate_70

"""

age = sql_for_congestion(age_rate)
gender = sql_for_congestion(gender_rate)
with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(average_query, gender_topic_list, n_gender_congestion_schema, gender)
    executor.submit(average_query, age_topic_list, n_age_congestion_scheme, age)



