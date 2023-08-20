from schema.congestion_type import (
    n_gender_congestion_schema, n_age_congestion_scheme, result_n_gender
)
from schema.topic_list import (
    n_age_topic_list, 
    age_retrive_topic_list,
    n_gender_topic_list,
    gender_retrive_topic_list
)
# from properties import POPAREA_NOF_GENDER ,AVG_POPAREA_NOF_GEN, AVG_DEVMKT_GEN
from common_connection import average_query
from concurrent.futures import ThreadPoolExecutor

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
        cg.area_name,
        cg.ppltn_time,
        cg.area_congestion_msg,
        AVG(cg.area_congestion_lvl) as avg_congestion_lvl,
        AVG(cg.area_ppltn_min) as avg_ppltn_min,
        AVG(cg.area_ppltn_max) as avg_ppltn_max,
        {fields}
    FROM 
        congestion_data as cg
    GROUP BY
        cg.area_name, cg.area_congestion_msg, cg.ppltn_time
    """

gender_rate = """
    AVG(cg.gender_rate.male_ppltn_rate) as avg_male_ppltn_rate,
    AVG(cg.gender_rate.female_ppltn_rate) as avg_female_ppltn_rate
"""

age_rate = """
    AVG(cg.age_congestion_specific.ppltn_rate_0) as avg_ppltn_rate_0,
    AVG(cg.age_congestion_specific.ppltn_rate_10) as avg_ppltn_rate_10,
    AVG(cg.age_congestion_specific.ppltn_rate_20) as avg_ppltn_rate_20,
    AVG(cg.age_congestion_specific.ppltn_rate_30) as avg_ppltn_rate_30,
    AVG(cg.age_congestion_specific.ppltn_rate_40) as avg_ppltn_rate_40,
    AVG(cg.age_congestion_specific.ppltn_rate_50) as avg_ppltn_rate_50,
    AVG(cg.age_congestion_specific.ppltn_rate_60) as avg_ppltn_rate_60,
    AVG(cg.age_congestion_specific.ppltn_rate_70) as avg_ppltn_rate_70

"""

age_qs = sql_for_congestion(age_rate)
gender_qs = sql_for_congestion(gender_rate)
average_query(n_age_topic_list, n_gender_congestion_schema, gender_qs, age_retrive_topic_list)

