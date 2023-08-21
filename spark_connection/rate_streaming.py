from schema.congestion_type import (
    y_age_congestion_scheme,
    y_gender_congestion_schema,
    n_age_congestion_scheme,
    n_gender_congestion_schema    
)
from schema.topic_list import (
    n_age_topic_list,
    n_gender_topic_list,
    age_topic_list,
    gender_topic_list,
    
)
from schema.utils import (
    n_age_rate_query, 
    n_gender_rate_query
)
from properties import (
    AVG_AGE_TOPIC,
    AVG_N_AGE_TOPIC,
    AVG_GENDER_TOPIC,
    AVG_N_GENDER_TOPIC
)

from common_connection import average_query
from concurrent.futures import ThreadPoolExecutor


with ThreadPoolExecutor(max_workers=3) as executor:
    executor.submit(
        average_query, 
        n_gender_topic_list, 
        n_gender_congestion_schema, 
        n_gender_rate_query,
        AVG_N_GENDER_TOPIC, 
        "congestion_gender"
    )
    executor.submit(
        average_query, 
        n_age_topic_list, 
        n_age_congestion_scheme, 
        n_age_rate_query, 
        AVG_N_AGE_TOPIC, 
        "congestion_age"
    )

