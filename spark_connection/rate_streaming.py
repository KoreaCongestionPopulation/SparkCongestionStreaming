"""
spark 시작 동시성 core 4개
"""
from concurrent.futures import ThreadPoolExecutor
from schema.congestion_type import (
    y_age_congestion_schema,
    y_gender_congestion_schema,
    n_age_congestion_schema,
    n_gender_congestion_schema,
)
from schema.topic_list import (
    n_age_topic_list,
    n_gender_topic_list,
    age_topic_list,
    gender_topic_list,
)
from schema.utils import SparkStreamingQueryOrganization as SparkQuery
from properties import (
    AVG_AGE_TOPIC,
    AVG_GENDER_TOPIC,
    AVG_N_AGE_TOPIC,
    AVG_N_GENDER_TOPIC,
    AGE_CONGESTION,
    AGE_CONGESTION_PRED,
    GENDER_CONGESTION,
    GENDER_CONGESTION_PRED
)

from common_connection import SparkCongestionProcessor

y_age_query = SparkQuery(
    with_temp_view="congest_age",
    temp_view="congest_pred_age",
    data_type="age_rate"
)
y_gender_query = SparkQuery(
    with_temp_view="congest_gender",
    temp_view="congest_pred_gender",
    data_type="gender_rate"
)

n_age_query = SparkQuery(
    with_temp_view=None,
    temp_view="congestion_age",
    data_type=None
)
n_gender_query = SparkQuery(
    with_temp_view=None,
    temp_view="congestion_gender",
    data_type=None
)

average_query = SparkCongestionProcessor()
with ThreadPoolExecutor(max_workers=4) as executor:
    try:
        executor.submit(
            average_query.process,
            n_gender_topic_list,
            n_gender_congestion_schema,
            n_gender_query.temp_view,
            n_gender_query.n_gender_rate_query(),
            AVG_N_GENDER_TOPIC,
            GENDER_CONGESTION
        )
        executor.submit(
            average_query.process,
            n_age_topic_list,
            n_age_congestion_schema,
            n_age_query.temp_view,
            n_age_query.n_age_rate_query(),
            AVG_N_AGE_TOPIC,
            AGE_CONGESTION
        )
        executor.submit(
            average_query.process,
            age_topic_list,
            y_age_congestion_schema,
            y_age_query.temp_view,
            y_age_query.y_age_rate_query("age_rate"),
            AVG_AGE_TOPIC,
            AGE_CONGESTION_PRED
        )
        executor.submit(
            average_query.process,
            gender_topic_list,
            y_gender_congestion_schema,
            y_gender_query.temp_view,
            y_gender_query.y_age_rate_query("gender_rate"),
            AVG_GENDER_TOPIC,
            GENDER_CONGESTION_PRED
        )
    except Exception as error:
        print(error)
