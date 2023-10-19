"""쿼리 모음집"""


class SparkStreamingQueryOrganization:
    """
    Spark 동적 쿼리 모음집
    """

    def __init__(self, with_temp_view: str, temp_view: str, data_type: str) -> None:
        self.with_temp_view = with_temp_view
        self.temp_view = temp_view
        self.data_type = data_type

        self.fcst_yn_rate = """
            AVG(fcst_data.fcst_congest_lvl) AS avg_fcst_congest_lvl,
            AVG(fcst_data.fcst_ppltn_min) AS avg_fcst_ppltn_min,
            AVG(fcst_data.fcst_ppltn_max) AS avg_fcst_ppltn_max,
        """
        self.gender_rate = """
            AVG(cg.gender_rate.male_ppltn_rate) as avg_male_ppltn_rate,
            AVG(cg.gender_rate.female_ppltn_rate) as avg_female_ppltn_rate
            """
        self.with_data_congestion = f"""
            WITH {self.with_temp_view} AS (
                SELECT 
                    category,
                    area_name,
                    ppltn_time,
                    area_congestion_lvl,
                    area_congestion_msg,
                    area_ppltn_max, 
                    area_ppltn_min,
                    explode(fcst_yn.fcst_ppltn) AS fcst_data,
                    {self.data_type}
                FROM 
                    {self.temp_view}
            )
            """
        self.age_rate = """
            AVG(cg.age_rate.ppltn_rate_0) as avg_ppltn_rate_0,
            AVG(cg.age_rate.ppltn_rate_10) as avg_ppltn_rate_10,
            AVG(cg.age_rate.ppltn_rate_20) as avg_ppltn_rate_20,
            AVG(cg.age_rate.ppltn_rate_30) as avg_ppltn_rate_30,
            AVG(cg.age_rate.ppltn_rate_40) as avg_ppltn_rate_40,
            AVG(cg.age_rate.ppltn_rate_50) as avg_ppltn_rate_50,
            AVG(cg.age_rate.ppltn_rate_60) as avg_ppltn_rate_60,
            AVG(cg.age_rate.ppltn_rate_70) as avg_ppltn_rate_70
            """

    def sql_for_congestion(self, temp_view: str, fields: str) -> str:
        """
        congestion 데이터를 위한 SQL 쿼리를 생성

        :param fields: 집계하거나 선택할 추가 필드
        :return: 생성된 SQL 문자열.
        """
        if not fields:
            raise ValueError("Fields는 비워둘 수 없습니다.")

        return f"""
        SELECT 
            cg.category,
            cg.area_name,
            regexp_replace(cg.ppltn_time, 'Z', '') as ppltn_time,
            cg.area_congestion_msg,
            AVG(cg.area_congestion_lvl) as avg_congestion_lvl,
            AVG(cg.area_ppltn_min) as avg_ppltn_min,
            AVG(cg.area_ppltn_max) as avg_ppltn_max,
            {fields}
        FROM 
            {temp_view} as cg
        GROUP BY
            cg.area_name, 
            cg.area_congestion_msg, 
            cg.ppltn_time,
            cg.category
        """

    def n_gender_rate_query(self) -> str:
        """성별 혼잡도 관련둰 쿼리"""
        return self.sql_for_congestion("congestion_gender", self.gender_rate)

    def n_age_rate_query(self) -> str:
        """나이대별 혼잡도"""
        return self.sql_for_congestion("congestion_age", self.age_rate)

    def y_age_rate_query(self, query_type: str) -> str:
        """예측값이 존재하는 쿼리 혼잡도"""
        if query_type == "age_rate":
            data: str = self.fcst_yn_rate + self.age_rate
        elif query_type == "gender_rate":
            data: str = self.fcst_yn_rate + self.gender_rate
        else:
            raise ValueError(f"선택한 : {query_type}의 적절한 쿼리문이 존재하지 않습니다")

        main_query: str = self.sql_for_congestion(
            temp_view=self.with_temp_view, fields=data
        )
        return self.with_data_congestion + main_query
