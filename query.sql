SELECT
    congestion.area_name AS area_name,
    congestion.ppltn_time AS ppltn_time,
    congestion.area_congestion_lvl AS area_congestion_lvl,
    congestion.area_ppltn_min AS area_ppltn_min,
    congestion.area_ppltn_max AS area_ppltn_max,
    congestion.gender_rate.male_ppltn_rate AS male_ppltn_rate,
    congestion.gender_rate.female_ppltn_rate AS female_ppltn_rate
FROM 
    congestion


SELECT 
    cd.area_name,
    cd.ppltn_time,
    AVG(cd.area_congestion_lvl) as avg_congestion_lvl,
    AVG(cd.area_ppltn_min) as avg_ppltn_min,
    AVG(cd.area_ppltn_max) as avg_ppltn_max,
    AVG(cd.male_ppltn_rate) as avg_male_ppltn_rate,
    AVG(cd.female_ppltn_rate) as avg_female_ppltn_rate
FROM 
    congestion_data AS cd
GROUP BY
    cd.area_name, cd.ppltn_time


USE YourDatabaseName;

WITH CombienData AS (
    SELECT 
        A.area_name,
        A.ppltn_time,
        AVG(G.fcst_congest_lvl) as avg_fcst_congest_lvl,
        AVG(G.fcst_ppltn_max) as avg_fcst_ppltn_max,
        AVG(G.fcst_ppltn_min) as avg_fcst_ppltn_min, 
        G.fcst_time,
        A.area_congestion_msg,
        AVG(A.area_congestion_lvl) as avg_area_congestion_lvl,
        AVG(A.area_ppltn_max) as avg_ppltn_max,
        AVG(A.area_ppltn_min) as avg_ppltn_min,       
        AVG(R.ppltn_rate_0) as avg_ppltn_rate_0,
		AVG(R.ppltn_rate_10) as avg_ppltn_rate_10,
		AVG(R.ppltn_rate_20) as avg_ppltn_rate_20,
		AVG(R.ppltn_rate_30) as avg_ppltn_rate_30,
		AVG(R.ppltn_rate_40) as avg_ppltn_rate_40,
		AVG(R.ppltn_rate_50) as avg_ppltn_rate_50,
		AVG(R.ppltn_rate_60) as avg_ppltn_rate_60,
		AVG(R.ppltn_rate_70) as avg_ppltn_rate_70
    FROM 
        AgeRateData as R
    JOIN
        AreaData as A ON R.area_name = A.area_name
    LEFT OUTER JOIN ForecastData as G
        ON 
			A.area_name = G.area_name 
    GROUP BY 
        A.area_name, 
        A.ppltn_time,
        A.area_congestion_msg,
        G.fcst_time,
        G.fcst_congest_lvl,
        G.fcst_ppltn_max,
        G.fcst_ppltn_min
)

SELECT
	*
FROM 
	CombienData




CREATE TABLE gender_congestion_pred (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category VARCHAR(255) NOT NULL,
    area_name VARCHAR(255) NOT NULL,
    ppltn_time DATETIME NOT NULL,
    area_congestion_msg TEXT,
    avg_congestion_lvl FLOAT NOT NULL,
    avg_ppltn_min FLOAT NOT NULL,
    avg_ppltn_max FLOAT NOT NULL,
    avg_fcst_congest_lvl FLOAT NOT NULL,
    avg_fcst_ppltn_min FLOAT NOT NULL,
    avg_fcst_ppltn_max FLOAT NOT NULL,
    avg_male_ppltn_rate FLOAT NOT NULL,
    avg_female_ppltn_rate FLOAT NOT NULL
);

CREATE TABLE age_congestion_pred (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category VARCHAR(255) NOT NULL,
    area_name VARCHAR(255) NOT NULL,
    ppltn_time DATETIME NOT NULL,
    area_congestion_msg TEXT,
    avg_congestion_lvl FLOAT NOT NULL,
    avg_ppltn_min FLOAT NOT NULL,
    avg_ppltn_max FLOAT NOT NULL,
    avg_fcst_congest_lvl FLOAT NOT NULL,
    avg_fcst_ppltn_min FLOAT NOT NULL,
    avg_fcst_ppltn_max FLOAT NOT NULL,
    avg_ppltn_rate_0 FLOAT NOT NULL,
    avg_ppltn_rate_10 FLOAT NOT NULL,
    avg_ppltn_rate_20 FLOAT NOT NULL,
    avg_ppltn_rate_30 FLOAT NOT NULL,
    avg_ppltn_rate_40 FLOAT NOT NULL,
    avg_ppltn_rate_50 FLOAT NOT NULL,
    avg_ppltn_rate_60 FLOAT NOT NULL,
    avg_ppltn_rate_70 FLOAT NOT NULL
);

CREATE TABLE age_congestion (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    area_name VARCHAR(100) NOT NULL,
    ppltn_time DATETIME,
    area_congestion_msg TEXT,
    avg_congestion_lvl FLOAT NOT NULL,
    avg_ppltn_min FLOAT NOT NULL,
    avg_ppltn_max FLOAT NOT NULL,
    avg_ppltn_rate_0 FLOAT NOT NULL, 
    avg_ppltn_rate_10 FLOAT NOT NULL,
    avg_ppltn_rate_20 FLOAT NOT NULL,
    avg_ppltn_rate_30 FLOAT NOT NULL,
    avg_ppltn_rate_40 FLOAT NOT NULL,
    avg_ppltn_rate_50 FLOAT NOT NULL,
    avg_ppltn_rate_60 FLOAT NOT NULL,
    avg_ppltn_rate_70 FLOAT NOT NULL
);

CREATE TABLE gender_congestion (
    id INT AUTO_INCREMENT PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    area_name VARCHAR(100) NOT NULL,
    ppltn_time DATETIME,
    area_congestion_msg TEXT,
    avg_congestion_lvl FLOAT NOT NULL,
    avg_ppltn_min FLOAT NOT NULL,
    avg_ppltn_max FLOAT NOT NULL,
    avg_male_ppltn_rate FLOAT NOT NULL,
    avg_female_ppltn_rate FLOAT NOT NULL
);

