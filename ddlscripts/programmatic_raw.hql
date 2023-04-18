CREATE EXTERNAL TABLE IF NOT EXISTS programmatic_raw (
file_date String,
ssp String,
deal String,
advertiser String,
country String,
device_category String,
agency String,
property String,
marketplace String,
integration_type_id String,
monetization_channel_id String,
ad_unit_id String,
total_impressions String,
total_revenue String,
viewable_impressions String,
measurable_impressions String,
revenue_share_percent String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';


CREATE EXTERNAL TABLE IF NOT EXISTS programmatic_stg (
ssp	 String,
deal String,
advertiser String,
country String,
device_category	 String,
agency String,
property String,
marketplace String,
integration_type_id	 Bigint,
monetization_channel_id	 Bigint,
ad_unit_id bigint,
total_impressions Bigint,
total_revenue DOUBLE,
viewable_impressions Bigint,
measurable_impressions Bigint,
revenue_share_percent DOUBLE,
load_time Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS ssp_dim (
ssp_id Bigint,
ssp_desc String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS deal_dim (
deal_id Bigint,
deal_desc String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS advertiser_dim (
advertiser_id Bigint,
advertiser_des String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS country_dim (
country_id Bigint,
country_desc String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS property_identifier_dim (
property_identifier_id Bigint,
property_identifier_desc String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS device_category_dim (
device_category_id Bigint,
device_category_desc String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS agency_dim (
agency_id Bigint,
agency_desc String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS marketplace_dim (
marketplace_id Bigint,
marketplace_desc String,
load_date Timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';

CREATE EXTERNAL TABLE IF NOT EXISTS programmatic_fact (
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data';