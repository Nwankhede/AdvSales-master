SQL = """CREATE TABLE IF NOT EXISTS programmatic_stg(
 adv_ssp  string
,adv_deal   string
,advertiser   string
,country   string
,device_category   string
,adv_agency   string
,adv_property   string
,marketplace   string
,integration_type_id     bigint
,monetization_channel_id bigint
,ad_unit_id              bigint
,total_impressions       bigint
,total_revenue           DOUBLE
,viewable_impressions    bigint
,measurable_impressions  bigint
,revenue_share_percent    DOUBLE
,load_time timestamp)
PARTITIONED BY ( 
filedate int
)
stored AS PARQUET; """
ssp_dim = """CREATE TABLE IF NOT EXISTS ssp_dim(
 ssp_id bigint,
 adv_ssp  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """
# create other dim tables take reference of ssp_dim creation
adv_deal = """CREATE TABLE IF NOT EXISTS deal_dim(
 deal_id bigint,
 adv_deal  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """
advertiser = """CREATE TABLE IF NOT EXISTS advertiser_dim(
 advertiser_id bigint,
 advertiser  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """
country = """CREATE TABLE IF NOT EXISTS country_dim(
 country_id bigint,
 country  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """
adv_property = """CREATE TABLE IF NOT EXISTS property_identifier_dim(
 property_id bigint,
 property  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """
device_category = """CREATE TABLE IF NOT EXISTS device_category_dim(
 device_id bigint,
 device  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """
adv_agency = """CREATE TABLE IF NOT EXISTS agency_dim(
 agency_id bigint,
 agency  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """
marketplace = """CREATE TABLE IF NOT EXISTS marketplace_dim(
 marketplace_id bigint,
 marketplace  string)
PARTITIONED BY ( 
load_time timestamp
)
stored AS PARQUET; """