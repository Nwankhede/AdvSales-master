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
testSQL = """CREATE TABLE IF NOT EXISTS programmatic_stg_tst(
 adv_ssp  string
,adv_deal   string
,load_time timestamp)
PARTITIONED BY ( 
filedate int
)
stored AS PARQUET; """