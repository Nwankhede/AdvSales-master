from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from utils import hive_utils

spark = SparkSession.builder().master('local[1]').appName('AdvSales').getOrCreate()

# read the data from dim tables

int_df = spark.sql("select * from programmatic_rv.programmatic_stg")
ssp_dim = hive_utils.hive_table_setup.read_table("ssp_dim")
deal_dim = hive_utils.hive_table_setup.read_table("deal_dim")
advertiser_dim = hive_utils.hive_table_setup.read_table("advertiser_dim")
country_dim = hive_utils.hive_table_setup.read_table("country_dim")
device_category_dim = hive_utils.hive_table_setup.read_table("device_category_dim")
agency_dim = hive_utils.hive_table_setup.read_table("agency_dim")
property_dim = hive_utils.hive_table_setup.read_table("property_dim")
marketplace_dim = hive_utils.hive_table_setup.read_table("marketplace_dim")

#joining with stage table to load IDs

final_df =int_df.join(ssp_dim, int_df(col("ssp"))==ssp_dim(col("ssp_desc")) , "left_join")\
      .join(deal_dim, int_df(col("deal"))==ssp_dim(col("deal_desc")) , "left_join") \
    .join(advertiser_dim, int_df(col("advertiser")) == ssp_dim(col("advertiser_desc")), "left_join") \
    .join(country_dim, int_df(col("country")) == ssp_dim(col("country_desc")), "left_join") \
    .join(device_category_dim, int_df(col("device_category")) == ssp_dim(col("device_category_desc")), "left_join") \
    .join(agency_dim, int_df(col("agency")) == ssp_dim(col("agency_desc")), "left_join") \
    .join(property_dim, int_df(col("property")) == ssp_dim(col("property_desc")), "left_join") \
    .join(marketplace_dim, int_df(col("marketplace")) == ssp_dim(col("marketplace_desc")), "left_join")\
    .select("ssp_id","deal_id","advertiser_id","country_id","device_category_id","agency_id","property_id","marketplace_id",\
            "filedate","revenue_share_percent","total_revenue","total_impressions","ad_unit_id","monetization_channel_id"\
            "integration_type_id","viewable_impressions","measurable_impressions").distinct()
