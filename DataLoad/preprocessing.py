from pyspark.sql.functions import *
from Config import config as cfg
import os

class data_preprocessing(object):
    def __init__(self):
        self.mode = "local"

    def schema_clean(self):
        df_schema = self.process().select(cfg.prop["stg_schema"])\
        .withColumnRenamed("ssp","adv_ssp")\
        .withColumnRenamed("deal","adv_deal")\
        .withColumnRenamed("agency","adv_agency")\
        .withColumnRenamed("property","adv_property")\
        .withColumn("filedate", regexp_replace(split(col("date"),' ').getItem(0).cast("string"),'/','').cast("int"))\
        .withColumn("load_time", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss SSS"))\
        .withColumn("revenue_share_percent",col("revenue_share_percent").cast("Double"))\
        .withColumn("total_revenue",col("total_revenue").cast("Double"))\
        .withColumn("total_impressions",col("total_impressions").cast("bigint"))\
        .withColumn("ad_unit_id",col("ad_unit_id").cast("bigint"))\
        .withColumn("monetization_channel_id",col("monetization_channel_id").cast("bigint"))\
        .withColumn("integration_type_id",col("integration_type_id").cast("long"))\
        .withColumn("viewable_impressions",col("viewable_impressions").cast("bigint"))\
        .withColumn("measurable_impressions",col("measurable_impressions").cast("bigint"))
        dfSchema = df_schema.select("adv_ssp", "adv_deal", "advertiser", "country", "device_category", "adv_agency", "adv_property", "marketplace","integration_type_id", "monetization_channel_id", "ad_unit_id", "total_impressions", "total_revenue", "viewable_impressions", "measurable_impressions", "revenue_share_percent", "load_time", "filedate" )
        return dfSchema

    def stage_load_table(self):
         df_select = self.schema_clean().select("*")
         df_select.printSchema()
         df_select.show(10, False)
         df_select.write.mode("overwrite").insertInto("programmatic_rv.programmatic_stg")
         print(f"programmatic_rv.programmatic_stg loaded successfully, records inserted: {df_select.count()}")