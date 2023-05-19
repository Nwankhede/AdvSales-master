from pyspark.sql.functions import *

from pyspark.sql import SparkSession
from Config import config as cfg
from Test.Config import test_config as tcfg
import os


class data_preprocessing(object):
    def __init__(self):
        self.mode = "local"

    def process(self):
        if self.mode == "local":
            spark = SparkSession.builder \
                .master('local[1]') \
                .enableHiveSupport() \
                .appName('AdvSales') \
                .config("hive.exec.dynamic.partition", "true") \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .getOrCreate()

            df = spark.read.option("delimiter", ",") \
                .option("header", "true") \
                .option("inferSchema", "true")\
                .csv(tcfg.prop["file_path"])
            return df


        elif self.mode == "cluster":
            spark = SparkSession.builder \
                .master('yarn') \
                .appName('AdvSales') \
                .config("spark.sql.warehouse.dir", cfg.prop['warehouse_location']) \
                .config("hive.exec.dynamic.partition", "true") \
                .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                .enableHiveSupport() \
                .getOrCreate()
            df = spark.read.option("delimiter", ",") \
                .option("header", "true") \
                .csv(cfg.prop["node_file_path"])
            return df

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

    def load_table(self):
         #self.schema_clean().show(5)
         print("Db name : " +cfg.prop["database"]+"."+cfg.prop["stg_table"])
         self.schema_clean().printSchema()
         self.schema_clean().write.mode("overwrite").insertInto(f"{cfg.prop['database']}.{cfg.prop['stg_table']}")


# df_raw = spark.read.table(cfg.prop['raw_table'])
#
# #Removing unnecessary columns from the raw data
#


