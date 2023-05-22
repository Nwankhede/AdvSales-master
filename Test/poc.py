from pyspark.sql.functions import *
from pyspark.sql.window import Window
from DataLoad import preprocessing as pre
from pyspark.sql import SparkSession as sc
from ddlscripts import ddl as dd
from Test.Config import test_config as tcfg
import drop_tables as dt


spark = sc.builder \
    .master('local[1]') \
    .enableHiveSupport() \
    .appName('AdvSales') \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .getOrCreate()

#creating databse and staging tables

if __name__ == '__main__':
    spark.sql("use programmatic_rv;")
    spark.sql(dt.droptablessp)
    spark.sql(dt.droptablesadvertiser)
    spark.sql(dt.droptablesdeal)
    spark.sql(dt.droptablesagency)
    spark.sql(dt.droptablescountry)
    spark.sql(dt.droptablesdevicecategory)
    spark.sql(dt.droptablesmarketplace)
    spark.sql(dt.droptablesproperty)

    df = spark.read.option("delimiter", ",") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(tcfg.prop["file_path"])
   # Creating ssp_dim hive table(Empty)
    spark.sql("create database if not exists programmatic_rv;")
    spark.sql("use programmatic_rv;")
    spark.sql(dd.ssp_dim)
    #deal tables
    spark.sql(dd.deal_dim)
    spark.sql(dd.advertiser_dim)
    spark.sql(dd.country_dim)
    spark.sql(dd.property_dim)
    spark.sql(dd.device_category_dim)
    spark.sql(dd.agency_dim)
    spark.sql(dd.marketplace_dim)
    spark.sql("show tables;").show()

    #Read the staging table
    df_stg = spark.sql("select * from programmatic_rv.programmatic_stg")
    #df_stg.persist(storageLevel="MEMORY_ONLY")
    #read ssp
    df_ssp = df_stg.select("adv_ssp").distinct()
    win = Window.orderBy(col("adv_ssp"))
    df_sspdim = df_ssp.withColumn("ssp_id", dense_rank().over(win))\
    .withColumnRenamed("adv_ssp", "ssp_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))

    df_sspdim.show(20, False)

    #read deal

    df_deal = df_stg.select("adv_deal").distinct()
    win = Window.orderBy(col("adv_deal"))
    df_dealdim = df_deal.withColumn("deal_id", dense_rank().over(win)) \
    .withColumnRenamed("adv_deal", "deal_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_dealdim.show(20, False)

    df_advertiser = df_stg.select("advertiser").distinct()
    win = Window.orderBy(col("advertiser"))
    df_advertiserdim = df_advertiser.withColumn("advertiser_id", dense_rank().over(win)) \
    .withColumnRenamed("advertiser", "advertiser_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_advertiserdim.show(20, False)

    df_country = df_stg.select("country").distinct()
    win = Window.orderBy(col("country"))
    df_countrydim = df_country.withColumn("country_id", dense_rank().over(win)) \
    .withColumnRenamed("country", "country_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_countrydim.show(20, False)

    df_property = df_stg.select("adv_property").distinct()
    win = Window.orderBy(col("adv_property"))
    df_propertydim = df_property.withColumn("property_id", dense_rank().over(win)) \
    .withColumnRenamed("adv_property", "property_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_propertydim.show(20, False)

    df_device_category = df_stg.select("device_category").distinct()
    win = Window.orderBy(col("device_category"))
    df_devicecategorydim = df_device_category.withColumn("device_id", dense_rank().over(win)) \
    .withColumnRenamed("device_category", "device_category_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_devicecategorydim.show(20, False)

    df_agency = df_stg.select("adv_agency").distinct()
    win = Window.orderBy(col("adv_agency"))
    df_agencydim = df_agency.withColumn("agency_id", dense_rank().over(win)) \
    .withColumnRenamed("adv_agency", "adv_agency_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_agencydim.show(20, False)

    df_marketplace = df_stg.select("marketplace").distinct()
    win = Window.orderBy(col("marketplace"))
    df_marketplacedim = df_marketplace.withColumn("marketplace_id", dense_rank().over(win)) \
    .withColumnRenamed("marketplace", "marketplace_desc")\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_marketplacedim.show(20, False)


    # Writing the df to the hive ssp dim table(Loading the data)
    df_sspdim.write.mode("overwrite").saveAsTable("programmatic_rv.ssp_dim")
    print("ssp table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.ssp_dim limit 10").show()


    # Writing the df to the hive ssp dim table(Loading the data)
    df_dealdim.write.mode("overwrite").saveAsTable("programmatic_rv.deal_dim")
    print("deal table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.deal_dim limit 10").show()

    df_advertiserdim.write.mode("overwrite").saveAsTable("programmatic_rv.advertiser_dim")
    print("adv table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.advertiser_dim limit 10").show()

    df_countrydim.write.mode("overwrite").saveAsTable("programmatic_rv.country_dim")
    print("country table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.country_dim limit 10").show()

    df_propertydim.write.mode("overwrite").saveAsTable("programmatic_rv.property_identifier_dim")
    print("property table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.property_identifier_dim limit 10").show()

    df_devicecategorydim.write.mode("overwrite").saveAsTable("programmatic_rv.device_category_dim")
    print("device_cat table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.device_category_dim limit 10").show()

    df_agencydim.write.mode("overwrite").saveAsTable("programmatic_rv.agency_dim")
    print("agency table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.agency_dim limit 10").show()

    df_marketplacedim.write.mode("overwrite").saveAsTable("programmatic_rv.marketplace_dim")
    print("market table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.marketplace_dim limit 10").show()

    # Loading fact logic

    dfstg = spark.sql("select * from programmatic_rv.programmatic_stg")
    dfsspdim = spark.sql("select * from programmatic_rv.ssp_dim")
    dfdealdim = spark.sql("select * from programmatic_rv.deal_dim")

    df_finaljoin = dfstg.join(dfsspdim, dfstg.adv_ssp == dfsspdim.ssp_desc, "left_outer")\
    .join(dfdealdim, dfstg.adv_deal == dfdealdim.deal_desc, "left_outer")\
    .select("ssp_id","deal_id", "integration_type_id", "monetization_channel_id", "ad_unit_id", "total_impressions", "total_revenue", "viewable_impressions", "measurable_impressions", "revenue_share_percent")

    df_finaljoin.show(10)