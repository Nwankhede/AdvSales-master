from pyspark.sql.functions import *
from pyspark.sql.window import Window
from DataLoad import preprocessing as pre
from pyspark.sql import SparkSession as sc
from ddlscripts import ddl as dd
from Test.Config import test_config as tcfg


spark = sc.builder \
    .master('local[1]') \
    .enableHiveSupport() \
    .appName('AdvSales') \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .getOrCreate()

#creating databse and staging tables

if __name__ == '__main__':
    df = spark.read.option("delimiter", ",") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(tcfg.prop["file_path"])
   # Creating ssp_dim hive table(Empty)
    spark.sql("create database if not exists programmatic_rv;")
    spark.sql("use programmatic_rv;")
    spark.sql(dd.ssp_dim)
    #deal tables
    spark.sql(dd.adv_deal)
    spark.sql(dd.advertiser)
    spark.sql(dd.country)
    spark.sql(dd.adv_property)
    spark.sql(dd.device_category)
    spark.sql(dd.adv_agency)
    spark.sql(dd.marketplace)
    spark.sql("show tables;").show()

    #Read the staging table
    df_stg = spark.sql("select * from programmatic_rv.programmatic_stg")
    #df_stg.persist(storageLevel="MEMORY_ONLY")
    #read ssp
    df_ssp = df_stg.select("adv_ssp").distinct()
    win = Window.orderBy(col("adv_ssp"))
    df_sspdim = df_ssp.withColumn("ssp_id", dense_rank().over(win))\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_sspdim.show(20, False)

    #read deal

    df_deal = df_stg.select("adv_deal").distinct()
    win = Window.orderBy(col("adv_deal"))
    df_dealdim = df_deal.withColumn("deal_id", dense_rank().over(win)) \
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_dealdim.show(20, False)

    df_advertiser = df_stg.select("advertiser").distinct()
    win = Window.orderBy(col("advertiser"))
    df_advertiserdim = df_advertiser.withColumn("advertiser_id", dense_rank().over(win)) \
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_advertiserdim.show(20, False)

    df_country = df_stg.select("country").distinct()
    win = Window.orderBy(col("country"))
    df_countrydim = df_country.withColumn("country_id", dense_rank().over(win)) \
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_countrydim.show(20, False)

    df_property = df_stg.select("property").distinct()
    win = Window.orderBy(col("property"))
    df_propertyidentifierdim = df_property.withColumn("property_id", dense_rank().over(win)) \
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_propertyidentifierdim.show(20, False)

    df_device_category = df_stg.select("device").distinct()
    win = Window.orderBy(col("device"))
    df_devicecategorydim = df_device_category.withColumn("device_id", dense_rank().over(win)) \
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_devicecategorydim.show(20, False)

    df_agency = df_stg.select("agency").distinct()
    win = Window.orderBy(col("agency"))
    df_agencydim = df_agency.withColumn("agency_id", dense_rank().over(win)) \
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_agencydim.show(20, False)

    df_marketplace = df_stg.select("marketplace").distinct()
    win = Window.orderBy(col("marketplace"))
    df_marketplacedim = df_marketplace.withColumn("marketplace_id", dense_rank().over(win)) \
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

    df_propertyidentifierdim.write.mode("overwrite").saveAsTable("programmatic_rv.property_identifier_dim")
    print("property table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.property_identifier_dim limit 10").show()

    df_devicecategorydim.write.mode("overwrite").saveAsTable("programmatic_rv.device_category_dim")
    print("device_cat table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.device_category_dim limit 10").show()

    df_agencydim.write.mode("overwrite").saveAsTable("programmatic_rv.agency_dim")
    print("agency table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.agency_dim 10").show()

    df_marketplacedim.write.mode("overwrite").saveAsTable("programmatic_rv.marketplace_dim")
    print("market table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.marketplace_dim limit 10").show()

