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
    spark.sql("show tables;").show()

    #Read the staging table
    df_stg = spark.sql("select * from programmatic_rv.programmatic_stg")

    #read ssp
    df_ssp = df_stg.select("adv_ssp").distinct()
    win = Window.orderBy(col("adv_ssp"))
    df_sspdim = df_ssp.withColumn("ssp_id", dense_rank().over(win))\
    .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    df_sspdim.show(20, False)

    #read deal


    # Writing the df to the hive ssp dim table(Loading the data)
    df_sspdim.write.mode("overwrite").saveAsTable("programmatic_rv.ssp_dim")
    print("ssp table data loaded successfully")
    spark.sql(f"select * from programmatic_rv.ssp_dim limit 10").show()

    # Writing the df to the hive ssp dim table(Loading the data)
    # df_sspdeal.write.mode

