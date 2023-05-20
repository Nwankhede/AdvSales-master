from pyspark.sql.functions import *

from DataLoad import preprocessing as pre
from pyspark.sql import SparkSession as sc
from ddlscripts import ddl as dd
from Test.Config import test_config as cfg

spark = sc.builder \
    .master('local[1]') \
    .enableHiveSupport() \
    .appName('AdvSales') \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .getOrCreate()

#creating databse and staging tables
def hive_table_setup():
    # Created for testing less no of columns
    spark.sql("create database if not exists programmatic_rv;")
    spark.sql("use programmatic_rv;")
    spark.sql(dd.SQL)
    spark.sql("show tables;").show()
    spark.sql("describe table programmatic_stg;").show(20, False)


if __name__ == '__main__':
    hive_table_setup()
      # Load logic test
    obj = pre.data_preprocessing()
    obj.process()
    df_select = obj.schema_clean().select("*")
    df_select.printSchema()
    df_select.show(10, False)
    df_select.write.mode("overwrite").insertInto("programmatic_rv.programmatic_stg")
    print("New test table data")
    spark.sql(f"select * from programmatic_rv.programmatic_stg limit 10").show()
