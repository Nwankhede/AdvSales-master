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
def hive_table_setup():
    df = spark.sql("show databases;").show()
    df = spark.sql("create database if not exists programmatic_rv;")
    df = spark.sql("use programmatic_rv;")
    df = spark.sql(dd.SQL)
    df = spark.sql("show tables;").show()

if __name__ == '__main__':
        # Created for testing less no of columns
          spark.sql("use programmatic_rv;")
        # spark.sql("drop table programmatic_rv.programmatic_stg_tst")
        # spark.sql(dd.testSQL)
        # spark.sql("show tables;").show()
        # spark.sql("describe table programmatic_stg;").show(20, False)
        #
        #  Load logic test
          obj = pre.data_preprocessing()
          obj.process()
          df_select = obj.schema_clean().select("*")
          df_select.show()
          df_select.write.mode("overwrite").insertInto("programmatic_rv.programmatic_stg")
          print("New test table data")
          spark.sql(f"select * from programmatic_rv.programmatic_stg limit 10").show()

