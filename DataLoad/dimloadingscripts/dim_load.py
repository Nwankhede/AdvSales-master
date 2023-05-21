from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder().master('local[1]').appName('AdvSales').getOrCreate()

int_df = spark.sql("select * from programmatic_rv.programmatic_stg")

def dim_tables_load(tableName):
    win = Window.orderBy(col("tableName"))
    tableName_id = tableName+"_id"
    tableName_dim = int_df.select("tableName").distinct()
    tableNameDF = tableName_dim.withColumn(tableName_id, dense_rank().over(win)) \
        .withColumn("load_date", to_timestamp(current_timestamp(), "MM-dd-yyyy HH mm ss"))
    return tableNameDF

#read the data from dim tables

ssp_dim = dim_tables_load("ssp")
deal_dim = dim_tables_load("deal")
advertiser_dim = dim_tables_load("advertiser")
country_dim = dim_tables_load("country")
device_category_dim = dim_tables_load("device_category")
agency_dim = dim_tables_load("agency")
property_dim = dim_tables_load("property")
marketplace_dim = dim_tables_load("marketplace")
