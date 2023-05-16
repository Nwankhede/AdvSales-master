from pyspark.sql import SparkSession

spark = SparkSession.builder().master('local[1]').appName('AdvSales').getOrCreate()

# loading the data into different dim tables
from Config import config as cfg


print(cfg.prop["agency_table"])