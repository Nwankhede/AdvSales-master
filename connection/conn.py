from pyspark.sql import SparkSession
from Test.Config import test_config as tcfg
from Config import config as cfg

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
            .option("inferSchema", "true") \
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