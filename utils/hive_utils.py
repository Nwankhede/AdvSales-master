from connection import conn
from ddlscripts import ddl

spark = conn.process()
class hive_table_setup(object):
    def hive_setup(self):
        spark.sql("create database if not exists programmatic_rv;")
        spark.sql("use programmatic_rv;")

        # stage table creation
        spark.sql(ddl.SQL)

        # dim tables creation
        spark.sql(ddl.ssp_dim)
        spark.sql(ddl.deal_dim)
        spark.sql(ddl.advertiser_dim)
        spark.sql(ddl.country_dim)
        spark.sql(ddl.device_category_dim)
        spark.sql(ddl.agency_dim)
        spark.sql(ddl.property_dim)
        spark.sql(ddl.marketplace_dim)
        spark.sql("show tables;").show()

    def read_table(self, tableName):
        tableName = spark.sql(f"select * from programmatic_rv.{tableName}")

