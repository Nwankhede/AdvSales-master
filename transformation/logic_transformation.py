from connection import conn
# Data transformation and logic for populating the data into fact and dim

# logic for dim

df_stg = conn.process().spark.read.table("select * from programmatic_rv.programmatic_stg")
