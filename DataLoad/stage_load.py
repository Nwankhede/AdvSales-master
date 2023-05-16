import preprocessing

df_stage = preprocessing.obj.schema_clean()
df_stage.write.partitionBy("date").mode("append").\
    option("path","hdfs://m01.itversity.com:9000/user/itv000094/AdvSalesProject/data").\
    saveAsTable("programmatic_rv.programmatic_stg")