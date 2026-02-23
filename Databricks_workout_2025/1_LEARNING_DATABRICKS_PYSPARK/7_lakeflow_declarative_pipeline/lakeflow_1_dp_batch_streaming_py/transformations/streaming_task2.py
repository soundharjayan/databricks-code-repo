from pyspark import pipelines
@pipelines.table(name="catalog1_we47.schema1_we47.shipment1_bronze3")#Since I used readStream function, this pipelines.table will create a streaming table (incremental table SCDType2 (only inserts are engaged))
def load_data_bronze_imp_dp():
    df1 = spark.readStream.table("workspace.default.silver_staff_tbl1")  # only work in the delta tables only inserts, skip updates/deletes
    return df1

#Understanding in this program:
#1. I have created a streaming table (incremental table SCDType2 (only inserts are engaged))
#2. I have used readStream function to read the data from the delta table (only inserted data)
#3. I have used pipelines.table function to create a streaming table (HOW to do DP knows it much better than me)
