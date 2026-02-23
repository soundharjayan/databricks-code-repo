from pyspark import pipelines as dp
@dp.table(name="catalog1_we47.schema1_we47.shipmentwe47")
def return_df():
    df1=spark.read.table("gcp_mysql_fc_wd361.logistics.shipments1")
    return df1