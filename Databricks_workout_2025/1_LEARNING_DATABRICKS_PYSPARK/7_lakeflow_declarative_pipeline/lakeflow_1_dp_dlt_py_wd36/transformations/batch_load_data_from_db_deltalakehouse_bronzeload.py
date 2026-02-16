from pyspark import pipelines

#load_data_bronze_imp_dp().write.saveAsTable("lakehousecat.default.shipment1_bronze")
@pipelines.table(name="catalog1_we47.schema1_we47.shipment1_bronze1")#it becomes declarative by specifying decorator on top of the function
def load_data_bronze_imp_dp():#Imperative program
    df1=spark.read.table("gcp_mysql_fc_wd361.logistics.shipments1")#foreign catalog external DB source
    df2=df1.filter("city is null")
    return df2
#Few important understanding we need to get out of this program (most of our DP learning will be over if you do this..)
#1. How to write a declarative program rather than imperative - We used pipelines and decorator
#2. How to handle batch data ingestion from the source - 
#  a. Source table will be collected completely with inserted/updated/deleted data
#  b. Target table (materialized view) will be updated with only inserted/updated/deleted data (We will not be updating the target table with all the data from source table)
