from pyspark import pipelines
#load_data_bronze_imp_dp().write.saveAsTable("lakehousecat.default.shipment1_bronze")
@pipelines.table(name="catalog1_we47.schema1_we47.shipment1_bronze2")#it becomes declarative by specifying decorator on top of the function (Table function will create Materialized view if we use read.table for ingestion)
def load_data_bronze_imp_dp():#Imperative program
    df1=spark.read.table("gcp_mysql_fc_wd361.logistics.shipments1")#foreign catalog external DB source
    df2=df1.filter("city is null")
    return df2
#concepts learned:
#1. Imperative to Declarative
#2. Materialized View - mat view is a stored query (store data), that refers the data from the base table
#3. SCD Type1
#4. Foreign Catalog

#Iteration:
#I am inserting a row, updating a row and deleting a row in my mysql table (now total data is 20 rows)
#What df2 will have -> 20 rows
#What happend behind in this @pipelines.table(matview) -> take 20 from df2 and compare with matview data and insert new rows, update existing rows and delete deleted rows.

#Few important understanding we need to get out of this program (most of our DP learning will be over if you do this..)
#1. How to write a declarative program rather than imperative - We used pipelines and decorator
#2. How to handle batch (data collected in different time, executed in a particular time by scheduling) data ingestion from the source - 
#  a. Source table will be collected completely with inserted/updated/deleted data
#  b. Target table (materialized view) will be updated with only inserted/updated/deleted data (DLT will not be updating the target table with all the data from source table) - benefit is (we didn't write the big merge statement as we wrote in the foreign catalog program, but achieved same feature with mat view concept)
# c. We achieved SCD Type1 Feature simply.
#Question: does declarative pipeline in dlt creates managed delta table directly in databricks or only we can get materialized view (batch) or stream table (Streaming) created?
# No, a Databricks declarative pipeline (also known as a Lakeflow Spark Declarative Pipeline) does not create managed Delta tables directly, rather it will be created as streaming tables or materialized views for better performance and other features.
