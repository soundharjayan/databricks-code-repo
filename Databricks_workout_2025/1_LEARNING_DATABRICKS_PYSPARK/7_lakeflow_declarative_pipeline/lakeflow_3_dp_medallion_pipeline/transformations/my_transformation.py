from pyspark import pipelines as dp
from pyspark.sql import functions as F
#Matview doesn't support optimize/zorder/partition but it supports cluster_by that takes care of everything
@dp.table(name="catalog1_we47.default.drugstbl_medal_bronze5",cluster_by=["uniqueid"])
def drugstbl_bronze_tbl():
    return spark.read.table("lakehousecat.deltadb.drugstbl")#requirement is to collect ins/upd/deleted data and merge to bronze table
    #return spark.readStream.table("lakehousecat.deltadb.drugstbl")#requirement is to collect only ins data and merge to bronze table

#This is equivalent to spark createOrReplaceTempView("drugstbl_silverview2")
@dp.view(name="drugstbl_silverview2")#We use view very specifically, if we don't need the data to replicated in silver layer also (because in this project no one is going to use this silver data)
#@dp.expect_all_or_fail({"rating_is_valid": "rating <= 10","nonull_drugname":"drugname is not null"})
@dp.expect_all_or_drop({"rating_is_valid": "rating <= 10","nonull_drugname":"drugname is not null"})
#@dp.expect_all({"rating_is_valid": "rating <= 10","nonull_drugname":"drugname is not null"})
#insert into lakehousecat.deltadb.drugstbl(uniqueid,drugname,rating,usefulcount) values(10000015,'drug2',12,1);
@dp.expect("useful_cnt_is_valid","usefulcount > 1")
#Throw warning in the log/statistics like how much expectation is missed/met
#{"rating_is_valid": "rating <=10"}
def drugstbl_silvernewtbl():
    transformed_df=spark.read.table("catalog1_we47.default.drugstbl_medal_bronze5").filter("usefulcount > 0")#3400 rows fetched
    return transformed_df

@dp.materialized_view(name="catalog1_we47.default.drugstbl_medal_gold_mv5")
def drugstbl_medal_gold():
    df = spark.read.table("drugstbl_silverview2")
    return (
        df.groupBy("drugname")
        .agg(F.sum("usefulcount").alias("sum_usefulcount"),
             F.avg("rating").alias("avg_rating")))
