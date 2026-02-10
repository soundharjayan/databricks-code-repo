from pyspark import pipelines as dp

@dp.table(name="catalog1_dropme.default.drugstbl_bronze1")
def drugstbl_bronze_tbl():
    return spark.readStream.table("lakehousecat.deltadb.drugstbl")
