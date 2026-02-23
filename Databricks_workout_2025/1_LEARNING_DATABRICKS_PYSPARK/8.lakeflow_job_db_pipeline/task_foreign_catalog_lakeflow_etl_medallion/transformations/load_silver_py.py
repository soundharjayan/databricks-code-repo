from pyspark import pipelines as dp
from pyspark.sql.functions import *
@dp.table(name="catalog1_we47.schema1_we47.silver_shipments_dlt1")
def load_silver():
    return spark.read.table("catalog1_we47.schema1_we47.bronze_shipments1").withColumn("loadts", current_timestamp())