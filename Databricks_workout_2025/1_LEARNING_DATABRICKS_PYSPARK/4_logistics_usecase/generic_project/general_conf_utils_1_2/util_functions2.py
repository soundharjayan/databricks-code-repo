# Databricks notebook source
print("Running Utility Notebook to initialize all functions to use further")

# COMMAND ----------

# MAGIC %md
# MAGIC Creating UDF to convert string to number, hence we don't have to filter string values or manipulate string values manually using dictionary word_to_num={'one':'1','two':'2'}<br>
# MAGIC Eg. If we pass "twenty thousand two hundred and one" -> 20201

# COMMAND ----------

# MAGIC %pip install word2number

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from word2number import w2n

def word_to_num(value):
    try:
        # If already numeric
        return int(value)
    except:
        try:
            return w2n.word_to_num(value.lower())
        except:
            return None

word_to_num_udf = udf(word_to_num, IntegerType())


# COMMAND ----------

# MAGIC %md
# MAGIC ###Inline/Business Specific Functions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def standardize_staff(df):
    return (
        df
        .withColumn("shipment_id",word_to_num_udf(F.col("shipment_id")).cast("long"))
        .withColumn("age",word_to_num_udf(F.col("age")).cast("int"))
        .withColumn("role", F.lower("role"))
        .withColumn("origin_hub_city", F.initcap("hub_location"))
        .withColumn("load_dt", F.current_timestamp())
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("hub_location", F.initcap("hub_location"))
        .drop("first_name", "last_name")
        .withColumnRenamed("full_name", "staff_full_name")
    )

def scrub_geotag(df):
    return (
        df
        .withColumn("city_name", F.initcap("city_name"))
        .withColumn("masked_hub_location", F.initcap("country"))
    )

def standardize_shipments(df):
    return (
        df
        .withColumn("domain", F.lit("Logistics"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("is_expedited", F.lit(False).cast("boolean"))
        .withColumn("shipment_date", F.to_date("shipment_date", "yy-MM-dd"))
        .withColumn("shipment_cost", F.round("shipment_cost", 2))
        .withColumn("shipment_weight_kg", F.col("shipment_weight_kg").cast("double"))
    )

def enrich_shipments(df):
    return (
        df
        .withColumn("route_segment",
            F.concat_ws("-", "source_city", "destination_city"))
        .withColumn("vehicle_identifier",
            F.concat_ws("_", "vehicle_type", "shipment_id"))
        .withColumn("shipment_year", F.year("shipment_date"))
        .withColumn("shipment_month", F.month("shipment_date"))
        .withColumn("is_weekend",
            F.dayofweek("shipment_date").isin([1,7]))
        .withColumn("is_expedited",
            F.col("shipment_status").isin("IN_TRANSIT", "DELIVERED"))
        .withColumn("cost_per_kg",
            F.round(F.col("shipment_cost") / F.col("shipment_weight_kg"), 2))
        .withColumn("tax_amount",
            F.round(F.col("shipment_cost") * 0.18, 2))
        .withColumn("days_since_shipment",
            F.datediff(F.current_date(), "shipment_date"))
        .withColumn("is_high_value",
            F.col("shipment_cost") > 50000)
    )

def split_columns(df):
    return (
        df
        .withColumn("order_prefix", F.substring("order_id", 1, 3))
        .withColumn("order_sequence", F.substring("order_id", 4, 10))
        .withColumn("ship_year", F.year("shipment_date"))
        .withColumn("ship_month", F.month("shipment_date"))
        .withColumn("ship_day", F.dayofmonth("shipment_date"))
        .withColumn("route_lane",
            F.concat_ws("->", "source_city", "destination_city"))
    )

def mask_name(col):
    return F.concat(
        F.substring(col, 1, 2),
        F.lit("****"),
        F.substring(col, -1, 1)
    )



# COMMAND ----------

# MAGIC %md
# MAGIC ###Generic Functions

# COMMAND ----------

#Return Spark session
from pyspark.sql.session import SparkSession
def get_spark_session(app_name="Some Anonymous Data Engineering Project"):
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            return spark
    except:
        pass

    return (SparkSession.builder.appName(app_name).getOrCreate())



# COMMAND ----------

#All generic functions for reading data from files & tables

def read_csv_df(spark,path,header=True,infer_schema=True,sep=","):
    return_df=spark.read.option("header", header).option("inferSchema", infer_schema)\
        .option("sep", sep)\
        .csv(path)
    return return_df

def read_json_df(spark, path,mline=True):
    return spark.read.json(path,multiLine=mline,mode="PERMISSIVE")

   
def read_delta_df(spark,path):
    return spark.read.format("delta").load(path)

def read_file(spark,filetype,path,header=True,infer_schema=True,mline=True):
    if filetype=="csv":
        return spark.read.csv(path,header=header,inferSchema=infer_schema)#read_csv_df(spark,path)
    elif filetype=="json":
        return read_json_df(spark,path)
    elif filetype=="delta":
        return read_delta_df(spark,path)
    elif filetype=='orc':
        return spark.read.orc(path)
    else:
        raise Exception("File type not supported")


def read_table(spark,table_name):
    return spark.table(table_name)

# COMMAND ----------

#Return Joined DF
def join_df(df1,df2,how="inner",on="shipment_id"):#To avoid cartesian/cross join, i am adding some column in the on
    return df1.join(df2, on=on, how=how)

def unionDf(df1,df2):
    return df1.union(df2)
def unionDfSql(spark,view1,view2):    
    returndf=spark.sql(f"select * from view1 union select * from view2")
    return returndf

def mergeDf(df1,df2,allowmissingcol=True):
    return df1.unionByName(df2, allowMissingColumns=allowmissingcol)

# COMMAND ----------

from pyspark.sql.functions import lit
def add_literal_columns(df, columns, default_value=None):
    for col_name in columns:
        df = df.withColumn(col_name, lit(default_value))
    return df


# COMMAND ----------

#All generic functions for writing data to files(datalake) & tables(lakehouse)

def write_file(df, path, mode="overwrite", format="delta"):
    return df.write.mode(mode).format(format).save(path)

def write_table(df, tablename, mode="overwrite"):
    df.write.mode(mode).format("delta").saveAsTable(tablename)    