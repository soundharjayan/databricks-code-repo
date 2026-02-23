# Databricks notebook source
# MAGIC %md
# MAGIC # 1.Generic functions

# COMMAND ----------

# Return Sparksession functions

from pyspark.sql.session import SparkSession

def get_spark_session(app_name = "project_name"):
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            return spark
    except:
        pass
    
    return (SparkSession.builder.appName(app_name).getOrCreate())


# COMMAND ----------

# All generic functions for reading data from files & tables

# Reading csv file function

def read_csv_df(spark,path,header= True,infer_schema = True,sep = ","):
    return_df = spark.read.option("header",header).option("infeSchema",infer_schema).option("sep",sep).csv(path)
    return return_df

# Reading json file function
def read_json_df(spark,path,mline = True):
    return spark.read.json(path,multiLine = mline,mode="PERMISSIVE")

# Reading delta file function
def read_delta_df(spark,path):
    return spark.read.format("delta").load(path)

# Reading all the file format in one function by using looping
def read_file(spark,filetype,path,header= True,infer_schema = True,sep = ",",mline=True):
    if filetype=="csv":
        return spark.read.csv(path,header=header,inferSchema = infer_schema,sep=sep)
    elif filetype == "json":
        return spark.read.json(path,multiLine = mline,mode ="PERMISSIVE")
    elif filetype == "delta":
        return spark.read.format("delta").load(path)
    else:
        raise Exception("File type not supported")

# Reading table function
def read_table(spark,table_name):
    return spark.table(table_name)




# COMMAND ----------

# Return joined DF

def join_df(df1,df2,how ="inner",on="shipment_id"):
    return df1.join(df2,how = how,on = on)

def mergeDF(df1,df2,allowmissingcol = True):
    return df1.unionByName(df2,allowMissingColumns = allowmissingcol)


# COMMAND ----------

# Adding lit values functions
from pyspark.sql.functions import lit
def add_lit_columns(df,columns,default_value = None):
    for col_name in columns:
        df = df.withColumn(col_name,lit(default_value))
    return df

# COMMAND ----------

# All generic functions for writing data from files & tables

def write_file(df,path,mode = "overwrite",format="delta"):
    return df.write.format(format).mode(mode).save(path)

def write_table(df,tablename,mode ="overwrite"):
    return df.write.mode(mode).format("delta").saveAsTable(tablename)
  

# COMMAND ----------

# MAGIC %pip install word2number

# COMMAND ----------

from word2number import w2n
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

def word_to_num(value):
    try:
        return int(value)
    except:
        try:
            return w2n.word_to_num(value.lower())
        except:
            return None
        
word_to_num_udf = udf(word_to_num, IntegerType())
        

# COMMAND ----------

# MAGIC %md
# MAGIC #2.Inline/Business Specific Functions

# COMMAND ----------

from pyspark.sql import functions as F

def standardize_staff(df):
    return(
        df
        .withColumn("shipment_id",word_to_num_udf(F.col("shipment_id")).cast("long"))
        .withColumn("age",word_to_num_udf(F.col("age")).cast("int"))
        .withColumn("role",F.lower("role"))
        .withColumn("origin_hub_city",F.initcap("hub_location"))
        .withColumn("load_dt",F.current_timestamp())
        .withColumn("full_name",F.concat_ws(" ","first_name","last_name"))
        .withColumn("hub_location",F.initcap("hub_location"))
        .drop("first_name","last_name")
        .withColumnRenamed("full_name","staff_full_name")       
    )

def scrub_geotag(df):
    return(
        df
        .withColumn("city_name",F.initcap("city_name"))
        .withColumn("masked_hub_location", F.initcap("country"))
    )

def standardize_shipments(df):
    return(
        df
        .withColumn("domain",F.lit("Logistics"))
        .withColumn("ingestion_timestamp",F.current_timestamp())
        .withColumn("is_expedited",F.lit(False).cast("boolean"))
        .withColumn("shipment_date",F.to_date("shipment_date","yy-MM-dd"))
        .withColumn("shipment_cost",F.round("shipment_cost",2))
        .withColumn("shipment_weight_kg",F.col("shipment_weight_kg").cast("double"))
    )

def enrich_shipments(df):
    return(
        df
        .withColumn("route_segment",F.concat_ws("->","source_city","destination_city"))
        .withColumn("vehicle_identifier",F.concat_ws("_","vehicle_type","shipment_id"))
        .withColumn("shipment_year",F.year("shipment_date"))
        .withColumn("shipment_month",F.month("shipment_date"))
        .withColumn("is_weekend",F.dayofweek("shipment_date").isin([1,7]))
        .withColumn("is_expedited",F.col("shipment_status").isin("IN_TRANSIT","DELIVERED"))
        .withColumn("cost_per_kg",F.round(F.col("shipment_cost")/F.col("shipment_weight_kg"),2))
        .withColumn("tax_amount",F.round(F.col("shipment_cost")*0.18,2))
        .withColumn("days_since_shipment",F.datediff(F.current_date(),"shipment_date"))
        .withColumn("is_high_value", F.col("shipment_cost")> 50000)        
    )

def split_columns(df):
    return(
        df
        .withColumn("order_prefix",F.substring("order_id",1,3))
        .withColumn("order_sequence",F.substring("order_id",4,10))
        .withColumn("ship_year",F.year("shipment_date"))
        .withColumn("ship_month",F.month("shipment_date"))
        .withColumn("ship_day",F.dayofmonth("shipment_date"))
        .withColumn("route_lane", F.concat_ws("->","source_city","destination_city"))
    )

def mask_name(col):
    return F.concat(
        F.substring(col,1,2),
        F.lit("****"),
        F.substring(col,-1,1)
    )

