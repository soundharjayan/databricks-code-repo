# Databricks notebook source
# MAGIC %md
# MAGIC ###We are going to learn some advanced options available in the different file/table write functions (Least bother)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Important - There are 4 typical write modes and default is error.
# MAGIC 1. overwrite
# MAGIC 2. append
# MAGIC 3. ignore
# MAGIC 4. error

# COMMAND ----------

# MAGIC %md
# MAGIC ####CSV Write Advanced Feature - Very Important, Important, Not Important (just try to know once for all)
# MAGIC **Very Important:** path, mode, compression, sep, quote, escape, header   <br>
# MAGIC **Important:** nullValue,quoteAll,dateFormat, timestampFormat, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace,lineSep  <br>
# MAGIC **Not Important:** escapeQuotes, charToEscapeQuoteEscaping, encoding, emptyValue

# COMMAND ----------

struct1="custid int,name string,age int"
df1=spark.read.schema(struct1).csv("/Volumes/workspace/default/volumewd36/malformeddata1.txt",header=False,sep=',',mode='permissive',comment='#',quote="'",escape="|")
df1.show(10,False)

# COMMAND ----------

df1.write.csv( path="/Volumes/workspace/default/volumewd36/csvadvwrite", mode='overwrite', compression='None', sep=',', quote="'", escape='~', header=True)

# COMMAND ----------

df1.write.csv( path="/Volumes/workspace/default/volumewd36/csvadvwrite", mode='overwrite', compression='None', sep=',', quote="'", escape='~', header=True,nullValue='na',quoteAll=True,lineSep='-')
#nullValue,quoteAll,dateFormat, timestampFormat, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace,lineSep

# COMMAND ----------

# MAGIC %md
# MAGIC ####JSON Write Advanced Feature - Very Important, Important, Not Important (just try to know once for all)
# MAGIC **Very Important:** path, mode, compression  <br>
# MAGIC **Important:** dateFormat, timestampFormat, lineSep, ignoreNullFields  <br>
# MAGIC **Not Important:** encoding

# COMMAND ----------

df1.write.json(path="/Volumes/workspace/default/volumewd36/jsonadvwrite", mode="append", compression="None",
               ignoreNullFields=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Serialized Data format (orc/parquet/delta(we learn indepth in project)) Write Advanced Feature - Very Important, Important, Not Important (just try to know once for all)
# MAGIC **Very Important:** path, mode, partitionBy, compression  <br>

# COMMAND ----------

df1.write.orc(path="/Volumes/workspace/default/volumewd36/orcadvwrite",mode='error',compression='snappy'
              ,partitionBy='age')
df1.write.parquet(path="/Volumes/workspace/default/volumewd36/parquetadvwrite",mode='error',compression='snappy'
              ,partitionBy='age')

# COMMAND ----------

spark.read.orc("/Volumes/workspace/default/volumewd36/orcadvwrite").where("age=30").show()
spark.read.parquet("/Volumes/workspace/default/volumewd36/parquetadvwrite").where("age=30").show()