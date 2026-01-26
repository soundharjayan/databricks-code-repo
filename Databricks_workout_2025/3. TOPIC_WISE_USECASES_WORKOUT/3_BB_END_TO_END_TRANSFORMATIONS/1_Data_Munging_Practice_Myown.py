# Databricks notebook source
df = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/custs")
df.printSchema()
# display(df.take(20))
# display(df.sample(0.1,1))
# display(df.sample(0.2,2))


# COMMAND ----------

print(df.columns)
print(df.dtypes)

for i in df.dtypes:
    if i[1]=='string':
        print(i[0])

print(df.schema)    #important, no need to write the full code, we can modify the values once we get the structure by using schema

# COMMAND ----------

#print("Actual count of rawdata",df.count())
#print("De-duplicate the record (all columns) count",df.distinct().count())
#print("De-duplicate the record (all columns) count",df.dropDuplicates().count())
#print("De-duplicate given column count",df.dropDuplicates(['_c0']).count())

display(df.describe())
display(df.summary())
