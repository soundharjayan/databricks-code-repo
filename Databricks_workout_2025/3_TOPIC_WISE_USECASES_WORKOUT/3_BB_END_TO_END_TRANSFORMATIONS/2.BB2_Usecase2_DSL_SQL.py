# Databricks notebook source
# MAGIC %md
# MAGIC #Enterprise Fleet Analytics Pipeline: Focuses on the business outcome (analytics) and the domain (fleet/logistics).

# COMMAND ----------

# MAGIC %md
# MAGIC ![logistics](logistics_project.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Download the data from the below gdrive and upload into the catalog
# MAGIC https://drive.google.com/drive/folders/1J3AVJIPLP7CzT15yJIpSiWXshu1iLXKn?usp=drive_link

# COMMAND ----------

# MAGIC %sql CREATE VOLUME IF NOT EXISTS workspace.default.logistics_vol;

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/default/logistics_vol/logistics_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##**1. Data Munging** -

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Visibily/Manually opening the file and capture couple of data patterns (Manual Exploratory Data Analysis) <br>
# MAGIC
# MAGIC 1. Duplicate rows observed
# MAGIC 2. Additional columns observed
# MAGIC 3. Values are not present in the given columns
# MAGIC 4. file contains header and no footer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Programatically try to find couple of data patterns applying below EDA (File: logistics_source1)
# MAGIC 1. Apply inferSchema and toDF to create a DF and analyse the actual data.
# MAGIC 2. Analyse the schema, datatypes, columns etc.,
# MAGIC 3. Analyse the duplicate records count and summary of the dataframe.

# COMMAND ----------

# 1.Apply inferSchema and toDF to create a DF and analyse the actual data.
read_df = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source1",inferSchema = True,header=True)
display(read_df)

# 2.Analyse the schema, datatypes, columns etc.,
read_df.printSchema()
display(read_df.schema)
display(read_df.columns)
display(read_df.dtypes)

# 3.Analyse the duplicate records count and summary of the dataframe.
print("Total count of dataset",read_df.count())
print("de-duplicated record (all columns) count",read_df.distinct().count())        # distinct will remove entire columns duplicates and give the count of the records
print("de-duplicated record (all columns) count",read_df.dropDuplicates().count())  # dropDuplicates also will do the same, but we can pass required columns
print("de-duplicated record (selected columns) count",read_df.dropDuplicates(['shipment_id']).count())

display(read_df.summary())


# COMMAND ----------

# MAGIC %md
# MAGIC ###a. Passive Data Munging -  (File: logistics_source1  and logistics_source2)
# MAGIC Without modifying the data, identify:<br>
# MAGIC Shipment IDs that appear in both master_v1 and master_v2<br>
# MAGIC Records where:<br>
# MAGIC 1. shipment_id is non-numeric
# MAGIC 2. age is not an integer<br>
# MAGIC
# MAGIC Count rows having:
# MAGIC 3. fewer columns than expected
# MAGIC 4. more columns than expected

# COMMAND ----------

read_raw_df = spark.read.csv(path=["/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source1","/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source2"],header=True,inferSchema = True)
display(read_raw_df)

# 1. Shipment_id is non-numeric
from pyspark.sql.functions import col
#invalid_id_df = read_raw_df.filter(~col("shipment_id").rlike("^[0-9]+$").show()) # using Pyspark function
invalid_id_df = read_raw_df.where("shipment_id rlike '[a-zA-Z]'").show()   # using Sql function

# Optional - If we need to filter the string values with Null values

invalid_id_df = read_raw_df.filter((~col("shipment_id").rlike("^[0-9]+$"))| col("shipment_id").isNull()).show()  # using Pyspark function
invalid_id_df = read_raw_df.where("shipment_id rlike '[a-zA-Z]' OR shipment_id IS NULL" ).show()   # using Sql function

# 2. Age is not an integer
invalid_age_df = read_raw_df.filter((~col("age").rlike("^[0-9]+$"))).show()  # using Pyspark function
invalid_age_df = read_raw_df.where("age rlike '[a-zA-Z]' " ).show()          # using Sql function

# 3. Count rows having fewer columns than expected

from pyspark.sql.types import StructType,StructField,StringType,ShortType,IntegerType 

struttype1=StructType([StructField('ship_id', IntegerType(), True), StructField('first_name', StringType(), True), StructField('last_name', StringType(), True), StructField('age', ShortType(), True), StructField('role', StringType(), True),StructField("corruptedrows",StringType())])

df_1 = spark.read.schema(struttype1).csv(path="/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source1",header=True,mode='permissive',columnNameOfCorruptRecord="corruptedrows")
#display(df_1)
fewer_df = df_1.where("size(split(corruptedrows, ',')) < 5")
display(fewer_df)
display(len(fewer_df.collect()))

# 4.Count rows having fewer columns than expected
more_df = df_1.where("size(split(corruptedrows, ',')) > 5")
display(more_df)
display(len(more_df.collect()))




# COMMAND ----------

#Create a Spark Session Object
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("Enterprise Fleet/Logistics Analytics Pipeline").getOrCreate()


# COMMAND ----------

# MAGIC %md
# MAGIC ###**b. Active Data Munging** File: logistics_source1 and logistics_source2

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.Combining Data + Schema Merging (Structuring)
# MAGIC 1. Read both files without enforcing schema
# MAGIC 2. Align them into a single canonical schema: shipment_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC age,
# MAGIC role,
# MAGIC hub_location,
# MAGIC vehicle_type,
# MAGIC data_source
# MAGIC 3. Add data_source column with values as: system1, system2 in the respective dataframes

# COMMAND ----------

from pyspark.sql.functions import lit

# 1.Read both files without enforcing schema 
raw_data_df1 = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source1",header= True)
#display(raw_data_df1)
raw_data_df2 = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source2",header= True)
#display(raw_data_df2)

# 2.Add data_source column with values as: system1, system2 in the respective dataframes
raw_data_df1 = raw_data_df1.withColumn("data_source",lit("system1"))
raw_data_df2 = raw_data_df2.withColumn("data_source",lit("system2"))

# 3.Align them into a single canonical schema: shipment_id, first_name, last_name, age, role, hub_location, vehicle_type, data_source
schema_merged_df = raw_data_df1.unionByName(raw_data_df2,allowMissingColumns = True)
final_df = schema_merged_df.select("shipment_id","first_name","last_name","age","role","hub_location","vehicle_type","data_source")
display(final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Cleansing, Scrubbing: 
# MAGIC Cleansing (removal of unwanted datasets)<br>
# MAGIC 1. Mandatory Column Check - Drop any record where any of the following columns is NULL:shipment_id, role<br>
# MAGIC 2. Name Completeness Rule - Drop records where both of the following columns are NULL: first_name, last_name<br>
# MAGIC 3. Join Readiness Rule - Drop records where the join key is null: shipment_id<br>
# MAGIC
# MAGIC Scrubbing (convert raw to tidy)<br>
# MAGIC 4. Age Defaulting Rule - Fill NULL values in the age column with: -1<br>
# MAGIC 5. Vehicle Type Default Rule - Fill NULL values in the vehicle_type column with: UNKNOWN<br>
# MAGIC 6. Invalid Age Replacement - Replace the following values in age:
# MAGIC "ten" to -1
# MAGIC "" to -1<br>
# MAGIC 7. Vehicle Type Normalization - Replace inconsistent vehicle types: 
# MAGIC truck to LMV
# MAGIC bike to TwoWheeler

# COMMAND ----------

# 1.Mandatory Column Check - Drop any record where any of the following columns is NULL:shipment_id, role

cleansed_df1 = final_df.na.drop(how="any",subset=["shipment_id","role"])
#display(cleansed_df1)

# 2.Name Completeness Rule - Drop records where both of the following columns are NULL: first_name, last_name 
cleansed_df2 = cleansed_df1.na.drop(how="all",subset=["first_name","last_name"])
#display(cleansed_df2)

# 3.Join Readiness Rule - Drop records where the join key is null: shipment_id
#cleansed_df3 = cleansed_df2.filter(~col("shipment_id").rlike("[a-zA-Z]"))
cleansed_df3 = cleansed_df2.where("shipment_id not rlike '[a-zA-Z]' AND shipment_id IS NOT NULL")
#display(cleansed_df3)


# Scrubbing
# 4. Age Defaulting Rule - Fill NULL values in the age column with: -1
scrubbed_df1 = cleansed_df3.na.fill('-1',subset=["age"]) 
#display(scrubbed_df1)

# 5.Vehicle Type Default Rule - Fill NULL values in the vehicle_type column with: UNKNOWN
scrubbed_df2 = scrubbed_df1.na.fill("UNKNOWN",subset=["vehicle_type"])
#display(scrubbed_df2)

# 6.Invalid Age Replacement - Replace the following values in age: "ten" to -1, "" to -1
find_replace = {"ten": "-1","": "-1" }
scrubbed_df3 = scrubbed_df2.na.replace(find_replace,subset=['age'])
#display(scrubbed_df3)

# 7.Vehicle Type Normalization - Replace inconsistent vehicle types: truck to LMV bike to TwoWheeler
find_replace_2 = {'Truck':'LMV','Bike':'TwoWheeler'}
scrubbed_df4 = scrubbed_df3.na.replace(find_replace_2,subset=["vehicle_type"])
display(scrubbed_df4)


# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Standardization, De-Duplication and Replacement / Deletion of Data to make it in a usable format

# COMMAND ----------

# MAGIC %md
# MAGIC Creating shipments Details data Dataframe creation <br>
# MAGIC 1. Create a DF by Reading Data from logistics_shipment_detail.json
# MAGIC 2. As this data is a clean json data, it doesn't require any cleansing or scrubbing.

# COMMAND ----------

dfjson1 = spark.read.json("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_shipment_detail_3000.json",multiLine=True)
dfjson1.printSchema()
display(dfjson1)

#str1="shipment_id int,order_id string,source_city string,destination_city string,shipment_status string,cargo_type string,vehicle_type string,payment_mode string,shipment_weight_kg double,shipment_cost double,shipment_date date"
#dfjson1=spark.read.schema(str1).json("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_shipment_detail_3000.json",multiLine=True)
#display(dfjson1)

# COMMAND ----------

# MAGIC %md
# MAGIC Standardizations:<br>
# MAGIC
# MAGIC 1. Add a column<br> 
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>: domain as 'Logistics',  current timestamp 'ingestion_timestamp' and 'False' as 'is_expedited'
# MAGIC 2. Column Uniformity: 
# MAGIC role - Convert to lowercase<br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC vehicle_type - Convert values to UPPERCASE<br>
# MAGIC Source Files: DF of logistics_shipment_detail_3000.json
# MAGIC hub_location - Convert values to initcap case<br>
# MAGIC Source Files: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC 3. Format Standardization:<br>
# MAGIC Source Files: DF of logistics_shipment_detail_3000.json<br>
# MAGIC Convert shipment_date to yyyy-MM-dd<br>
# MAGIC Ensure shipment_cost has 2 decimal precision<br>
# MAGIC 4. Data Type Standardization<br>
# MAGIC Standardizing column data types to fix schema drift and enable mathematical operations.<br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2) <br>
# MAGIC age: Cast String to Integer<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC shipment_weight_kg: Cast to Double<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC is_expedited: Cast to Boolean<br>
# MAGIC 5. Naming Standardization <br>
# MAGIC Source File: DF of merged(logistics_source1 & logistics_source2)<br>
# MAGIC Rename: first_name to staff_first_name<br>
# MAGIC Rename: last_name to staff_last_name<br>
# MAGIC Rename: hub_location to origin_hub_city<br>
# MAGIC 6. Reordering columns logically in a better standard format:<br>
# MAGIC Source File: DF of Data from all 3 files<br>
# MAGIC shipment_id (Identifier), staff_first_name (Dimension)staff_last_name (Dimension), role (Dimension), origin_hub_city (Location), shipment_cost (Metric), ingestion_timestamp (Audit)

# COMMAND ----------

#Add a column
#Source File: DF of logistics_shipment_detail_3000.json
#Domain as 'Logistics', current timestamp 'ingestion_timestamp' and 'False' as 'is_expedited'

from pyspark.sql.functions import upper,lower,col,lit,initcap,lpad,to_date,when,current_timestamp
add_col_df = dfjson1.withColumn("domain",lit('Logistics'))
#display(add_col_df)
add_col_df1 = add_col_df.withColumn("ingestion_timestamp",current_timestamp())
#display(add_col_df1)
expedited_df = add_col_df1.withColumn("is_expedited",lit("False"))
#display(expedited_df)

#2.Column Uniformity: role - Convert to lowercase
#Source File: DF of merged(logistics_source1 & logistics_source2)
#vehicle_type - Convert values to UPPERCASE
#Source Files: DF of merged(logistics_source1 & logistics_source2) - hub_location - Convert values to initcap case

col_uni_df = scrubbed_df4.withColumn("role",lower(col("role")))
#display(col_uni_df)
col_uni_df1= col_uni_df.withColumn('vehicle_type',upper(col('vehicle_type')))
#display(col_uni_df1)
col_uni_df2 = col_uni_df1.withColumn("hub_location",initcap(col("hub_location")))
#display(col_uni_df2)


#3.Format Standardization:
#Source Files: logistics_shipment_detail_3000.json Convert shipment_ref to string
#Pad to 10 characters with leading zeros
#Convert dispatch_date to yyyy-MM-dd
#Ensure delivery_cost has 2 decimal precision
df = dfjson1.withColumnRenamed("shipment_id","shipment_ref").withColumnRenamed("shipment_date","dispatch_date").withColumnRenamed("shipment_cost","delivery_cost")
format_fun_df = df.withColumn("shipment_id",lpad(col("shipment_id").cast("string"),10,"0"))
#display(df)

date_coversion_df = df.withColumn("dispatch_date",to_date(col("dispatch_date"),"yyyy/MM/dd"))
#display(date_coversion_df)

delivery_cost_df = df.withColumn("delivery_cost",col("delivery_cost").cast("double"))
#display(delivery_cost_df)

#4. Data Type Standardization
#Standardizing column data types to fix schema drift and enable mathematical operations.
#Source File: logistics_source1 & logistics_source2
#age: Cast String to Integer
#Source File: logistics_shipment_detail_3000.json
#shipment_weight_kg: Cast to Double
#Source File: logistics_shipment_detail_3000.json
#is_expedited: Cast to Boolean

dtype_df= scrubbed_df4.withColumn("shipment_id",col("shipment_id").cast("int")).withColumn("first_name",col("first_name").cast("string")).withColumn("last_name",col("last_name").cast("string")).withColumn("age",col("age").cast("int"))
dtype_df = dtype_df.withColumn("role",col("role").cast("string")).withColumn("vehicle_type",col("vehicle_type").cast("string")).withColumn("data_source",col("data_source").cast("string"))
#display(dtype_df)


dtype_df2 = dfjson1.withColumn("shipment_weight_kg",col("shipment_weight_kg").cast("double"))
#display(dtype_df2)

expedited_cast_df = expedited_df.withColumn("is_expedited",when((col("shipment_status")=="IN_TRANSIT"),True).otherwise(False))
#display(expedited_cast_df)

#5.Naming Standardization
#Source File: logistics_source1 & logistics_source2
#Rename: first_name to staff_first_name
#Rename: last_name to staff_last_name
#Rename: hub_location to origin_hub_city

naming_df = scrubbed_df4.withColumnsRenamed({'first_name':'staff_first_name','last_name':'staff_last_name','hub_location':'origin_hub_city'})
#display(naming_df)

#6.Reordering columns logically in a better standard format:
#Source File: All 3 files
#shipment_id (Identifier), staff_first_name (Dimension)staff_last_name (Dimension), role (Dimension), origin_hub_city (Location), shipment_cost (Metric), ingestion_timestamp (Audit)
expedited_cast_df = expedited_cast_df.select("shipment_id","order_id","cargo_type","vehicle_type","shipment_status","payment_mode","destination_city","source_city","shipment_weight_kg","shipment_cost","shipment_date","domain","is_expedited","ingestion_timestamp")
#display(expedited_cast_df)

raw_data_df1 = raw_data_df1.select("shipment_id", "first_name","last_name","role","age","data_source")
#display(raw_data_df1)

raw_data_df2 = raw_data_df2.select("shipment_id", "first_name","last_name","role","age","vehicle_type","hub_location","data_source")
#display(raw_data_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC Deduplication:
# MAGIC 1. Apply Record Level De-Duplication
# MAGIC 2. Apply Column Level De-Duplication (Primary Key Enforcement)

# COMMAND ----------

duplicate_df = dfjson1.dropDuplicates()
#display(duplicate_df.count())

col_duplicate_df = dfjson1.dropDuplicates(["shipment_id"])
display(col_duplicate_df.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Data Enrichment - Detailing of data
# MAGIC Makes your data rich and detailed <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Adding of Columns (Data Enrichment)
# MAGIC *Creating new derived attributes to enhance traceability and analytical capability.*
# MAGIC
# MAGIC **1. Add Audit Timestamp (`load_dt`)**
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** We need to track exactly when this record was ingested into our Data Lakehouse for auditing purposes.
# MAGIC * **Action:** Add a column `load_dt` using the function `current_timestamp()`.
# MAGIC
# MAGIC **2. Create Full Name (`full_name`)**
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** The reporting dashboard requires a single field for the driver's name instead of separate columns.
# MAGIC * **Action:** Create `full_name` by concatenating `first_name` and `last_name` with a space separator.
# MAGIC * **Result:** "Rajesh" + " " + "Kumar" -> **"Rajesh Kumar"**
# MAGIC
# MAGIC **3. Define Route Segment (`route_segment`)**
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** The logistics team wants to analyze performance based on specific transport lanes (Source to Destination).
# MAGIC * **Action:** Combine `source_city` and `destination_city` with a hyphen.
# MAGIC * **Result:** "Chennai" + "-" + "Pune" -> **"Chennai-Pune"**
# MAGIC
# MAGIC **4. Generate Vehicle Identifier (`vehicle_identifier`)**
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** We need a unique tracking code that immediately tells us the vehicle type and the shipment ID.
# MAGIC * **Action:** Combine `vehicle_type` and `shipment_id` to create a composite key.
# MAGIC * **Result:** "Truck" + "_" + "500001" -> **"Truck_500001"**

# COMMAND ----------

# 1. Add Audit Timestamp (load_dt) Source File: logistics_source1 and logistics_source2

'''Scenario: We need to track exactly when this record was ingested into our Data Lakehouse for auditing purposes.
Action: Add a column load_dt using the function current_timestamp().'''

audit_df = scrubbed_df4.withColumn("load_dt",current_timestamp())
display(audit_df)

#2. Create Full Name (full_name) Source File: logistics_source1 and logistics_source2

'''Scenario: The reporting dashboard requires a single field for the driver's name instead of separate columns.
Action: Create full_name by concatenating first_name and last_name with a space separator.
Result: "Rajesh" + " " + "Kumar" -> "Rajesh Kumar"'''
from pyspark.sql.functions import concat_ws
concat_df = audit_df.withColumn('full_name',concat_ws(" ",col("first_name"),col("last_name")))
display(concat_df)

#3. Define Route Segment (route_segment) Source File: logistics_shipment_detail_3000.json

'''Scenario: The logistics team wants to analyze performance based on specific transport lanes (Source to Destination).
Action: Combine source_city and destination_city with a hyphen.
Result: "Chennai" + "-" + "Pune" -> "Chennai-Pune"'''
from pyspark.sql.functions import concat_ws
concat_df1 = dfjson1.withColumn('source_destination',concat_ws("-",col("source_city"),col("destination_city")))
display(concat_df1)

#4. Generate Vehicle Identifier (vehicle_identifier) Source File: logistics_shipment_detail_3000.json

'''Scenario: We need a unique tracking code that immediately tells us the vehicle type and the shipment ID.
Action: Combine vehicle_type and shipment_id to create a composite key.
Result: "Truck" + "_" + "500001" -> "Truck_500001"'''
from pyspark.sql.functions import concat_ws
concat_df2 = dfjson1.withColumn('tracking_code',concat_ws("_",col("vehicle_type"),col("shipment_id")))
display(concat_df2)




# COMMAND ----------

# MAGIC %md
# MAGIC ###### Deriving of Columns (Time Intelligence)
# MAGIC *Extracting temporal features from dates to enable period-based analysis and reporting.*<br>
# MAGIC Source File: logistics_shipment_detail_3000.json<br>
# MAGIC **1. Derive Shipment Year (`shipment_year`)**
# MAGIC * **Scenario:** Management needs an annual performance report to compare growth year-over-year.
# MAGIC * **Action:** Extract the year component from `shipment_date`.
# MAGIC * **Result:** "2024-04-23" -> **2024**
# MAGIC
# MAGIC **2. Derive Shipment Month (`shipment_month`)**
# MAGIC * **Scenario:** Analysts want to identify seasonal peaks (e.g., increased volume in December).
# MAGIC * **Action:** Extract the month component from `shipment_date`.
# MAGIC * **Result:** "2024-04-23" -> **4** (April)
# MAGIC
# MAGIC **3. Flag Weekend Operations (`is_weekend`)**
# MAGIC * **Scenario:** The Operations team needs to track shipments handled during weekends to calculate overtime pay or analyze non-business day capacity.
# MAGIC * **Action:** Flag as **'True'** if the `shipment_date` falls on a Saturday or Sunday.
# MAGIC
# MAGIC **4. Flag shipment status (`is_expedited`)**
# MAGIC * **Scenario:** The Operations team needs to track shipments is IN_TRANSIT or DELIVERED.
# MAGIC * **Action:** Flag as **'True'** if the `shipment_status` IN_TRANSIT or DELIVERED.

# COMMAND ----------

#1. Derive Shipment Year (shipment_year)

'''Scenario: Management needs an annual performance report to compare growth year-over-year.
Action: Extract the year component from shipment_date.
Result: "2024-04-23" -> 2024'''

from pyspark.sql.functions import year
year_extract_df = expedited_cast_df.select("*",year("shipment_date").alias("shipment_year"))
#display(year_extract_df)

#2. Derive Shipment Month (shipment_month)

'''Scenario: Analysts want to identify seasonal peaks (e.g., increased volume in December).
Action: Extract the month component from shipment_date.
Result: "2024-04-23" -> 4 (April)'''

from pyspark.sql.functions import year,month,date_format
year_month_df = year_extract_df.select("*",date_format("shipment_date","MMMM").alias("shipment_month"))
#display(year_month_df)

#3. Flag Weekend Operations (is_weekend)

'''Scenario: The Operations team needs to track shipments handled during weekends to calculate overtime pay or analyze non-business day capacity.
Action: Flag as 'True' if the shipment_date falls on a Saturday or Sunday.
'''

from pyspark.sql.functions import dayofweek,when,col,when
flag_df = year_month_df.select("*",when(dayofweek(col("shipment_date")).isin([1,7]),True).otherwise(False).alias("flag"))
#display(flag_df)

# OR

from pyspark.sql.functions import dayofweek,when,col
flag_df = year_month_df.withColumn("Flag",when(dayofweek(col('shipment_date')).isin([1,7]),True).otherwise(False))
#display(flag_df)

#4. Flag shipment status (is_expedited)

'''Scenario: The Operations team needs to track shipments is IN_TRANSIT or DELIVERED.
Action: Flag as 'True' if the shipment_status IN_TRANSIT or DELIVERED.
'''
from pyspark.sql.functions import when,col
flag_df1 = expedited_cast_df.withColumn("is_expedited",when((col("shipment_status")=="IN_TRANSIT") | (col("shipment_status")=="DELIVERED"),True).otherwise(False))
display(flag_df1)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Enrichment/Business Logics (Calculated Fields)
# MAGIC *Deriving new metrics and financial indicators using mathematical and date-based operations.*<br>
# MAGIC Source File: logistics_shipment_detail_3000.json<br>
# MAGIC
# MAGIC **1. Calculate Unit Cost (`cost_per_kg`)**
# MAGIC * **Scenario:** The Finance team wants to analyze the efficiency of shipments by determining the cost incurred per unit of weight.
# MAGIC * **Action:** Divide `shipment_cost` by `shipment_weight_kg`.
# MAGIC * **Logic:** `shipment_cost / shipment_weight_kg`
# MAGIC
# MAGIC **2. Track Shipment Age (`days_since_shipment`)**
# MAGIC * **Scenario:** The Operations team needs to monitor how long it has been since a shipment was dispatched to identify potential delays.
# MAGIC * **Action:** Calculate the difference in days between the `current_date` and the `shipment_date`.
# MAGIC * **Logic:** `datediff(current_date(), shipment_date)`
# MAGIC
# MAGIC **3. Compute Tax Liability (`tax_amount`)**
# MAGIC * **Scenario:** For invoicing and compliance, we must calculate the Goods and Services Tax (GST) applicable to each shipment.
# MAGIC * **Action:** Calculate 18% GST on the total `shipment_cost`.
# MAGIC * **Logic:** `shipment_cost * 0.18`

# COMMAND ----------

#1. Calculate Unit Cost (cost_per_kg)

"""Scenario: The Finance team wants to analyze the efficiency of shipments by determining the cost incurred per unit of weight.
Action: Divide shipment_cost by shipment_weight_kg.
Logic: shipment_cost / shipment_weight_kg"""
from pyspark.sql.functions import round
cost_unit_of_weight = expedited_cast_df.withColumn("cost_per_unit",round(col("shipment_cost")/col("shipment_weight_kg"),2))
#display(cost_unit_of_weight.select("shipment_id","order_id","cargo_type","vehicle_type","shipment_status","payment_mode","destination_city","source_city","shipment_weight_kg","cost_per_unit","shipment_cost","shipment_date","domain","is_expedited","ingestion_timestamp"))

#2. Track Shipment Age (days_since_shipment)

"""Scenario: The Operations team needs to monitor how long it has been since a shipment was dispatched to identify potential delays.
Action: Calculate the difference in days between the current_date and the shipment_date.
Logic: datediff(current_date(), shipment_date)"""
from pyspark.sql.functions import datediff,current_date
days_diff_df = cost_unit_of_weight.withColumn("delay_days",datediff(current_date(),"shipment_date"))
#display(days_diff_df.select("shipment_id","order_id","cargo_type","vehicle_type","shipment_status","payment_mode","destination_city","source_city","shipment_weight_kg","cost_per_unit","shipment_cost","shipment_date","delay_days","domain","is_expedited","ingestion_timestamp"))

#3. Compute Tax Liability (tax_amount)

"""Scenario: For invoicing and compliance, we must calculate the Goods and Services Tax (GST) applicable to each shipment.
Action: Calculate 18% GST on the total shipment_cost.
Logic: shipment_cost * 0.18"""
from pyspark.sql.functions import round
tax_calc_df = days_diff_df.withColumn("tax_cost",round(col("shipment_cost")*0.18,2))
display(tax_calc_df.select("shipment_id","order_id","cargo_type","vehicle_type","shipment_status","payment_mode","destination_city","source_city","shipment_weight_kg","cost_per_unit","shipment_cost","tax_cost","shipment_date","delay_days","domain","is_expedited","ingestion_timestamp"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Remove/Eliminate (drop, select, selectExpr)
# MAGIC *Excluding unnecessary or redundant columns to optimize storage and privacy.*<br>
# MAGIC Source File: DF of logistics_source1 and logistics_source2<br>
# MAGIC
# MAGIC **1. Remove Redundant Name Columns**
# MAGIC * **Scenario:** Since we have already created the `full_name` column in the Enrichment step, the individual name columns are now redundant and clutter the dataset.
# MAGIC * **Action:** Drop the `first_name` and `last_name` columns.
# MAGIC * **Logic:** `df.drop("first_name", "last_name")`

# COMMAND ----------

#1. Remove Redundant Name Columns

"""Scenario: Since we have already created the full_name column in the Enrichment step, the individual name columns are now redundant and clutter the dataset.
Action: Drop the first_name and last_name columns.
Logic: df.drop("first_name", "last_name")"""

drop_first_last_name_df = concat_df.drop("first_name","last_name")
source2_df = drop_first_last_name_df.select("shipment_id","full_name","role","age","hub_location","vehicle_type","data_source","load_dt")
display(source2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Splitting & Merging/Melting of Columns
# MAGIC *Reshaping columns to extract hidden values or combine fields for better analysis.*<br>
# MAGIC Source File: DF of logistics_shipment_detail_3000.json<br>
# MAGIC **1. Splitting (Extraction)**
# MAGIC *Breaking one column into multiple to isolate key information.*
# MAGIC * **Split Order Code:**
# MAGIC   * **Action:** Split `order_id` ("ORD100000") into two new columns:
# MAGIC     * `order_prefix` ("ORD")
# MAGIC     * `order_sequence` ("100000")
# MAGIC * **Split Date:**
# MAGIC   * **Action:** Split `shipment_date` into three separate columns for partitioning:
# MAGIC     * `ship_year` (2024)
# MAGIC     * `ship_month` (4)
# MAGIC     * `ship_day` (23)
# MAGIC
# MAGIC **2. Merging (Concatenation)**
# MAGIC *Combining multiple columns into a single unique identifier or description.*
# MAGIC * **Create Route ID:**
# MAGIC   * **Action:** Merge `source_city` ("Chennai") and `destination_city` ("Pune") to create a descriptive route key:
# MAGIC     * `route_lane` ("Chennai->Pune")

# COMMAND ----------

#1. Splitting (Extraction) Breaking one column into multiple to isolate key information.

"""Split Order Code:
Action: Split order_id ("ORD100000") into two new columns:
order_prefix ("ORD")
order_sequence ("100000")"""

from pyspark.sql.functions import split,substring
split_df = tax_calc_df.withColumn("order_prefix",substring(col("order_id"),1,3)).withColumn("order_sequence",substring(col("order_id"),4,9))
#display(split_df.select("shipment_id","order_id","order_prefix","order_sequence","cargo_type","vehicle_type","shipment_status","payment_mode","destination_city","source_city","shipment_weight_kg","cost_per_unit","shipment_cost","tax_cost","shipment_date","delay_days","domain","is_expedited","ingestion_timestamp"))

"""Split Date:
Action: Split shipment_date into three separate columns for partitioning:
ship_year (2024)
ship_month (4)
ship_day (23)"""
from pyspark.sql.functions import year,month,dayofmonth
split_df1 = split_df.withColumn("ship_year",year("shipment_date")).withColumn("ship_month",month("shipment_date")).withColumn("ship_day",dayofmonth("shipment_date"))
#display(split_df1.select("shipment_id","order_id","order_prefix","order_sequence","cargo_type","vehicle_type","shipment_status","payment_mode","destination_city","source_city","shipment_weight_kg","cost_per_unit","shipment_cost","tax_cost","shipment_date","ship_year","ship_month","ship_day","delay_days","domain","is_expedited","ingestion_timestamp"))

#2. Merging (Concatenation) Combining multiple columns into a single unique identifier or description.

"""Create Route ID:
Action: Merge source_city ("Chennai") and destination_city ("Pune") to create a descriptive route key:
route_lane ("Chennai->Pune")"""
from pyspark.sql.functions import concat_ws
route_id_df = split_df1. withColumn("route_lane",concat_ws("->","source_city","destination_city"))
display(route_id_df.select("shipment_id","order_id","order_prefix","order_sequence","cargo_type","vehicle_type","shipment_status","payment_mode","source_city","destination_city","route_lane","shipment_weight_kg","cost_per_unit","shipment_cost","tax_cost","shipment_date","ship_year","ship_month","ship_day","delay_days","domain","is_expedited","ingestion_timestamp"))



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Customization & Processing - Application of Tailored Business Specific Rules
# MAGIC
# MAGIC ### **UDF1: Complex Incentive Calculation**
# MAGIC **Scenario:** The Logistics Head wants to calculate a "Performance Bonus" for drivers based on tenure and role complexity.
# MAGIC
# MAGIC **Action:** Create a Python function `calculate_bonus(role, age)` and register it as a Spark UDF.
# MAGIC
# MAGIC **Logic:**
# MAGIC * **IF** `Role` == 'Driver' **AND** `Age` > 50:
# MAGIC   * `Bonus` = 15% of Salary (Reward for Seniority)
# MAGIC * **IF** `Role` == 'Driver' **AND** `Age` < 30:
# MAGIC   * `Bonus` = 5% of Salary (Encouragement for Juniors)
# MAGIC * **ELSE**:
# MAGIC   * `Bonus` = 0
# MAGIC
# MAGIC **Result:** A new derived column `projected_bonus` is generated for every row in the dataset.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **UDF2: PII Masking (Privacy Compliance)**
# MAGIC **Scenario:** For the analytics dashboard, we must hide the full identity of the staff to comply with privacy laws (GDPR/DPDP), while keeping names recognizable for internal managers.
# MAGIC
# MAGIC **Business Rule:** Show the first 2 letters, mask the middle characters with `****`, and show the last letter.
# MAGIC
# MAGIC **Action:** Create a UDF `mask_identity(name)`.
# MAGIC
# MAGIC **Example:**
# MAGIC * **Input:** `"Rajesh"`
# MAGIC * **Output:** `"Ra****h"`
# MAGIC <br>
# MAGIC **Note: Convert the above udf logic to inbult function based transformation to ensure the performance is improved.**

# COMMAND ----------

#UDF1: Complex Incentive Calculation
'''Scenario: The Logistics Head wants to calculate a "Performance Bonus" for drivers based on tenure and role complexity.
Action: Create a Python function calculate_bonus(role, age) and register it as a Spark UDF.
Logic:
IF Role == 'Driver' AND Age > 50:
Bonus = 15% of Salary (Reward for Seniority)
IF Role == 'Driver' AND Age < 30:
Bonus = 5% of Salary (Encouragement for Juniors)
ELSE:
Bonus = 0
Result: A new derived column projected_bonus is generated for every row in the dataset.'''

def calculate_bonus(role,age):
    age = int(age)                     # The age is string datatype in given dataset,that's why we are typecasting the datatype while writing function
    if  role == 'Driver' and age > 50:
        return 0.15
    elif role == 'Driver' and age < 30:
        return 0.05
    else:
        return 0
    
#UDF2: PII Masking (Privacy Compliance)
"""Scenario: For the analytics dashboard, we must hide the full identity of the staff to comply with privacy laws (GDPR/DPDP), while keeping names recognizable for internal managers.

Business Rule: Show the first 2 letters, mask the middle characters with ****, and show the last letter.

Action: Create a UDF mask_identity(name).

Example:

Input: "Rajesh"
Output: "Ra****h"
**Note: Convert the above udf logic to inbult function based transformation to ensure the performance is improved.**"""

def mask_identity(name):
    return name[0:2]+"****"+name[-1]


# COMMAND ----------

from pyspark.sql.functions import udf

bonusfn= udf(calculate_bonus)
perf_bonus_df = source2_df.withColumn("projected_bonus",bonusfn(col("role"),col("age")))
#display(perf_bonus_df)

maskfn = udf(mask_identity)
mask_identity_df = perf_bonus_df.withColumn("name",maskfn(col("full_name")))
display(mask_identity_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Core Curation & Processing (Pre-Wrangling)
# MAGIC *Applying business logic to focus, filter, and summarize data before final analysis.*
# MAGIC
# MAGIC **1. Select (Projection)**<br>
# MAGIC Source Files: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** The Driver App team only needs location data, not sensitive HR info.
# MAGIC * **Action:** Select only `first_name`, `role`, and `hub_location`.
# MAGIC
# MAGIC **2. Filter (Selection)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** We need a report on active operational problems.
# MAGIC * **Action:** Filter rows where `shipment_status` is **'DELAYED'** or **'RETURNED'**.
# MAGIC * **Scenario:** Insurance audit for senior staff.
# MAGIC * **Action:** Filter rows where `age > 50`.
# MAGIC
# MAGIC **3. Derive Flags & Columns (Business Logic)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Identify high-value shipments for security tracking.
# MAGIC * **Action:** Create flag `is_high_value` = **True** if `shipment_cost > 50,000`.
# MAGIC * **Scenario:** Flag weekend operations for overtime calculation.
# MAGIC * **Action:** Create flag `is_weekend` = **True** if day is Saturday or Sunday.
# MAGIC
# MAGIC **4. Format (Standardization)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Finance requires readable currency formats.
# MAGIC * **Action:** Format `shipment_cost` to string like **"₹30,695.80"**.
# MAGIC * **Scenario:** Standardize city names for reporting.
# MAGIC * **Action:** Format `source_city` to Uppercase (e.g., "chennai" → **"CHENNAI"**).
# MAGIC
# MAGIC **5. Group & Aggregate (Summarization)**<br>
# MAGIC Source Files: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Scenario:** Regional staffing analysis.
# MAGIC * **Action:** Group by `hub_location` and **Count** the number of staff.
# MAGIC * **Scenario:** Fleet capacity analysis.
# MAGIC * **Action:** Group by `vehicle_type` and **Sum** the `shipment_weight_kg`.
# MAGIC
# MAGIC **6. Sorting (Ordering)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Prioritize the most expensive shipments.
# MAGIC * **Action:** Sort by `shipment_cost` in **Descending** order.
# MAGIC * **Scenario:** Organize daily dispatch schedule.
# MAGIC * **Action:** Sort by `shipment_date` (Ascending) then `priority_flag` (Descending).
# MAGIC
# MAGIC **7. Limit (Top-N Analysis)**<br>
# MAGIC Source File: DF of json<br>
# MAGIC * **Scenario:** Dashboard snapshot of critical delays.
# MAGIC * **Action:** Filter for 'DELAYED', Sort by Cost, and **Limit to top 10** rows.

# COMMAND ----------

# DBTITLE 1,Select, Filter, and Business Logic
#1. Select (Projection)
'''Source Files: DF of logistics_source1 and logistics_source2

Scenario: The Driver App team only needs location data, not sensitive HR info.
Action: Select only first_name, role, and hub_location.'''

projection_df = concat_df.select("first_name","role","hub_location")
#display(projection_dfter_df)

#2. Filter (Selection)
'''Source File: DF of json'''
"""Scenario: We need a report on active operational problems.
Action: Filter rows where shipment_status is 'DELAYED' or 'CANCELED'."""


selection_df = expedited_cast_df.filter((col("shipment_status")=='DELAYED') | (col("shipment_status")=='CANCELED'))
#display(selection_df)

selection_df1 = expedited_cast_df.where("shipment_status = 'DELAYED' " or "shipment_status = 'CANCELED'")
#display(selection_df1)

"""Scenario: Insurance audit for senior staff.
Action: Filter rows where age > 50."""
filter_df1 = concat_df.where("age > 50")
#display(filter_df1)

filter_df2 = concat_df.filter(col("age") > 50)
#display(filter_df2)

#3. Derive Flags & Columns (Business Logic)
"""Source File: DF of json
Scenario: Identify high-value shipments for security tracking.
Action: Create flag is_high_value = True if shipment_cost > 50,000."""

flag_df1 = expedited_cast_df.withColumn("is_high_value",col("shipment_cost")> 50000)
#display(flag_df1)

"""Scenario: Flag weekend operations for overtime calculation.
Action: Create flag is_weekend = True if day is Saturday or Sunday."""

flag_df2 = flag_df1.withColumn("is_weekend",when(dayofweek(col('shipment_date')).isin([1,7]),True).otherwise(False))
#display(flag_df2)

#4.Format (Standardization)
'''Source File: DF of json
Scenario: Finance requires readable currency formats.
Action: Format shipment_cost to string like "₹30,695.80".'''
from pyspark.sql.functions import concat_ws
currency_df = flag_df2.withColumn("shipment_cost",concat_ws("",lit('₹'),col("shipment_cost")))
#display(currency_df)

'''Scenario: Standardize city names for reporting.
Action: Format source_city to Uppercase (e.g., "chennai" → "CHENNAI").'''
upper_city_df = currency_df.withColumn("source_city",upper("source_city"))
#display(upper_city_df)

#5. Group & Aggregate (Summarization)
'''Source Files: DF of logistics_source1 and logistics_source2
Scenario: Regional staffing analysis.
Action: Group by hub_location and Count the number of staff.'''
from pyspark.sql.functions import count
reg_staff_df = concat_df.groupBy("hub_location").agg(count("full_name").alias("staff_count"))
#display(reg_staff_df)

'''Scenario: Fleet capacity analysis.
Action: Group by vehicle_type and Sum the shipment_weight_kg.'''
from pyspark.sql.functions import sum,round
grpby_df = flag_df2.groupBy("vehicle_type").agg(round(sum("shipment_weight_kg"),2).alias("sum_of_shipment_weight"))
#display(grpby_df)

#6. Sorting (Ordering)
'''Source File: DF of json
Scenario: Prioritize the most expensive shipments.
Action: Sort by shipment_cost in Descending order.'''
sort_df = flag_df2.orderBy(col("shipment_cost"),ascending=False)
#display(sort_df)

'''Scenario: Organize daily dispatch schedule.
Action: Sort by shipment_date (Ascending) then priority_flag (Descending).'''
sort_df1 = sort_df.orderBy(col("shipment_date"),col("shipment_cost"),ascending=[True,False])
#display(sort_df1)

#7. Limit (Top-N Analysis)
'''Source File: DF of json
Scenario: Dashboard snapshot of critical delays.
Action: Filter for 'DELAYED', Sort by Cost, and Limit to top 10 rows.'''
fil_df = flag_df2.filter(col("shipment_status")=='DELAYED').orderBy(col("shipment_cost"),ascending= False).limit(10)
display(fil_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Wrangling - Transformation & Analytics
# MAGIC *Combining, modeling, and analyzing data to answer complex business questions.*
# MAGIC
# MAGIC ### **1. Joins**
# MAGIC Source Files:<br>
# MAGIC Left Side (staff_df):<br> DF of logistics_source1 & logistics_source2<br>
# MAGIC Right Side (shipments_df):<br> DF of logistics_shipment_detail_3000.json<br>
# MAGIC #### **1.1 Frequently Used Simple Joins (Inner, Left)**
# MAGIC * **Inner Join (Performance Analysis):**
# MAGIC   * **Scenario:** We only want to analyze *completed work*. Connect Staff to the Shipments they handled.
# MAGIC   * **Action:** Join `staff_df` and `shipments_df` on `shipment_id`.
# MAGIC   * **Result:** Returns only rows where a staff member is assigned to a valid shipment.
# MAGIC * **Left Join (Idle Resource check):**
# MAGIC   * **Scenario:** Find out which staff members are currently *idle* (not assigned to any shipment).
# MAGIC   * **Action:** Join `staff_df` (Left) with `shipments_df` (Right) on `shipment_id`. Filter where `shipments_df.shipment_id` is NULL.
# MAGIC
# MAGIC #### **1.2 Infrequent Simple Joins (Self, Right, Full, Cartesian)**
# MAGIC * **Self Join (Peer Finding):**
# MAGIC   * **Scenario:** Find all pairs of employees working in the same `hub_location`.
# MAGIC   * **Action:** Join `staff_df` to itself on `hub_location`, filtering where `staff_id_A != staff_id_B`.
# MAGIC * **Right Join (Orphan Data Check):**
# MAGIC   * **Scenario:** Identify shipments in the system that have *no valid driver* assigned (Data Integrity Issue).
# MAGIC   * **Action:** Join `staff_df` (Left) with `shipments_df` (Right). Focus on NULLs on the left side.
# MAGIC * **Full Outer Join (Reconciliation):**
# MAGIC   * **Scenario:** A complete audit to find *both* idle drivers AND unassigned shipments in one view.
# MAGIC   * **Action:** Perform a Full Outer Join on `shipment_id`.
# MAGIC * **Cartesian/Cross Join (Capacity Planning):**
# MAGIC   * **Scenario:** Generate a schedule of *every possible* driver assignment to *every* pending shipment to run an optimization algorithm.
# MAGIC   * **Action:** Cross Join `drivers_df` and `pending_shipments_df`.
# MAGIC
# MAGIC #### **1.3 Advanced Joins (Semi and Anti)**
# MAGIC * **Left Semi Join (Existence Check):**
# MAGIC   * **Scenario:** "Show me the details of Drivers who have *at least one* shipment." (Standard filtering).
# MAGIC   * **Action:** `staff_df.join(shipments_df, "shipment_id", "left_semi")`.
# MAGIC   * **Benefit:** Performance optimization; it stops scanning the right table once a match is found.
# MAGIC * **Left Anti Join (Negation Check):**
# MAGIC   * **Scenario:** "Show me the details of Drivers who have *never* touched a shipment."
# MAGIC   * **Action:** `staff_df.join(shipments_df, "shipment_id", "left_anti")`.
# MAGIC
# MAGIC ### **2. Lookup**<br>
# MAGIC Source File: DF of logistics_source1 and logistics_source2 (merged into Staff DF)<br>
# MAGIC * **Scenario:** Validation. Check if the `hub_location` in the staff file exists in the corporate `Master_City_List`.
# MAGIC * **Action:** Compare values against a reference list.
# MAGIC
# MAGIC ### **3. Lookup & Enrichment**<br>
# MAGIC Source File: DF of logistics_source1 and logistics_source2 (merged into Staff DF)<br>
# MAGIC * **Scenario:** Geo-Tagging.
# MAGIC * **Action:** Lookup `hub_location` ("Pune") in a Master Latitude/Longitude table and enrich the dataset by adding `lat` and `long` columns for map plotting.
# MAGIC
# MAGIC ### **4. Schema Modeling (Denormalization)**<br>
# MAGIC Source Files: DF of All 3 Files (logistics_source1, logistics_source2, logistics_shipment_detail_3000.json)<br>
# MAGIC * **Scenario:** Creating a "Gold Layer" Table for PowerBI/Tableau.
# MAGIC * **Action:** Flatten the Star Schema. Join `Staff`, `Shipments`, and `Vehicle_Master` into one wide table (`wide_shipment_history`) so analysts don't have to perform joins during reporting.
# MAGIC
# MAGIC ### **5. Windowing (Ranking & Trends)**<br>
# MAGIC Source Files:<br>
# MAGIC DF of logistics_source2: Provides hub_location (Partition Key).<br>
# MAGIC logistics_shipment_detail_3000.json: Provides shipment_cost (Ordering Key)<br>
# MAGIC * **Scenario:** "Who are the Top 3 Drivers by Cost in *each* Hub?"
# MAGIC * **Action:**
# MAGIC   1. Partition by `hub_location`.
# MAGIC   2. Order by `total_shipment_cost` Descending.
# MAGIC   3. Apply `dense_rank()` and `row_number()
# MAGIC   4. Filter where `rank or row_number <= 3`.
# MAGIC
# MAGIC ### **6. Analytical Functions (Lead/Lag)**<br>
# MAGIC Source File: <br>
# MAGIC DF of logistics_shipment_detail_3000.json<br>
# MAGIC * **Scenario:** Idle Time Analysis.
# MAGIC * **Action:** For each driver, calculate the days elapsed since their *previous* shipment.
# MAGIC
# MAGIC ### **7. Set Operations**<br>
# MAGIC Source Files: DF of logistics_source1 and logistics_source2<br>
# MAGIC * **Union:** Combining `Source1` (Legacy) and `Source2` (Modern) into one dataset (Already done in Active Munging).
# MAGIC * **Intersect:** Identifying Staff IDs that appear in *both* Source 1 and Source 2 (Duplicate/Migration Check).
# MAGIC * **Except (Difference):** Identifying Staff IDs present in Source 2 but *missing* from Source 1 (New Hires).
# MAGIC
# MAGIC ### **8. Grouping & Aggregations (Advanced)**<br>
# MAGIC Source Files:<br>
# MAGIC DF of logistics_source2: Provides hub_location and vehicle_type (Grouping Dimensions).<br>
# MAGIC DF of logistics_shipment_detail_3000.json: Provides shipment_cost (Aggregation Metric).<br>
# MAGIC * **Scenario:** The CFO wants a subtotal report at multiple levels:
# MAGIC   1. Total Cost by Hub.
# MAGIC   2. Total Cost by Hub AND Vehicle Type.
# MAGIC   3. Grand Total.
# MAGIC * **Action:** Use `cube("hub_location", "vehicle_type")` or `rollup()` to generate all these subtotals in a single query.

# COMMAND ----------

#1. Joins
'''Source Files:
Left Side (staff_df):
DF of logistics_source1 & logistics_source2
Right Side (shipments_df):
DF of logistics_shipment_detail_3000.json'''

df1 = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source1",header=True,inferSchema=True).dropDuplicates()
df1 = df1.where("shipment_id not rlike '[a-zA-Z]' AND age not rlike '[a-zA-Z]' AND shipment_id IS NOT NULL" )

df2 = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source2",header=True,inferSchema = True).dropDuplicates()
df2 = df2.withColumnRenamed("vehicle_type","vehicle_ty")
df2 = df2.where("shipment_id not rlike '[a-zA-Z]' AND age not rlike '[a-zA-Z]' AND shipment_id IS NOT NULL" )

staff_df = df1.unionByName(df2,allowMissingColumns=True)
staff_df = staff_df.na.fill("Not Provided",subset=["hub_location"]).na.fill("NA",subset=["first_name","last_name","role"])
display(staff_df)

from pyspark.sql.types import StructType,StructField,StringType,DateType,DoubleType,LongType

str1 = StructType([StructField('cargo_type', StringType(), True), StructField('destination_city', StringType(), True), StructField('order_id', StringType(), True), StructField('payment_mode', StringType(), True), StructField('shipment_cost', DoubleType(), True), StructField('shipment_date', DateType(), True), StructField('shipment_id', LongType(), True), StructField('shipment_status', StringType(), True), StructField('shipment_weight_kg', DoubleType(), True), StructField('source_city', StringType(), True), StructField('vehicle_type', StringType(), True)])

shipments_df = spark.read.schema(str1).json("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_shipment_detail_3000.json",multiLine=True,mode="Permissive")
shipments_df = shipments_df.where("shipment_id not rlike '[a-zA-Z]' AND shipment_id IS NOT NULL" )
#print(shipments_df.schema)
#display(shipments_df)

#1.1 Frequently Used Simple Joins (Inner, Left)
"""Inner Join (Performance Analysis):
Scenario: We only want to analyze completed work. Connect Staff to the Shipments they handled.
Action: Join staff_df and shipments_df on shipment_id.
Result: Returns only rows where a staff member is assigned to a valid shipment."""

inner_df = staff_df.join(shipments_df,how="inner",on="shipment_id")
#display(inner_df)

left_df =staff_df.join(shipments_df,how="left",on="shipment_id")
#display(left_df)

"""Left Join (Idle Resource check):
Scenario: Find out which staff members are currently idle (not assigned to any shipment).
Action: Join staff_df (Left) with shipments_df (Right) on shipment_id. Filter where shipments_df.shipment_id is NULL.
"""
from pyspark.sql.functions import col
left_df1 =staff_df.join(shipments_df,how="left",on="shipment_id").filter(col("order_id").isNull())
#display(left_df1)

left_df1 =staff_df.join(shipments_df,how="left_anti",on="shipment_id")
#display(left_df1)

#1.2 Infrequent Simple Joins (Self, Right, Full, Cartesian)
"""Self Join (Peer Finding):
Scenario: Find all pairs of employees working in the same hub_location.
Action: Join staff_df to itself on hub_location, filtering where staff_id_A == staff_id_B."""
staff_df1 = staff_df.filter(col("hub_location")!= "Not Provided")
df1 = staff_df1.withColumnRenamed("shipment_id","staff_id_A")
df2 = staff_df1.withColumnRenamed("shipment_id","staff_id_B")
peer_df = df1.join(df2,how="inner",on="hub_location").filter((col("staff_id_A"))==(col("staff_id_B")))
#display(peer_df)

"""Right Join (Orphan Data Check):
Scenario: Identify shipments in the system that have no valid driver assigned (Data Integrity Issue).
Action: Join staff_df (Left) with shipments_df (Right). Focus on NULLs on the left side."""
orphan_df = staff_df.join(shipments_df,how="right",on="shipment_id").filter("role is null")
#display(orphan_df)

"""Full Outer Join (Reconciliation):
Scenario: A complete audit to find both idle drivers AND unassigned shipments in one view.
Action: Perform a Full Outer Join on shipment_id."""
reconciliation_df = staff_df.join(shipments_df,how="full",on="shipment_id")
#display(reconciliation_df)

"""Cartesian/Cross Join (Capacity Planning):
Scenario: Generate a schedule of every possible driver assignment to every pending shipment to run an optimization algorithm.
Action: Cross Join drivers_df and pending_shipments_df."""
drivers_df = staff_df.filter(col("role") == "Driver")
pending_shipments_df = shipments_df.filter(col("shipment_status").isin("IN_TRANSIT", "DELAYED"))
cross_df = drivers_df.join(pending_shipments_df,how="cross")
#display(cross_df)

#1.3 Advanced Joins (Semi and Anti)
#Left Semi Join (Existence Check):
"""Scenario: "Show me the details of Drivers who have at least one shipment." (Standard filtering).
Action: staff_df.join(shipments_df, "shipment_id", "left_semi").
Benefit: Performance optimization; it stops scanning the right table once a match is found."""

std_fil_df = staff_df.join(shipments_df,how="left_semi",on="shipment_id").filter(col("role")== "Driver")
#display(std_fil_df)

#Left Anti Join (Negation Check):
"""Scenario: "Show me the details of Drivers who have never touched a shipment."
Action: staff_df.join(shipments_df, "shipment_id", "left_anti")."""

Negation_df = staff_df.join(shipments_df,how="left_anti",on="shipment_id").filter(col("role")== "Driver")
#display(Negation_df)




# COMMAND ----------

#2. Lookup
#Source File: DF of logistics_source1 and logistics_source2 (merged into Staff DF)

"""Scenario: Validation. Check if the hub_location in the staff file exists in the corporate Master_City_List.
Action: Compare values against a reference list"""

master_df = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/Master_City_List 1.csv",header=True,inferSchema=True)
master_df = master_df.withColumnRenamed("city_name","hub_location")
#display(master_df)

lookup_df = staff_df.join(master_df,how="semi",on="hub_location")
#display(lookup_df)

#3. Lookup & Enrichment
#Source File: DF of logistics_source1 and logistics_source2 (merged into Staff DF)
"""Scenario: Geo-Tagging.
Action: Lookup hub_location ("Pune") in a Master Latitude/Longitude table and enrich the dataset by adding lat and long columns for map plotting."""
staff_df1 = staff_df.filter(col("hub_location")== "Pune")
lookup_enrich_df =  staff_df1.join(master_df.select("hub_location","latitude","longitude"),how="left",on='hub_location')
#display(lookup_enrich_df)



# COMMAND ----------

#4. Schema Modeling (Denormalization)
#Source Files: DF of All 3 Files (logistics_source1, logistics_source2, logistics_shipment_detail_3000.json)

"""Scenario: Creating a "Gold Layer" Table for PowerBI/Tableau.
Action: Flatten the Star Schema. Join Staff, Shipments, and Vehicle_Master into one wide table (wide_shipment_history) so analysts don't have to perform joins during reporting."""

denormalized_df = staff_df.join(shipments_df,how="full",on="shipment_id")
#display(denormalized_df)

#5. Windowing (Ranking & Trends)
"""Source Files:
DF of logistics_source2: Provides hub_location (Partition Key).
logistics_shipment_detail_3000.json: Provides shipment_cost (Ordering Key)
"""
"""Scenario: "Who are the Top 3 Drivers by Cost in each Hub?"
Action:
Partition by hub_location.
Order by total_shipment_cost Descending.
Apply dense_rank() and `row_number()
Filter where rank or row_number <= 3."""
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,desc,lag,datediff,to_date

source_df2 = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source2",header=True,inferSchema = True).dropDuplicates()
source_df2 = source_df2.where("shipment_id not rlike '[a-zA-Z]' AND age not rlike '[a-zA-Z]' AND shipment_id IS NOT NULL" ).withColumnRenamed("vehicle_type","vehicle_ty")

shipments_df = spark.read.schema(str1).json("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_shipment_detail_3000.json",multiLine=True,mode="Permissive")
shipments_df = shipments_df.where("shipment_id not rlike '[a-zA-Z]' AND shipment_id IS NOT NULL" )
#display(shipments_df)

top3_df = source_df2.join(shipments_df,how="full",on="shipment_id").select("shipment_id","order_id","hub_location","role","shipment_cost")
top3_df = top3_df.filter(col("role")== "Driver").withColumn("rn",row_number().over(Window.partitionBy("hub_location").orderBy(desc("shipment_cost")))).where("rn <= 3 ")
display(top3_df)

#6. Analytical Functions (Lead/Lag)
"""Source File: DF of logistics_shipment_detail_3000.json
Scenario: Idle Time Analysis.
Action: For each driver, calculate the days elapsed since their previous shipment."""

top3_df = source_df2.join(shipments_df,how="full",on="shipment_id")

lag_df = top3_df.withColumn("previous_shipment_date",lag("shipment_date",1).over(Window.partitionBy("role").orderBy("shipment_date"))).filter(col("role")=="Driver")

lag_df1 = lag_df.withColumn("shipment_date",to_date("shipment_date","yyyy-MM-dd")).withColumn("previous_shipment_date",to_date("previous_shipment_date","yyyy-MM-dd")).withColumn("days_elapsed",datediff("shipment_date","previous_shipment_date"))
#display(lag_df1)

#7. Set Operations
"""Source Files: DF of logistics_source1 and logistics_source2
Union: Combining Source1 (Legacy) and Source2 (Modern) into one dataset (Already done in Active Munging).
Intersect: Identifying Staff IDs that appear in both Source 1 and Source 2 (Duplicate/Migration Check).
Except (Difference): Identifying Staff IDs present in Source 2 but missing from Source 1 (New Hires)."""
read_df1 = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source1",inferSchema = True,header=True)
read_df2 = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source2",inferSchema = True,header=True)

union_df = read_df1.unionByName(read_df2,allowMissingColumns=True)
#display(union_df)

intersect_df = read_df1.intersect(read_df2)
#display(intersect_df)                        # intersect and except will work, if the number of columns are same in both datasets.

except_df = read_df1.exceptAll(read_df2)
#display(except_df)

# COMMAND ----------

# DBTITLE 1,Grouping & Aggregations (Advanced) [FIXED]
#8. Grouping & Aggregations (Advanced)
'''Source Files:
DF of logistics_source2: Provides hub_location and vehicle_type (Grouping Dimensions).
DF of logistics_shipment_detail_3000.json: Provides shipment_cost (Aggregation Metric).'''

read_df = spark.read.csv("/Volumes/workspace/default/logistics_vol/logistics_data/logistics_source2",inferSchema = True,header=True)
read_df = read_df.where("shipment_id not rlike '[a-zA-Z]'")
read_df = read_df.withColumnRenamed("vehicle_type","vehicle_ty")
group_df = read_df.join(shipments_df,how="inner",on="shipment_id")
display(group_df)

from pyspark.sql.functions import col
"""Scenario: The CFO wants a subtotal report at multiple levels:"""
#Total Cost by Hub.
total_cost_df = group_df.groupBy("hub_location").agg(sum("shipment_cost").alias("total_cost"))
display(total_cost_df)

#Total Cost by Hub AND Vehicle Type.
total_cost_df1= group_df.groupBy("hub_location","vehicle_type").agg(sum("shipment_cost").alias("total_cost"))
display(total_cost_df1)

#Grand Total.
grand_total_df = group_df.agg(round(sum("shipment_cost"),2).alias("grand_total"))
display(grand_total_df)

#Action: Use cube("hub_location", "vehicle_type") or rollup() to generate all these subtotals in a single query.
cube_df = group_df.cube("hub_location","vehicle_type").agg(sum("shipment_cost").alias("cube_cost")).orderBy("hub_location","cube_cost",ascending=[False,False])
display(cube_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Data Persistance (LOAD)-> Data Publishing & Consumption<br>
# MAGIC
# MAGIC Store the inner joined, lookup and enrichment, Schema Modeling, windowing, analytical functions, set operations, grouping and aggregation data into the delta tables.

# COMMAND ----------

#Inner join delta table
inner_df = staff_df.join(shipments_df,how="inner",on="shipment_id")
inner_write_df = inner_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.inner_data_table")

#lookup delta table
lookup_df = staff_df.join(master_df,how="semi",on="hub_location")
lookup_write_df = lookup_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.lookup_data_table")

#lookup enrichment delta table
lookup_enrich_df =  staff_df1.join(master_df.select("hub_location","latitude","longitude"),how="left",on='hub_location')
lookup_enrich_write_df = lookup_enrich_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.lookup_enrich_data_table")

#Schema modeling delta table
denormalized_df = staff_df.join(shipments_df,how="full",on="shipment_id")
denormalized_write_df = denormalized_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.schema_modeling_data_table")

#Windowing delta table
top3_df = top3_df.filter(col("role")== "Driver").withColumn("rn",row_number().over(Window.partitionBy("hub_location").orderBy(desc("shipment_cost")))).where("rn <= 3 ")
window_write_df = top3_df.write.format("delta").mode('overwrite').saveAsTable("workspace.default.windowing_data_table")

#Analytical functions delta table
lag_df1 = lag_df.withColumn("shipment_date",to_date("shipment_date","yyyy-MM-dd")).withColumn("previous_shipment_date",to_date("previous_shipment_date","yyyy-MM-dd")).withColumn("days_elapsed",datediff("shipment_date","previous_shipment_date"))
analytical_write_df = lag_df1.write.format("delta").mode('overwrite').saveAsTable("workspace.default.analytical_data_table")

#Set operations delta table
union_df = read_df1.unionByName(read_df2,allowMissingColumns=True)
union_write_df = union_df.write.format("delta").mode('overwrite').saveAsTable("workspace.default.set_operation_data_table")

#Grouping and aggregation delta table
total_cost_df = group_df.groupBy("hub_location").agg(sum("shipment_cost").alias("total_cost"))
group_write_df = total_cost_df.write.format("delta").mode('overwrite').saveAsTable("workspace.default.grouping_data_table")

cube_df = group_df.cube("hub_location","vehicle_type").agg(sum("shipment_cost").alias("cube_cost")).orderBy("hub_location","cube_cost",ascending=[False,False])
cube_write_df = cube_df.write.format("delta").mode('overwrite').saveAsTable("workspace.default.aggregation_data_table")



# COMMAND ----------

# MAGIC %md
# MAGIC ##7.Take the copy of the above notebook and try to write the equivalent SQL for which ever applicable.