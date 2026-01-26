# Databricks notebook source
# MAGIC %md
# MAGIC #Very Important Spark Learning - BY LEARNING this PROGRAM - WE BECOME A DATA ENGINEER (DATA CURATION DEVELOPER & DATA ANALYST)
# MAGIC Simply say- We are going to learn...
# MAGIC next level of SQL (Spark SQL) + Python Function based programming (Framework of Spark DSL) + Datawarehouse (Datalake+Lakehouse) -> Transformation & Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ##**1. Data Munging** - (Cleanup) Process of transforming and mapping data from Raw form into Tidy(usable) format with the intent of making it more appropriate and valuable for a variety of downstream purposes such for further Transformation/Enrichment, Egress/Outbound, analytics, Datascience/AI application & Reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ![Stage1](stage1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC **Passive Data Munging** - Data Discovery/Data Exploration/ EDA (Exploratory Data Analytics) (every layers ingestion/transformation/analytics/consumption) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns. <br>
# MAGIC
# MAGIC **Active Data Munging**
# MAGIC 1. Combining Data + Schema Evolution/Merging/Merging (Structuring)
# MAGIC 2. Validation, Cleansing, Scrubbing - Cleansing (removal of unwanted datasets), Scrubbing (convert raw to tidy)
# MAGIC 3. De Duplication and Levels of Standardization () of Data to make it in a usable format (Dataengineers/consumers)

# COMMAND ----------

# MAGIC %md
# MAGIC ###a. Passive Data Munging - 
# MAGIC - Visible - Data Discovery/Data Exploration/ EDA (Exploratory Data Analytics) (every layers ingestion/transformation/analytics/consumption) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Visibily/Manually opening the file we found couple of data patterns (Manual Exploratory Data Analysis)
# MAGIC - It is a Structured data with comma seperator (CSV)
# MAGIC - No Header, No comments, footer is there in the data
# MAGIC - Total columns are (seperator + 1)
# MAGIC - Data Quality 
# MAGIC - - Null columns and null records are there
# MAGIC - - duplicate rows & Duplicate id keys
# MAGIC - - format issues are there (age is not in number format eg. 7-7)
# MAGIC - - Uniformity issues (Artist, artist)
# MAGIC - - Number of columns are more or less than the expected
# MAGIC - eg. 4000011,Francis,McNamara,47,Therapist,NewYork & 4000014,Beth,Woodard,65
# MAGIC - - Identification of data type

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Programatically lets try to find couple of data patterns applying EDA - passively (without modifying, just for description).
# MAGIC

# COMMAND ----------

rawdf1=spark.read.csv("/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified",header=False,inferSchema=True).toDF("id","firstname","lastname","age","profession")
rawdf1.show(20,False)
display(rawdf1.take(20))
display(rawdf1.sample(.1))

# COMMAND ----------

#Important passive EDA structure functions we can use
rawdf1.printSchema()#I am realizing the id & age columns are having some non numerical values (supposed to be numeric)
print(rawdf1.columns)#I am understanding the column numbers/order and the column names
print(rawdf1.dtypes)#Realizing the datatype of every columns (even we can do programattic column & type identification for dynamic programming)
for i in rawdf1.dtypes:
    if i[1]=='string':
        print(i[0])

print(rawdf1.schema)#To identify the structure of the data in the StructType and StructField format

# COMMAND ----------

#Important passive EDA data functions we can use
#We identified few patterns on this data
#1. Deduplication of rows and given column(s)
#2. Null values ratio across all columns
#3. Distribution (Dense) of the data across all number columns
#4. Min, Max values
#5. StdDeviation - 
#6. Percentile - Distribution percentage from 0 to 100 in 4 quadrants of 25%
print("actual count of the data",rawdf1.count())
print("de-duplicated record (all columns) count",rawdf1.distinct().count())#de duplicate the entire columns of the given  dataframe
print("de-duplicated record (all columns) count",rawdf1.dropDuplicates().count())#de duplicate the entire columns of the given  dataframe
print("de-duplicated given cid column count",rawdf1.dropDuplicates(['id']).count())#de duplicate the entire columns of the given  dataframe
display(rawdf1.describe())
display(rawdf1.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ###**b. Active Data Munging**
# MAGIC 1. Combining Data + Schema Evolution/Merging (Structuring)
# MAGIC 2. Validation, Cleansing, Scrubbing - Cleansing (removal of unwanted datasets), Scrubbing (convert raw to tidy)
# MAGIC 3. De Duplication and Levels of Standardization () of Data to make it in a usable format (Dataengineers/consumers)

# COMMAND ----------

from pyspark.sql.session import SparkSession#15lakhs
spark=SparkSession.builder.appName("WD36 - ETL Pipeline - Bread & Butter").getOrCreate()#3 lakhs LOC by Databricks (for eg. display, delta, xml)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. **Structuring** - Combining Data + Schema Evolution/Merging 

# COMMAND ----------

# MAGIC %md
# MAGIC When do we go for Schema Evolution?<br>
# MAGIC Over the time, if no. of col are keep added by source<br>
# MAGIC Serialization  while writing+ mergeSchema while reading<br>
# MAGIC When do we go for Schema Merging?<br>
# MAGIC In a given day, If we get multiple files of related (not same) structure<br>
# MAGIC After reading in dataframe format -> unionByName + allowMissingColumns

# COMMAND ----------

#Extraction (Ingestion) methodologies
#1. Single file
struct1="id string, firstname string, lastname string, age string, profession string"
rawdf1=spark.read.schema(struct1).csv(path="/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified")
#2. Multiple files (with same or different names)
rawdf1=spark.read.schema(struct1).csv(path=["/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified","/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified"])
#3. Multiple files in multiple paths or sub paths
rawdf1=spark.read.schema(struct1).csv(path=["/Volumes/workspace/wd36schema/ingestion_volume/source/","/Volumes/workspace/wd36schema/ingestion_volume/staging/"],recursiveFileLookup=True,pathGlobFilter="custsm*")


# COMMAND ----------

#Active Data munging...
#When you go for Schema Merging/Melting and Schema Evolution?
#Schema Merging/Melting (unionByName,allowMissingColumns)- If we get multiple files
#Schema Evolution (orc/parquet with mergeSchema) - If no. of columns are keeps added by the source system
#when we know structure of the file already - schema merge/ schema not known earlier  - schema evolution

#COMBINING OR SCHEMA MERGING or SCHEMA MELTING of Data from different sources(Important interview question also as like schema evolution...)
#4. Multiple files with different structure in multiple paths or sub paths
strt1="id string, firstname string, lastname string, age string, profession string"
rawdf1=spark.read.schema(strt1).csv(path=["/Volumes/workspace/wd36schema/ingestion_volume/source/"],recursiveFileLookup=True,pathGlobFilter="custsmodified_N*")
strt2="id string, firstname string, age string, profession string,city string"
rawdf2=spark.read.schema(strt2).csv(path=["/Volumes/workspace/wd36schema/ingestion_volume/source/"],recursiveFileLookup=True,pathGlobFilter="custsmodified_T*")
display(rawdf1)
display(rawdf2)
rawdf_merged=rawdf1.union(rawdf2)#Use union only if the dataframes are having same columns in the same order with same datatype..
display(rawdf_merged)
#Expected right approach to follow
rawdf_merged=rawdf1.unionByName(rawdf2,allowMissingColumns=True)
display(rawdf_merged)

#Here, we are merging two files because both are in CSV format. If one file is CSV and the other file is in a different format, what should we do in this scenario? it will be handled automatically
#rawdf2.write.json("/Volumes/workspace/wd36schema/ingestion_volume/staging/csvjson")
rawdf3=spark.read.json("/Volumes/workspace/wd36schema/ingestion_volume/staging/csvjson")
rawdf_merged=rawdf_merged.unionByName(rawdf3,allowMissingColumns=True)
display(rawdf_merged)#Expected dataframe to proceed further munging on a single dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC Just for the simple learning of schema evolution & schema merging/melting<br>
# MAGIC Schema merging/melting<br>
# MAGIC 1,rajeshwari -day1(source1)<br>
# MAGIC 1,rajeshwari,30 -day1(source2)<br>
# MAGIC
# MAGIC Schema evolution<br>
# MAGIC 1,rajeshwari day1<br>
# MAGIC 1,rajeshwari,30 day2<br>
# MAGIC
# MAGIC Output is same in both cases...<br>
# MAGIC id,name,age<br>
# MAGIC 1,rajeshwari,null<br>
# MAGIC 1,rajeshwari,30<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Validation, Cleansing, Scrubbing - Cleansing (removal of unwanted datasets), Scrubbing (convert raw to tidy)

# COMMAND ----------

#Validation by doing cleansing
from pyspark.sql.types import StructType,StructField,StringType,ShortType,IntegerType
#print(rawdf1.schema)
struttype1=StructType([StructField('id', IntegerType(), True), StructField('firstname', StringType(), True), StructField('lastname', StringType(), True), StructField('age', ShortType(), True), StructField('profession', StringType(), True)])
#method1 - permissive with all rows with respective nulls
cleandf1=spark.read.schema(struttype1).csv(path="/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified",mode='permissive')
print("after keeping nulls on the wrong data format",cleandf1.count())#all rows count
display(cleandf1)#We are making nulls where ever data format mismatch is there (cutting down mud portition from potato)
#or
#method2 - drop malformed rows
cleandf1=spark.read.schema(struttype1).csv(path="/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified",mode='dropMalformed')
print("after cleaning wrong data (type mismatch, column number mismatch)",len(cleandf1.collect()))
display(cleandf1)#We are removing the entire row, where ever data format mismatch is there (throwing away the entire potato)
print(cleandf1.count())#count will return the original count of the raw data
print(len(cleandf1.collect()))#collect+len will return the dropmalformed count of the raw data

# COMMAND ----------

# MAGIC %md
# MAGIC #####Validation

# COMMAND ----------

#method3 best methodology of applying active data munging
#Validation by doing cleansing (not at the time of creating Dataframe, rather we will clean and scrub subsequently)...
struttype1=StructType([StructField('id', StringType(), True), StructField('firstname', StringType(), True), StructField('lastname', StringType(), True), StructField('age', StringType(), True), StructField('profession', StringType(), True)])
#method1 - permissive with all rows with respective nulls
rawdf1=spark.read.schema(struttype1).csv(path="/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified",mode='permissive')
print("allow all data showing the real values",rawdf1.count())#all rows count
display(rawdf1)#We are making nulls where ever data format mismatch is there (cutting down mud portition from potato)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Rejection Strategy

# COMMAND ----------

#Creating rejection dataset to send to our source system for future fix
from pyspark.sql.types import StructType,StructField,StringType,ShortType,IntegerType
struttype1=StructType([StructField('id', IntegerType(), True), StructField('firstname', StringType(), True), StructField('lastname', StringType(), True), StructField('age', ShortType(), True), StructField('profession', StringType(), True),StructField("corruptedrows",StringType())])
#method1 - permissive with all rows with respective nulls
cleandf1=spark.read.schema(struttype1).csv(path="/Volumes/workspace/wd36schema/ingestion_volume/source/custsmodified",mode='permissive',columnNameOfCorruptRecord="corruptedrows")
#Create a reject dataset
rejectdf1=cleandf1.where("corruptedrows is not null")
#display(rejectdf1)
rejectdf1.write.csv("/Volumes/workspace/wd36schema/ingestion_volume/staging/reject",mode="overwrite",header=True)
retaineddf1=cleandf1.where("corruptedrows is null")
print("Overall rows in the source data is ",len(cleandf1.collect()))
print("Rejected rows in the source data is ",len(rejectdf1.collect()))
print("Clean rows in the source data is ",len(retaineddf1.collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Cleansing 
# MAGIC na.drop()<br>
# MAGIC It is a process of cleaning/removing/deleting unwanted data
# MAGIC Eg. I am purchasing potato from a shop, I am cutting down the debris/rotten portion of it

# COMMAND ----------

#We already know how to do cleansing applying the strict Structure on method1 and method2
#Important na functions we can use to do cleansing
cleanseddf=rawdf1.na.drop(how="any")#This function will drop any column in a given row with null otherwise this function returns rows with no null columns - In a scenario of if the source send the Datascience Model features (we shouldn't have any one feature with null value, hence we can use this function)
print("any one row in the raw df with age null")
display(rawdf1.where("age is null"))
print("any one row in the cleansed df with age null")
display(cleanseddf.where("age is null"))#any one column contains null will be cleaned
cleanseddf=rawdf1.na.drop(how="any",subset=["id","age"])#If we need CDE without nulls (Critical Data Elements/Significant columns) columns
print("any one row in the cleansed df with id or age null")
display(cleanseddf)
cleanseddf=rawdf1.na.drop(how="all",subset=["firstname","lastname"])#4000004,Gretchen,,66,
print("any one row in the cleansed df with firstname and lastname is null")
print("Total rows without firstname and lastname with null values",len(cleanseddf.collect()))
display(cleanseddf)#We are taking this DF further for munging..

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scrubbing 
# MAGIC na.fill() & na.replace()<br>
# MAGIC It is a process of polishing/fine tuning/scrubbing/meaningful conversion the data in a usable format
# MAGIC Eg. I am purchasing potato from a shop, I am scrubbing/washing mud/sand portion of it

# COMMAND ----------

scrubbeddf1=cleanseddf.na.fill('not provided',subset=["lastname","profession"])#fill will help us replace nulls with some value
display(scrubbeddf1)
find_replace_values_dict1={'Pilot':'Captain','Actor':'Celeberity'}
find_replace_values_dict2={'not provided':'NA'}
scrubbeddf2=scrubbeddf1.na.replace(find_replace_values_dict1,subset=["profession"])#fill function is helping us find and replace the values
scrubbeddf3=scrubbeddf2.na.replace(find_replace_values_dict2,subset=["lastname"])
display(scrubbeddf3)

# COMMAND ----------

# MAGIC %md
# MAGIC #####DeDuplication
# MAGIC Removal of duplicate rows/columns based on a priority or non priority
# MAGIC distinct & dropDuplicates
# MAGIC

# COMMAND ----------

display(scrubbeddf3.where("id in ('4000001')"))#before row level dedup
dedupdf1=scrubbeddf3.distinct()#It will remove the row level duplicates
display(dedupdf1.where("id in ('4000001')"))

print("non prioritized deduplication, just remove the duplicates retaining only the first row")
display(dedupdf1.coalesce(1).where("id in ('4000003')"))#before col level dedup
dedupdf2=dedupdf1.coalesce(1).dropDuplicates(subset=["id"])#It will remove the column level duplicates (retaining the first row in the dataframe)
display(dedupdf2.where("id in ('4000003')"))
print("prioritized deduplication based on age")
display(dedupdf1.coalesce(1).where("id in ('4000003')"))#before col level dedup
#dedupdf1.coalesce(1).where("id in ('4000003')").orderBy(["id","age"],ascending=[True,False]).show(3)
dedupdf2=dedupdf1.coalesce(1).orderBy(["id","age"],ascending=[True,False]).dropDuplicates(subset=["id"])#It will remove the column level duplicates (retaining the first row in the dataframe)
display(dedupdf2.where("id in ('4000003')"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Standardization and Replacement / Deletion of Data to make it in a usable format

# COMMAND ----------

# MAGIC %md
# MAGIC #####Standardization - 
# MAGIC Making the data more standard by adding/removing/reordering columns as per the expected standard, unifying into expected format, converting the type as expected etc.,

# COMMAND ----------

# MAGIC %md
# MAGIC ######Standardization1 - Column Enrichment (Addition of columns)

# COMMAND ----------

from pyspark.sql.functions import lit,initcap,col
#withColumn("stringcolumnname to add in the df",lit('hardcoded')/initcap(col("colname")))
standarddf1=dedupdf2.withColumn("sourcesystem",lit("Retail"))#SparkSQL - DSL(FBP)
display(standarddf1.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ######Standardization2 - Column Uniformity

# COMMAND ----------

from pyspark.sql.functions import upper
#Basic Exploration/analysis of the profession column for identifying uniformity challenges
#standarddf1.createOrReplaceTempView("sqlview")
#display(spark.sql("select profession,count(*) from sqlview group by profession order by profession"))#SQL
#display(standarddf1.groupBy("profession").count())#DSL
#Standardization2 - column uniformity
standarddf2=standarddf1.withColumn("profession",initcap("profession"))#inicap or any other string function with columnOr name can accept either column or string type provided if the string is a column name for eg. profession/age/sourcesystem.
display(standarddf2.limit(20))
#display(standarddf2.groupBy("profession").count())#DSL

# COMMAND ----------

# MAGIC %md
# MAGIC ######Standardization3 - Format Standardization

# COMMAND ----------

#Did analysis to understand the format issues in our id and age columns
#standarddf2.where("id like 't%'").show()
standarddf2.where("id rlike '[a-zA-Z]'").show()#rlike is regular expression like function that help us identify any string data in our DF column
standarddf2.where("age rlike '[^0-9]'").show()#checking for any non number values in age column
#standarddf3=standarddf2.withColumn

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,replace
#Let's apply scrubbing features to our id column to replace ten with 10 (or we can think of using GenAI here)
replaceval={'one':'1','two':'2','three':'3','four':'4','five':'5','six':'6','seven':'7','eight':'8','nine':'9','ten':'10'}
standarddf3=standarddf2.na.replace(replaceval,["id"])
#standarddf3=standarddf2.withColumn("id",replace("id",lit('ten'),"10"))
standarddf3=standarddf3.withColumn("age",regexp_replace("age",'-',""))
display(standarddf3)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Standardization4 - Data Type Standardization

# COMMAND ----------

standarddf3.printSchema()#Still id and age are string type, though it contains int data
#standarddf4=standarddf3.withColumn("id","id".cast("long"))#this wil not work
standarddf4=standarddf3.withColumn("id",standarddf3.id.cast("long"))
standarddf4=standarddf3.withColumn("id",standarddf3["id"].cast("long"))
standarddf4=standarddf3.withColumn("id",col("id").cast("long"))
standarddf4=standarddf4.withColumn("age",col("age").cast("short"))
standarddf4.printSchema()
display(standarddf4)


# COMMAND ----------

# MAGIC %md
# MAGIC ######Standardization5 - Naming Standardization

# COMMAND ----------

standarddf5=standarddf4.withColumnRenamed("id","custid")
standarddf5=standarddf4.withColumnsRenamed({"id":"custid","sourcesystem":"srcsystem"})
display(standarddf5)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Standardization6 - Reorder Standadization

# COMMAND ----------

standarddf6=standarddf5.select("custid", "age", "firstname","lastname","profession","srcsystem")
#display(standarddf6)
mungeddf=standarddf6
display(mungeddf.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting Data Enrichment or before sharing the data to the consumer, we have to do EDA/Exploration/Validation

# COMMAND ----------

mungeddf.printSchema()
display(mungeddf.take(20))
display("total rows",len(mungeddf.collect()))
display(mungeddf.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ##**2. Data Enrichment** - Detailing of data
# MAGIC Makes your data rich and detailed <br>
# MAGIC a. Add (withColumn,select,selectExpr), Derive (withColumn,select,selectExpr), Remove(drop,select,selectExpr), Rename (withColumnRenamed,select,selectExpr), Modify/replace (withColumn,select,selectExpr) - very important spark sql functions <br>
# MAGIC b. split, merge/Concat <br>
# MAGIC c. Type Casting, reformat & Schema Migration <br>

# COMMAND ----------

# MAGIC %md
# MAGIC ![stage2](stage2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #####a. Add (), Derive (), Rename (), Modify/replace (), Remove/Eliminate () - very important spark sql DF functions

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding of columns
# MAGIC Lets add datadt (date of the data orgniated from the source for eg. provided in the filename in a format of yy/dd/MM) and loaddt (date when we are loading the data into our system)

# COMMAND ----------

derived_datadt='25/30/12'
print(f"hello '{derived_datadt}'")

# COMMAND ----------

from pyspark.sql.functions import lit,current_date#already imported, not needed here
original_filename='custsmodified_25/30/12.csv'#We are deriving this date from the filename provided by the source custsmodified_25/30/12.csv
derived_datadt=original_filename.split('_')[1].split('.')[0]
#derived_datadt='25/30/12'#We are deriving this date from the filename provided by the source custsmodified_25/30/12.csv
enrichdf1=mungeddf.withColumn("datadt",lit('25/30/12')).withColumn("loaddt",current_date())
enrichdf1.printSchema()
#or
enrichdf1=mungeddf.withColumns({"datadt":lit('25/30/12'),"loaddt":current_date()})
enrichdf1.printSchema()
#or
enrichdf1=mungeddf.select("*",lit(derived_datadt).alias('datadt'),current_date().alias('loaddt'))#DSLs (FBP function)
#or
enrichdf1=mungeddf.selectExpr("*","'25/30/12' as datadt","current_date() as loaddt")#DSL(select) + SQL expression
enrichdf1=mungeddf.selectExpr("*",f"'{derived_datadt}' as datadt","current_date() as loaddt")#DSL(select) + SQL expression
enrichdf1.printSchema()
display(enrichdf1)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Deriving of columns

# COMMAND ----------

from pyspark.sql.functions import *
enrichdf2=enrichdf1.withColumn("professionflag",substring("profession",1,1))
#or
enrichdf2=enrichdf1.select("*",substring("profession",1,1).alias("professionflag"))
#or
enrichdf2=enrichdf1.selectExpr("*","substr(profession,1,1) as professionflag")
display(enrichdf2.take(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ######Renaming of columns

# COMMAND ----------

#Can we use withColumn to rename? not directly, its costly
enrichdf3=enrichdf2.withColumn("sourcename",col("srcsystem"))
enrichdf3=enrichdf3.drop("srcsystem").select("custid","age","firstname","lastname","profession","sourcename","datadt","loaddt","professionflag")
#or
enrichdf3=enrichdf2.select("custid","age","firstname","lastname","profession",col("srcsystem").alias("sourcename"),"datadt","loaddt","professionflag")#costly too, since we have to choose all columns in the select
#or
#enrichdf2.printSchema()
enrichdf3=enrichdf2.selectExpr("custid","age","firstname","lastname","profession","srcsystem as sourcename","datadt","loaddt","professionflag")#costly too, since we have to choose all columns in the select
#or
enrichdf3=enrichdf2.withColumnRenamed("srcsystem","sourcename")#Best function to rename the column(s)
#or
enrichdf3=enrichdf2.withColumnsRenamed({"srcsystem":"sourcename","professionflag":"profflag"})
display(enrichdf3.take(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ######Modify/replace (withColumn, select/selectExpr)

# COMMAND ----------

enrichdf4=enrichdf3.withColumn("profession",col("sourcename"))#This will replace the profession with sourcename
#or
enrichdf4=enrichdf3.withColumn("profession",concat("profession",lit('-'),"profflag"))#This will modify/enrich the profession column with sourcename
#or using select/selectExpr
enrichdf4=enrichdf3.select("custid","age","firstname","lastname",concat("profession",lit('-'),"profflag").alias("profession"),"sourcename","datadt","loaddt","profflag")
#or use selectExpr
enrichdf4=enrichdf3.selectExpr("custid","age","firstname","lastname","concat(profession,'-',profflag) as profession","sourcename","datadt","loaddt","profflag")
display(enrichdf4.take(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ######Remove/Eliminate (drop,select,selectExpr) 

# COMMAND ----------

#enrichdf4=enrichdf3.withColumn("profession",col("sourcename"))#Cannot be used
#or using select/selectExpr (yes, but costly)
enrichdf5=enrichdf4.select("custid","age","firstname","lastname","profession","sourcename","datadt","loaddt")
#or use selectExpr (yes, but costly)
enrichdf5=enrichdf4.selectExpr("custid","age","firstname","lastname","profession","sourcename","datadt","loaddt")
#or 
enrichdf5=enrichdf4.drop("profflag")#right function to use from dropping
display(enrichdf5.take(20))

# COMMAND ----------

#how to write a python program to append a variable value to another variable and use it inside the selectExpr
name='irfan'
sqlexpression=f"'{name}' as owner"
print(sqlexpression)
mungeddf.selectExpr("*",sqlexpression).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ######Conclusion/Best practices of using different column enrichment functions
# MAGIC 1. **select** is good to use if we want to perform - 
# MAGIC Good for ordering/reordering of columns, only renaming column (not good), only reformatting/deriving a column (not good), **for all of these operation in a single iteration** such renaming, reordering, reformatting,deriving, dropping etc., (best to use)
# MAGIC 2. **selectExpr** is good to use if we want to perform - Same as select by using ISO/ANSI SQL functionality (if we are not familiar in DSL FBP) **for all of these operation in a single iteration**
# MAGIC 3. **withColumn** is good to use if we want to perform - 
# MAGIC **for adding/deriving/modifying/replacing in a single iteration**
# MAGIC Adding/Deriving column(s) in the last (Good), Modifying/replacing (Good), Renaming (not good), Dropping(not possible), reordering(not good)
# MAGIC 4. **withColumnRenamed** is good to use if we want to perform - only for renaming column (Good)
# MAGIC 5. **drop** is good to use if we want to perform - only dropping of columns in the given position (Good)

# COMMAND ----------

# MAGIC %md
# MAGIC #####b. Splitting & Merging/Melting of Columns

# COMMAND ----------

#Splitting of column
splitdf=enrichdf5.withColumn("profflag",split("profession",'-'))
splitdf=splitdf.withColumn("profession",col("profflag")[0])
#splitdf=splitdf.withColumn("profflag",col("profflag")[1])
#or
splitdf=splitdf.withColumn("shortprof",upper(substring(col("profession"),1,3))).drop("profflag")
#Merging of column
mergeddf=splitdf.select(col("custid"),"age",concat_ws(" ",col("firstname"),col("lastname")).alias("fullname"),"profession","sourcename","datadt","loaddt","shortprof")#usage of select will help us avoid chaining of withColumn,drop,select
display(mergeddf.limit(10))

# COMMAND ----------

mergeddf.printSchema()
#unconsious and incompetant(day1)
#consious and incompetant(month3)-current state
#consious and competant(further month)-another few month state
#unconsious and competant(further month)-near end of every stage (pyspark+databricks(sql/python/dwh))

# COMMAND ----------

# MAGIC %md
# MAGIC #####c. Formatting & Typecasting

# COMMAND ----------

formatteddf=mergeddf.withColumn("datadt",to_date(col("datadt"),'yy/dd/MM'))#25/30/12 -> 2025-12-30
formatteddf.printSchema()
display(formatteddf.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Data Customization - Application of Tailored Business specific Rules <br>
# MAGIC a. User Defined Functions <br>
# MAGIC b. Building of Frameworks & Reusable Functions (We will learn very next)

# COMMAND ----------

# MAGIC %md
# MAGIC ![Stage3](stage3.png)

# COMMAND ----------

#formatteddf2=formatteddf.withColumn("sourcename",upper("sourcename"))
#formatteddf2.show(2)
#Caveat - If there is no upper() function is available already in spark dsl/sql, we can either search for some functions in the online opensource platform or we have to create one (custom functions)
#from org.apache.sql.functions import upperodd

def upperodd(colname_containsvalue):
    convertedcolvalue=colname_containsvalue.upper()
    return convertedcolvalue
print(upperodd("irfan"))


# COMMAND ----------

formatteddf2=rawdf1.withColumn("firstname",upper(col("firstname")))#we can't run python function as it is
formatteddf2.explain()
#display(formatteddf2.take(10))#prefer
from pyspark.sql.functions import udf
udfupper=udf(upperodd)#promote normal python function to spark ready udf
formatteddf2=rawdf1.withColumn("firstname",udfupper(col("firstname")))#if udf is inevitable, then we create despite of performance bottleneck
formatteddf2.explain()
#display(formatteddf2.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create Python Custom Function with complex logics

# COMMAND ----------

#Calculating age category from the given age of the customer
def pythonAgeCat(dfcol):
    if dfcol is None:
        return "Unknown"
    elif dfcol<=10:
        return "child"
    elif dfcol>10 and dfcol<=18:
        return "teenager"
    elif dfcol>18 and dfcol<=30:
        return "young"
    elif dfcol>30 and dfcol<=50:
        return "middleaged"
    else:
        return "senior"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Import udf library, Convert to UDF, Apply in the DF

# COMMAND ----------

from pyspark.sql.functions import udf
sparkudfageCat=udf(pythonAgeCat)
customdf=formatteddf.withColumn("agecat",sparkudfageCat("age"))
display(customdf.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Data Curation/Processing - Applying different levels of business logics, transformation, filtering, grouping, aggregation and limits applying different transformation functions
# MAGIC 1. Select, Filter
# MAGIC 2. Derive flags & Columns
# MAGIC 3. Format
# MAGIC 4. Group & Aggregate
# MAGIC 5. Limit

# COMMAND ----------

# MAGIC %md
# MAGIC ![Curation](stage4.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.Select, Filter

# COMMAND ----------

#select, functions, case, literal ,from,where,group by, having, order by, limit...
#Select few columns by filtering few rows
selectdf=customdf.select("custid","age","agecat","profession","agecat")
filterdf=selectdf.filter("age is not null")#either where or filter both are same (we can write a normal sql where clause)..
#or
filterdf=selectdf.where("agecat in ('senior','teenager')")
display(filterdf.take(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. how to create pipelines using different data processing techniques by connecting with different sources/targets
# MAGIC 2. how to Standardize/Modernization/Industrializing the code and how create/consume generic/reusable functions & frameworks
# MAGIC 3. Testing (Unit, Peer Review, SIT/Integration, Regression, User Acceptance Testing), Masking engine,
# MAGIC 4. Reusable transformation(munge_data, optimize_performance),
# MAGIC 5. Quality suite/Data Profiling/Audit engine (Reconcilation) (Audit framework), Data/process Observability
# MAGIC
# MAGIC 6. how terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment ...
# MAGIC 7. performance tuning
# MAGIC 8. Deploying spark applications in Cloud & other Distributions like Hortonworks/Cloudera/Databricks
# MAGIC 9. Creating cloud pipelines using spark SQL programs & Cloud native tools
# MAGIC
# MAGIC What is the importance of learning this program or How this can address interview questions..?
# MAGIC VERY VERY IMPORTANT PROGRAM IN TERMS OF EXPLAINING/SOLVING PROBLEMS GIVEN IN INTERVIEW ,
# MAGIC WITH THIS ONE PROGRAM YOU CAN COVER ALMOST ALL DATAENGINEERING FEATURES
# MAGIC Tell me about the common transformations you performed,
# MAGIC tell me your daily roles in DE,
# MAGIC tell me some business logics you have writtened recently
# MAGIC How do you write an entire spark application,
# MAGIC levels/stages of DE pipelines or
# MAGIC have you created DE pipelines what are the transformations applied,
# MAGIC how many you have created or are you using existing framework or you created some framework?
# MAGIC
# MAGIC '''
# MAGIC TRANSFORMATION & ANALYTICAL TECHNIQUES
# MAGIC Starting point - (Data Governance (security) - Tagging, categorization, classification, masking/filteration)
# MAGIC 1. Data Munging - Process of transforming and mapping data from Raw form into Tidy(usable) format with the
# MAGIC intent of making it more appropriate and valuable for a variety of downstream purposes such for
# MAGIC further Transformation/Enrichment, Egress/Outbound, analytics, model application & Reporting
# MAGIC a. Passive - Data Discovery EDA (Exploratory Data Analytics)
# MAGIC (every layers ingestion/transformation/analytics/consumption) -
# MAGIC Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
# MAGIC b. Active - Combining Data + Schema Evolution/Merging (Structuring)
# MAGIC c. Validation, Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
# MAGIC Preprocessing, Preparation
# MAGIC Cleansing (removal of unwanted datasets eg. na.drop),
# MAGIC Scrubbing (convert of raw to tidy na.fill or na.replace),
# MAGIC d. Standardization, De Duplication and Replacement & Deletion of Data to make it in a usable format (Dataengineers/consumers)
# MAGIC
# MAGIC 2. Data Enrichment - Makes your data rich and detailed
# MAGIC a. Add, Remove, Rename, Modify/replace
# MAGIC b. split, merge/Concat
# MAGIC c. Type Casting, format & Schema Migration
# MAGIC
# MAGIC 3. Data Customization & Processing - Application of Tailored Business specific Rules
# MAGIC a. User Defined Functions
# MAGIC b. Building of Frameworks & Reusable Functions
# MAGIC
# MAGIC 4. Data Curation
# MAGIC a. Curation/Transformation
# MAGIC b. Analysis/Analytics & Summarization -> filter, transformation, Grouping, Aggregation/Summarization
# MAGIC
# MAGIC 5. Data Wrangling - Gathering, Enriching and Transfomation of pre processed data into usable data
# MAGIC a. Lookup/Reference
# MAGIC b. Enrichment
# MAGIC c. Joins
# MAGIC d. Sorting
# MAGIC e. Windowing, Statistical & Analytical processing
# MAGIC f. Set Operation
# MAGIC
# MAGIC 6. Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
# MAGIC a. Discovery,
# MAGIC b. Outbound/Egress,
# MAGIC c. Reports/exports
# MAGIC d. Schema migration
# MAGIC '''

# COMMAND ----------

# MAGIC %md
# MAGIC ##Capture all list of functionalities & functions we used in this entire notebook