# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 : Document the Notebook Using mark down %md
# MAGIC
# MAGIC - A good Title
# MAGIC - Description of the task
# MAGIC - Your name in some color
# MAGIC - Bring our Team photo from the given url "https://fpimages.withfloats.com/actual/6929d1ac956d0a744b5c9822.jpeg"
# MAGIC - Use headings, bold, italics appropriately.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #_WD36 Batch from Inceptez Technologies_
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### _Let's explore the magic commands_
# MAGIC We are going to explore the magic commands in databricks notebook.
# MAGIC Databricks tool is very important for the outside market. 
# MAGIC Let's get started....
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.how to change the text colour in notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC $${\color{pink}text-to-display}$$
# MAGIC $${\color{black}Black-color}$$
# MAGIC $${\color{red}Red}$$
# MAGIC $${\color{green}Green}$$
# MAGIC $${\color{blue}Blue}$$	

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.how to add our pictures using url in md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding team picture using url below
# MAGIC https://fpimages.withfloats.com/actual/6929d1ac956d0a744b5c9822.jpeg
# MAGIC ![](https://fpimages.withfloats.com/actual/6929d1ac956d0a744b5c9822.jpeg)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 2 : Create a volume namely usage_metrics using sql magic command %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.usage_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 3 : 
# MAGIC - Create a child notebook "2.Child_notebook_dataload" and write code to load data, Using the requests library, perform api call to pull data from "https://public.tableau.com/app/sample-data/mobile_os_usage.csv" into a python variable using the magic command %py
# MAGIC - Then write the data into the created volume "/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv" using the above variable.text using the magic command dbutils.fs.put("volume",variable.text,overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - **Step 1:** I have created a child notebook in this path ("/Workspace/Users/soundharjayan@gmail.com/databricks-code-repo/Databricks_workout_2025/USECASES_WORKOUT/2.Child_notebook_dataload" 
# MAGIC - **Step 2:** I written python code to get the data from URL
# MAGIC - **Step 3**  I written python code to put the data in Volumes/workspace/default/usage_metrics/mobile_os_usage.csv
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Task 4 : Call the notebook "2.Child_notebook_dataload" using the magic command %run

# COMMAND ----------

# MAGIC %run "/Workspace/Users/soundharjayan@gmail.com/databricks-code-repo/Databricks_workout_2025/USECASES_WORKOUT/2.Child_notebook_dataload"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Task 5 : List the file is created in the given volume or not and do the head of this file using fs magic command %fs

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/Volumes/workspace/default/usage_metrics" 

# COMMAND ----------

# MAGIC %fs head "dbfs:/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 6 : Create a pyspark dataframe df1 reading the data from the above file using pyspark magic command %python 

# COMMAND ----------

df1 = spark.read.csv("dbfs:/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv",header=True)
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 7 : Write the above dataframe df1 data into a databricks table called 'default.mobile_os_usage' using pyspark magic command %python

# COMMAND ----------


df2 = df1.withColumnRenamed(
    "Mobile Operating System", "Mobile_Operating_System"
).withColumnRenamed(
    "Percent of Usage", "Percent_of_Usage"
)
display(df2)
df2.write.mode("overwrite").format("delta").saveAsTable("mobile_os_usage")

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 8 : Write sql query to display the data loaded into the table 'default.mobile_os_usage' using the pyspark magic command %python

# COMMAND ----------

# Query the Delta table
df1 = spark.sql("SELECT * FROM default.mobile_os_usage")

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 9 : Create a python function to convert the given input to upper case

# COMMAND ----------


user_input = str(input("Enter your name: "))

def upper_case(x):
  return x.upper()

call_function = upper_case(user_input)
print(call_function)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Task 10 : Install pandas library using the pip python magic command %pip

# COMMAND ----------

# MAGIC %pip install pandas

# COMMAND ----------

import pandas as pd
print("Pandas version:", pd.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC # Task 11 : Import pandas, using pandas read_csv and display the output using the magic command %python

# COMMAND ----------


import pandas as pd

df = pd.read_csv("/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Task 12 : echo "Magic commands tasks completed" using the linux shell magic command %sh

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "Magic commands tasks completed"