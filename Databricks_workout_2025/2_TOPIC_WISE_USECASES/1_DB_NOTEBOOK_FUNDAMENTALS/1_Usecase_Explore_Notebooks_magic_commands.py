# Databricks notebook source
# MAGIC %md
# MAGIC ######Task1: Document the Notebook Using mark down %md <br>
# MAGIC A good Title <br>
# MAGIC Description of the task <br>
# MAGIC Your name in some color <br>
# MAGIC Bring our Team photo from the given url "https://fpimages.withfloats.com/actual/6929d1ac956d0a744b5c9822.jpeg" <br>
# MAGIC Use headings, bold, italics appropriately. <br>
# MAGIC
# MAGIC Task2: Create a volume namely usage_metrics using sql magic command %sql
# MAGIC
# MAGIC Task3: 
# MAGIC Create a child notebook "4_child_nb_dataload" and write code to load data, Using the requests library, perform api call to pull data from "https://public.tableau.com/app/sample-data/mobile_os_usage.csv" into a python variable using the magic command %py and write the data into the created volume "/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv" using the above variable.text using the magic command dbutils.fs.put("volume",variable.text,overwrite=True)
# MAGIC
# MAGIC Task4: Call the notebook 4_child_nb_dataload using the magic command %run
# MAGIC
# MAGIC Task5: list the file is created in the given volume or not and do the head of this file using fs magic command %fs 
# MAGIC
# MAGIC Task6: Create a pyspark dataframe df1 reading the data from the above file using pyspark magic command %python
# MAGIC
# MAGIC Task7: Write the above dataframe df1 data into a databricks table called 'default.mobile_os_usage' using pyspark magic command %python
# MAGIC
# MAGIC Task8: Write sql query to display the data loaded into the table 'default.mobile_os_usage' using the pyspark magic command %python 
# MAGIC
# MAGIC Task9: Create a python function to convert the given input to upper case
# MAGIC
# MAGIC Task10: Install pandas library using the pip python magic command %pip
# MAGIC
# MAGIC Task11: Import pandas, using pandas read_csv and display the output using the magic command %python
# MAGIC
# MAGIC Task12: echo "Magic commands tasks completed" using the linux shell magic command %sh 