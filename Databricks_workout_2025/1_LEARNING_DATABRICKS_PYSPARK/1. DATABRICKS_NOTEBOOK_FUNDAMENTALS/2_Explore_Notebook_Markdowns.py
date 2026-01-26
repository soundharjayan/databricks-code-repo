# Databricks notebook source
# MAGIC %md
# MAGIC ![Copyright!!](https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png)

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Basics of Python Programing

# COMMAND ----------

# MAGIC %md
# MAGIC ##A. Python is an indent based programming language
# MAGIC Why Python uses indend based programing ->
# MAGIC 1. Managing the program more efficiently
# MAGIC 2. Better Readablility of the code
# MAGIC 3. For creating the hierarchy of programming.
# MAGIC 4. By default 4 spaces we will give for indends, but more/less spaces or tabs also can be used...

# COMMAND ----------

# MAGIC %md
# MAGIC ###How many space for intending

# COMMAND ----------

if True:
    print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Multiple intents

# COMMAND ----------

aspirants_list=['Jeeva','Bharathi','Vaanmathy','Nag']
for aspirants in aspirants_list:
    print("good afternoon ",aspirants)
print("good after all aspirants")

# COMMAND ----------

# MAGIC %md
# MAGIC ##B. This is a commented line in Python

# COMMAND ----------

#1. Single line comment - use # in the starting
'''2.Multi line comment''' 
# - use ''' comment ''' or """ comment """

# COMMAND ----------

# MAGIC %md
# MAGIC #Main Heading1 using # <br> How to do some markdowns design <br> using the magic command

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Heading2 - prefix with "2#"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Heading3 - prefix with "3#"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sub Heading1 - prefix with "max 4#"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sub Heading2 - prefix with "max 5#"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Sub Heading3 - prefix with "max 6#"

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Sub Heading3 - prefix with "max 6#"

# COMMAND ----------

# MAGIC %md
# MAGIC ######Lets learn about bold
# MAGIC 1. <b> Bold </b> - using html tagging <b></b>
# MAGIC 2. **Bold** - prefixed and suffixed with **

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Lets learn about Italics
# MAGIC *Italics* - prefixed and suffixed with *

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Lets learn about bullet points
# MAGIC
# MAGIC - bullet points - prefix with -
# MAGIC - bullet points - prefix with -

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Lets learn about Color codes
# MAGIC $${\color{pink}text-to-display}$$
# MAGIC $${\color{black}Black-color}$$
# MAGIC $${\color{red}Red}$$
# MAGIC $${\color{green}Green}$$
# MAGIC $${\color{blue}Blue}$$	

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Lets learn about Embedding urls
# MAGIC [click here <-](https://www.google.com/search?q=whether+databricks+uses+hive+in+the+behind%3F&sca_esv=d340eac8d7c27e5b&sxsrf=AE3TifM0tbhMSJ32VMGLkFYoRjocGCu6jw%3A1765160969262&ei=CTg2abXhD4PD4-EPsuGjiAo&ved=0ahUKEwj1ia2E-ayRAxWD4TgGHbLwCKEQ4dUDCBE&uact=5&oq=whether+databricks+uses+hive+in+the+behind%3F&gs_lp=Egxnd3Mtd2l6LXNlcnAiK3doZXRoZXIgZGF0YWJyaWNrcyB1c2VzIGhpdmUgaW4gdGhlIGJlaGluZD8yBRAhGKABMgUQIRigATIFECEYoAEyBRAhGKABSM1VUABY7VFwBXgBkAEAmAGRAaABzyKqAQQwLjM2uAEDyAEA-AEBmAIkoAL0HsICBxAAGIAEGA3CAgYQABgHGB7CAggQABgHGAgYHsICCBAAGAgYDRgewgILEAAYgAQYhgMYigXCAgYQABgNGB7CAggQABiABBiiBMICBRAhGJ8FwgIEECEYFcICBxAhGKABGAqYAwCSBwQ1LjMxoAeJ8AGyBwQwLjMxuAfjHsIHBjIuMzIuMsgHRIAIAA&sclient=gws-wiz-serp)
# MAGIC Click here for [Inceptez Webpage](https://www.inceptez.in/)

# COMMAND ----------

# MAGIC %md
# MAGIC ######To learn markdowns more in detail
# MAGIC Click here [Microsoft markdown cheatsheet](https://docs.databricks.com/aws/en/notebooks/notebook-media)

# COMMAND ----------

# MAGIC %md
# MAGIC | col1 | col2 |
# MAGIC |------|------|
# MAGIC | a | b |
# MAGIC | c | d |