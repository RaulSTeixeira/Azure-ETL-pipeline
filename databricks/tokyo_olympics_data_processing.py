# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import split, col

# COMMAND ----------

#establish connection with ADLS

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "xxxxxxxxx",
"fs.azure.account.oauth2.client.secret": 'xxxxxxxxx,
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/xxxxxxxxxxxx/oauth2/token"}

dbutils.fs.mount(
source = "abfss://CONTAINER@STORAGE_ACCOUNT.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# COMMAND ----------

# List files in mounting point
dbutils.fs.ls("/mnt/tokyoolympic/")

# COMMAND ----------

#check if there is a spark session
spark


# COMMAND ----------

#read file and create dataframe
athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw-data/Athletes.csv")

# COMMAND ----------

#other sintax
athletes = spark.read.load("dbfs:/mnt/tokyoolympic/raw-data/Athletes.csv",
    format='csv',
    header=True
)
display(athletes.limit(10))

# COMMAND ----------

#read all dataframes
files = ["Athletes", "Coaches", "EntriesGender", "Medals", "Teams"]

for a in files:
    globals()[a] = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f'dbfs:/mnt/tokyoolympic/raw-data/{a}.csv')

# COMMAND ----------

#show a snipet of each dataframe
for a in files:
    print(a)
    print (globals()[a].show())

# COMMAND ----------

#show schemas of each dataframe
for a in files:
    print(a)
    print (globals()[a].printSchema())

# COMMAND ----------

#Order medals table by descending order
top_gold_medal_countries = Medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------

#show portuguese medals
Medals.filter(Medals.Team_Country == "Portugal").show()

# COMMAND ----------

# Create a new table for the Athetes that separates the first and last name

# Create the new FirstName and LastName fields
Athletes_transformed = Athletes.withColumn("FirstName", split(col("PersonName"), " ").getItem(1)).withColumn("LastName", split(col("PersonName"), " ").getItem(0))

# Remove the CustomerName field
Athletes_transformed = Athletes_transformed.drop("PersonName")

display(Athletes_transformed.limit(5))
     

# COMMAND ----------

#Run querys using SQL to find all the athletes from Portugal

Athletes_transformed.createOrReplaceTempView("athletestransformed")
spark_df = spark.sql("SELECT * FROM athletestransformed WHERE Country = 'Portugal'")
display(spark_df)

# COMMAND ----------

# Use SQL directly with the 'magic' %sql
# Count Portuguese athletes
%sql
SELECT COUNT(*) AS athlete_count
FROM athletestransformed
WHERE Country = 'Portugal'

# COMMAND ----------

#Use SQL directly with the 'magic' %sql
#Count portuguese athletes last name

%sql    
SELECT 
  LastName,
  COUNT (LastName) AS name_count
FROM athletestransformed
WHERE Country = 'Portugal'
GROUP BY LastName
ORDER BY name_count DESC


# COMMAND ----------

Athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed-data/athletes")

