# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import split, col
import pandas as pd

# COMMAND ----------

# Establish connection with ADLS

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "XXXXXXXXX",
"fs.azure.account.oauth2.client.secret": 'XXXXXXXXX',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/XXXXXXXXX/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympics-container@tokyoolympicsraul.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)

# COMMAND ----------

# List files in mounting point
dbutils.fs.ls("/mnt/tokyoolympic/")

# COMMAND ----------

# Check if there is a spark session and the details
spark


# COMMAND ----------

# Read a csv file from the mounting point and create a dataframe
athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/tokyoolympic/raw-data/Athletes.csv")

# COMMAND ----------

# Another sintax to read a csv file from the mounting point and create a dataframe
athletes = spark.read.load("dbfs:/mnt/tokyoolympic/raw-data/Athletes.csv",
    format='csv',
    header=True
)
display(athletes.limit(10))

# COMMAND ----------

# Create a function to read and convert all csv files in a specific folder, the function also outputs the structure and a snipet of each dataframe
def read_csv_files(files):
    for a in files:
        globals()[a] = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(f'dbfs:/mnt/tokyoolympic/raw-data/{a}.csv')
        print(a)
        globals()[a].printSchema()
        globals()[a].show()
    return globals()[a]

files_name = ["Athletes", "Coaches", "EntriesGender", "Medals", "Teams"]
read_csv_files(files_name)


# COMMAND ----------

# Order medals table by descending order and display first 10 countries
Medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").limit(10).display()

# COMMAND ----------

# Show only portuguese medals
Medals.filter(Medals.Team_Country == "Portugal").show()

# COMMAND ----------

# Create a new table for the Athetes that separates the first and last name

# Create the new FirstName and LastName fields
Athletes_transformed = Athletes.withColumn("FirstName", split(col("PersonName"), " ").getItem(1)).withColumn("LastName", split(col("PersonName"), " ").getItem(0))

# Remove the PersonName field
Athletes_transformed = Athletes_transformed.drop("PersonName")

display(Athletes_transformed.limit(5))
     

# COMMAND ----------

# Run querys using SQL to find all the athletes from Portugal

# Create a temporary view so it can be used by spark SQL library
Athletes_transformed.createOrReplaceTempView("athletestransformed")

spark_df = spark.sql("SELECT * FROM athletestransformed WHERE Country = 'Portugal'")
display(spark_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use SQL directly with the 'magic' %sql
# MAGIC -- Dont forget to create a temp view (already done in previous block)
# MAGIC -- Count Portuguese athletes
# MAGIC SELECT COUNT(*) AS Number_Portuguese_Athletes
# MAGIC FROM athletestransformed
# MAGIC Where Country = 'Portugal'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Use SQL directly with the 'magic' %sql
# MAGIC -- Count portuguese athletes last name
# MAGIC     
# MAGIC SELECT 
# MAGIC   LastName,
# MAGIC   COUNT (LastName) AS name_count
# MAGIC FROM athletestransformed
# MAGIC WHERE Country = 'Portugal'
# MAGIC GROUP BY LastName
# MAGIC ORDER BY name_count DESC
# MAGIC

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = EntriesGender.withColumn(
    'Avg_Female', EntriesGender['Female'] / EntriesGender['Total']
).withColumn(
    'Avg_Male', EntriesGender['Male'] / EntriesGender['Total']
)
average_entries_by_gender.show()

# COMMAND ----------

# Convert PySpark DataFrame to a pandas DataFrame and do the previous calculation

EntriesGender_pandas_df = EntriesGender.select("*").toPandas()

EntriesGender_pandas_df['Avg_Female'] = EntriesGender_pandas_df['Female'] / EntriesGender_pandas_df['Total']
EntriesGender_pandas_df['Avg_Male'] = 1 - EntriesGender_pandas_df['Avg_Female']

EntriesGender_pandas_df

# COMMAND ----------

# Some statistics on gender distribution

total_female = EntriesGender_pandas_df['Female'].sum()
total_male = EntriesGender_pandas_df['Male'].sum()

total = total_female + total_male

print('total_female',total_female, round(total_female/total,2) )
print('total_male', total_male, round(total_male/total,2))


# COMMAND ----------

# Write modified dataframes back to Azure Data Lake
# We can define the number of partitons per each dataframe using repartition
def write_df(df):
    for a in df:
        globals()[a].repartition(1).write.mode("overwrite").option("header",'true').csv(f"/mnt/tokyoolympic/transformed-data/{a}")

df_names = ["Athletes_transformed", "Coaches", "EntriesGender", "Medals", "Teams"]
write_df(df_names)

#Athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolympic/transformed-data/athletes")


# COMMAND ----------


