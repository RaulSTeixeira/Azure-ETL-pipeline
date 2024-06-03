# Data Engineer Foundations: Azure ETL pipeline

## Introduction

The aim of this project is to create an ETL pipeline to first extract data from a datasource, then transform and aggregate the data so that analytics can be derived easily and finally load the transformed data into a SQL database.

This pipeline uses tools from Azure, including Data Factory, Databricks, Data Lake Storage Gen2 and Synapse Analytics. PowerBi might be added in the future, for visualizations.

To keep things simple I've added the source data to github, in csv format, from where Data Factory can connect and ingest data.

## Table of Contents

- [Arquitecture overview](#Arquitecture-Overview)
- [Data Source](#DataSource)
- [Data Ingestion](#Data-Ingestion)
- [Data Storage](#Data-Storage)
- [Data Transformation](#Data-Transformation)
- [Data Load](#Data-Load)
- [Data Analysis](#Data-Analysis)


## Arquitecture Overview

![Pipeline2 drawio (1)](https://github.com/RaulSTeixeira/Azure-tokyo-olympics-project/assets/118553146/1ce08a90-a100-4a06-bbdf-edf539824b56)
- Data Factory was used to ingest data, using as source type HTTP (csv files hosted in Github) and sinking data to Data Lake Storage Gen2;
- DataBricks was connected to Data Lake and used to transform data;
- Synapse Analytics hosted the SQL database and was connected to Data Lake;

## Data Source
The data used in this project is from Tokyo Olympics 2021, available at: [Kaggle](https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo)

It consists of five different CSV files:

![image](https://github.com/RaulSTeixeira/Azure-tokyo-olympics-project/assets/118553146/62087d3a-f628-40b2-a532-7fa72a0949d3)

## Data Ingestion



### Data Storage

## Data Transformation
After data ingestion, Databricks was used to performe some transformations. The first step was to connect databricks to data lake storage, which was performed using mounting storage. Databricks enables users to mount cloud object storage to the Databricks File System (DBFS) to simplify data access. More information is available [HERE](https://docs.databricks.com/en/dbfs/mounts.html)

```python
 # Establish connection with ADLS

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "XXXXXXXXXXXXXXXXX",
"fs.azure.account.oauth2.client.secret": 'XXXXXXXXXXXXXXX',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/XXXXXXXXXXXX/oauth2/token"}

dbutils.fs.mount(
source = "abfss://XXXXXXXXXXXXXXX@XXXXXXXXXXX.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)
```

NOTE: This connection methode was used to keep things simple, but it is not the recomended way to perform this connection, for more information on how to properly connect databricks to data lake, see [HERE](https://docs.databricks.com/en/connect/storage/azure-storage.html#language-Azure%C2%A0service%C2%A0principal)

Databricks automaticaly creates a spark session, that you can retrieve some information by using the command "spark".

```python
SparkSession - hive

SparkContext

Spark UI

Version
v3.4.1
Master
local[*, 4]
AppName
Databricks Shell
```

The CSV files were read and converted to spark dataframes using PySpark, mantaining original schema and headers. The athletes full name was separeted into first and last name.

```python
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

# Create a new table for the Athetes that separates the first and last name

# Create the new FirstName and LastName fields
Athletes_transformed = Athletes.withColumn("FirstName", split(col("PersonName"), " ").getItem(1)).withColumn("LastName", split(col("PersonName"), " ").getItem(0))

# Remove the PersonName field
Athletes_transformed = Athletes_transformed.drop("PersonName")

display(Athletes_transformed.limit(5))
```

I also did some experiments and small data analysis using pyspark.sql module and %sql "Magic" that allows you to directly work with SQL to query data. For this you need to create a temporary view using "createOrReplaceTempView"

```python
# Order medals table by descending order and display first 10 countries
Medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").limit(10).display()

# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = EntriesGender.withColumn(
    'Avg_Female', EntriesGender['Female'] / EntriesGender['Total']
).withColumn(
    'Avg_Male', EntriesGender['Male'] / EntriesGender['Total']
)
average_entries_by_gender.show()

# Run querys using SQL to find all the athletes from Portugal

# Create a temporary view so it can be used by spark SQL library
Athletes_transformed.createOrReplaceTempView("athletestransformed")

spark_df = spark.sql("SELECT * FROM athletestransformed WHERE Country = 'Portugal'")
display(spark_df)
```
```sql
%sql
-- Use SQL directly with the 'magic' %sql
-- Dont forget to create a temp view (already done in previous block)
-- Count Portuguese athletes
SELECT COUNT(*) AS Number_Portuguese_Athletes
FROM athletestransformed
Where Country = 'Portugal'

%sql

-- Use SQL directly with the 'magic' %sql
-- Count portuguese athletes last name
    
SELECT 
  LastName,
  COUNT (LastName) AS name_count
FROM athletestransformed
WHERE Country = 'Portugal'
GROUP BY LastName
ORDER BY name_count DESC
```
It is also possible to convert spark dataframes to pandas dataframes, allowing to use a widly used sintax.

```python
EntriesGender_pandas_df = EntriesGender.select("*").toPandas()

EntriesGender_pandas_df['Avg_Female'] = EntriesGender_pandas_df['Female'] / EntriesGender_pandas_df['Total']
EntriesGender_pandas_df['Avg_Male'] = 1 - EntriesGender_pandas_df['Avg_Female']
```
Finaly after the transformations, all the modified dataframes were writen back do azure datalake.

```python
# Write modified dataframes back to Azure Data Lake
# We can define the number of partitons per each dataframe using ".repartition(numPartitions, *cols)", this method allows to define the number of partions that the table will have and by which column (hash partitioned), ".rdd.getNumPartitions()" allows to see the amount of partitions

def write_df(df):
    for a in df:
        globals()[a].repartition(1).write.mode("overwrite").option("header",'true').csv(f"/mnt/tokyoolympic/transformed-data/{a}")

df_names = ["Athletes_transformed", "Coaches", "EntriesGender", "Medals", "Teams"]
write_df(df_names)
```
### Data Load

### Data Analysis
