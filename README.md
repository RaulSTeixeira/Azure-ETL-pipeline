# Data Engineer Foundations: Azure ETL pipeline

## Introduction

The aim of this project is to create an ETL pipeline to first extract data from a datasource, then transform and aggregate the data so that analytics can be derived easily and finally load the transformed data into a SQL database.

This pipeline uses tools from Azure, including Data Factory, Databricks, Data Lake Storage Gen2 and Synapse Analytics. PowerBi might be added in the future, for visualizations.

To keep things simple I've added the source data to github, in csv format, from where Data Factory can connect and ingest data.

## Table of Contents

- [Arquitecture overview](#Arquitecture-Overview)
- [Source of data](#Source-of-data)
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

## Source of data
The data used in this project is from Tokyo Olympics 2021, available at: [Kaggle](https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo)

It consists of five different CSV files:

![image](https://github.com/RaulSTeixeira/Azure-tokyo-olympics-project/assets/118553146/62087d3a-f628-40b2-a532-7fa72a0949d3)

## Data Ingestion
For data ingestion a pipeline was developed in Data Factory, using the Copy data Activity. This requires a Source and a Sink to be defined. For this case, since the data was available in github, the source type is a generic protocol HTTP with DelimitedText format (CSV) and the first row was used as header. A linked service was also defined, where the URL of the specific file is placed. Even though this was performed using the graphical interface, it can also be defined in a JSON format:

```json
{
	"name": "MedalsSource",
	"properties": {
		"linkedServiceName": {
			"referenceName": "MedalsHTTP",
			"type": "LinkedServiceReference"
		},
		"annotations": [],
		"type": "DelimitedText",
		"typeProperties": {
			"location": {
				"type": "HttpServerLocation"
			},
			"columnDelimiter": ",",
			"escapeChar": "\\",
			"firstRowAsHeader": true,
			"quoteChar": "\""
		},
		"schema": []
	}
}

{
	"name": "MedalsHTTP",
	"properties": {
		"annotations": [],
		"type": "HttpServer",
		"typeProperties": {
			"url": "https://raw.githubusercontent.com/RaulSTeixeira/Azure-tokyo-olympics-project/master/data/Medals.csv",
			"enableServerCertificateValidation": true,
			"authenticationType": "Anonymous"
		}
	}
}
```

To keep things simple, the authentication method was kept as anonymous.
For the Sink, a linked service to Data Lake Gen2 was made to a previously created container. In this container two folders were defined, one for the raw_data and another to the transformed_data (used at a later stage). Again using DelimitedText format (CSV) and the first row as header.

Since data factory only stores the most recent version of a pipeline (after publishing), data factory was connected to github to allow version control and track of changes. Since this is a personal project, every change was published in the master branch.

Here is a preview of the pipeline:
![data factory pipeline3](https://github.com/RaulSTeixeira/Azure-tokyo-olympics-project/assets/118553146/bb5386a6-60b7-44bd-bbe6-449c537c0225)

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
## Data Load
### Serverless
During the creation of the Synapse Analytics workspace is necessary to create or select a Data Lake Storage Gen2, serving as the primary storage account for the workspace, holding catalog data and metadata.
Since i have selected the previously created Data Lake, where olympics data files are already stored, they automatically become acessible from synapse analytics.

Using the serverless SQL pool available by default in Synapse Analytics, it's a simple way to read the content and structure of the files from our datalake.

The OPENROWSET(BULK...) function allows to access files in Azure Storage. OPENROWSET function reads content of a remote data source and returns the content as a set of rows:

```sql
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://tokyoolympicsraul.dfs.core.windows.net/tokyo-olympics-container/transformed-data/Athletes_transformed/**',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = True
    ) AS [result]

```
It is also possible to create a database in a serverless SQL pool, by defining an external datasource (in this case the datalake), an external file format and finally external tables. This provides a relational database layer over files in a data lake, allowing the usage of standard SQL query semantics. This type of tables also have the advantage of always being up-to-date once the data ingestion is performed, since they only store metadata. Droping external tables does not delete the source data, only the stored metadata.

Here is an example of creating external tables, just for the athletes and coaches:

```sql
-- Creating a Serverless SQL Database
CREATE DATABASE TokyoTestDB
    COLLATE Latin1_General_100_BIN2_UTF8;
GO

USE TokyoTestDB;
GO

-- Create external datasource
CREATE EXTERNAL DATA SOURCE files
WITH (
    LOCATION = 'https://tokyoolympicsraul.dfs.core.windows.net/tokyo-olympics-container/transformed-data/'
);

-- Create external file format
CREATE EXTERNAL FILE FORMAT CsvFormat
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2 -- this specifies the first row to be read, since we have a header we only start at the second one
        )
    );
GO

-- Create external tables
CREATE EXTERNAL TABLE dbo.Athletes
(
    Country NVARCHAR(80),
    Discipline NVARCHAR(80),
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50)
)
WITH
(
    DATA_SOURCE = files,
    LOCATION = 'Athletes_transformed/*.csv',
    FILE_FORMAT = CsvFormat
);
GO

CREATE EXTERNAL TABLE dbo.Coaches
(
    Name NVARCHAR(80),
    Country NVARCHAR(80),
    Discipline NVARCHAR(50),
    Event NVARCHAR(50)
)
WITH
(
    DATA_SOURCE = files,
    LOCATION = 'Coaches/*.csv',
    FILE_FORMAT = CsvFormat
);
GO

-- sintax to drop tables and file format
--DROP EXTERNAL TABLE dbo.Athletes
--DROP EXTERNAL FILE FORMAT CsvFormat
```

### Dedicated SQL Pool
As previously mentioned Synapse Analytics will host the SQL database where data will be loaded, this type of dedicated SQL pool requires a running server that implies extra costs (usually charged by hour, instead of charged by query). Nevertheless it acts more like a "regular" database, storing data, metadata and allowing for more advanced operations and table relations. As a note, Synapse Analytics Dedicated SQL Pool is optimized for data warehousing workloads, having some limitations, such as defining foreight keys.

The first step of creating the database is to start a SQL pool (server), for this project the chosen performance was DW100c. More info on DWU's (Data Warehouse Units) [HERE](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/what-is-a-data-warehouse-unit-dwu-cdwu)

After the database was created, tables definitions was added. Notice that an extra column was added, with a unique ID, using the IDENTITY [(seed , increment)] function. This will automatically add an increasing and unique number per row, once the data is loaded. In synapse analytics, values for identity aren't incremental due to the distributed architecture of the data warehouse, so there might be some gaps on ID generation.

The different tables were created using a replicated distribution (equal across processing nodes) and CLUSTERED COLUMNSTORE INDEX as the storage type.

```sql
-- Creating Tables
-- Note: Tables created in synape analytics SQL pool does not suport foreight keys
CREATE TABLE dbo.Athletes
(
    AthleteID INT PRIMARY KEY NONCLUSTERED NOT ENFORCED IDENTITY(1,1) NOT NULL, --IDENTITY [ (seed , increment) ] seed is the first value and increment is the amount added to the previous row
    Country NVARCHAR(80) NULL,
    Discipline NVARCHAR(80) NOT NULL,
    FirstName NVARCHAR(50) NULL,
    LastName NVARCHAR(50) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dbo.Coaches
(
    CoacheID INT PRIMARY KEY NONCLUSTERED NOT ENFORCED IDENTITY NOT NULL,
    Country NVARCHAR(80) NULL,
    Discipline NVARCHAR(80) NOT NULL,
    Event NVARCHAR(50) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);
```
To load data into the table, the COPY INTO statement was used with IDENTITY_INSERT = 'OFF'. In this way, the column with the unique ID, previously created most not be specified.

```sql
COPY INTO dbo.Athletes
    (Country, Discipline, FirstName, LastName )
FROM 'https://tokyoolympicsraul.dfs.core.windows.net/tokyo-olympics-container/transformed-data/Athletes_transformed/**.csv'
WITH
(
    FILE_TYPE = 'CSV',
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);

COPY INTO dbo.Coaches
    (Name, Country, Discipline, Event)
FROM 'https://tokyoolympicsraul.dfs.core.windows.net/tokyo-olympics-container/transformed-data/Coaches/**.csv'
WITH
(
    FILE_TYPE = 'CSV',
    MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);
```

### Data Analysis
