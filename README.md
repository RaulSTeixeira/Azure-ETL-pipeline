# Data Engineer Foundations: Azure ETL pipeline

## Introduction

The aim of this project is to create an ETL pipeline to first extract data from a datasource, then transform and aggregate the data so that analytics can be derived easily and finally load the transformed data into a SQL database.

This pipeline will use tools from Azure, including Data Factory, Databricks, Data Lake Storage Gen2 and Synapse Analytics. PowerBi might be added in the future, for visualizations.

To keep things simple I've added the source data to github, in csv format, from where Data Factory can connect and extract the necessary data.

## Table of Contents

- [Arquitecture overview](#Arquitecture-Overview)
- [Used data](#DataSource)
- [Extract data from github using Data Factory](#Data-Ingestion)
- [Data Storage using Data Lake Storage Gen 2](#Data-Storage)
- [Transform data using Databricks](#Data-Transformation)
- [Load data with Synapse Analytics](#Load-Data-into-SQL-Database)
- [Data analys](#Data-Analysis)


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


### Data Analysis

## Load Data into SQL Database
