# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime

# COMMAND ----------

account_name = ""
account_key = ""
container_name = ""
directory_name = ""

spark.conf.set('fs.azure.account.key.' + account_name + '.blob.core.windows.net', account_key)
rootPath = "wasbs://" + container_name + "@" + account_name + ".blob.core.windows.net/"
bronzePath = rootPath+"delta/bronze"
silverPath = rootPath+"delta/silver"
landzonePath = rootPath+"landing"
#display(dbutils.fs.ls(filePath))
particao = datetime.now().strftime("%Y%m%d")

# COMMAND ----------

# %sql
# create database if not exists spotify_bronze;
# 
# create table if not exists spotify_bronze.playlists
# using delta
# location "wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze";
# 
# create database if not exists spotify_silver;
# 
# create table if not exists spotify_silver.playlists
# using delta
# location "wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/silver";

# COMMAND ----------

#Leitura do arquivo de Playlists do Spotify
df = (
    spark
        .read
        .option("multiline", "true")
        .json(landzonePath)
)

#gravação bronze
df\
    .withColumn("partition", lit(particao))\
    .withColumn("datesilver", to_timestamp(lit(None)))\
    .write\
    .format("delta")\
    .partitionBy("partition")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .save(bronzePath)
