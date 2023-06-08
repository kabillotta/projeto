# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, to_timestamp, input_file_name
#from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime
import pytz

# COMMAND ----------

account_name = "aulafiaead"
account_key = "QDKbVST0U3yAaEI4HN9DFwYTB3jGO6xb4Kk5r59UFYOzXrkrVLESZKmrKzPZ/eEsDLV8Fw5XxybA+ASt4EZ2zA=="
container_name = "grupo5"
directory_name = "landing"

spark = SparkSession.builder\
    .config("spark.sql.extensions", "org.apache.spark.sql.delta.sources.DeltaDataSource")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

# spark = (SparkSession.builder
#          .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#          .config("spark.hadoop.fs.s3a.access.key", "AqJ2HkXFs2hfWxS4")
#          .config("spark.hadoop.fs.s3a.secret.key", "f254hN80Xari9Fuh9HOPVIGyYSiVB8HY")
#          .config("spark.hadoop.fs.s3a.path.style.access", True)
#          .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#          .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
#          .getOrCreate()
#         )

spark.conf.set('fs.azure.account.key.' + account_name + '.blob.core.windows.net', account_key)
rootPath = "wasbs://" + container_name + "@" + account_name + ".blob.core.windows.net/"
bronzePath = rootPath+"delta/bronze"
silverPath = rootPath+"delta/silver"
landzonePath = rootPath+"landing"
#display(dbutils.fs.ls(filePath))
particao = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime("%Y%m%d")

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
df_land = (
    spark
        .read
        .option("multiline", "true")
        .json(landzonePath)
)
#coloca a partição, coluna de controle da silvar e nome do arquivos
df_land = df_land.withColumn("partition", lit(particao))\
                 .withColumn("datesilver", to_timestamp(lit(None)))\
                 .withColumn("file_name", input_file_name())\

#le tabela bronze no diretório
df_bronze = DeltaTable.forPath(spark, bronzePath)

#faz o merge apenas com insert de novos snapshot_ids
df_bronze\
    .alias("bronze")\
    .merge(df_land.alias("land"),
           "bronze.id = land.id and bronze.partition = land.partition"
          )\
    .whenNotMatchedInsertAll()\
    .execute()

#.whenMatchedUpdateAll()\

#gravação bronze
#df\
#    .withColumn("partition", lit(particao))\
#    .withColumn("datesilver", to_timestamp(lit(None)))\
#    .withColumn("file_name", input_file_name()))\
#    .write\
#    .format("delta")\
#    .partitionBy("partition")\
#    .mode("overwrite")\
#    .option("overwriteSchema", "true")\
#    .save(bronzePath)

# COMMAND ----------

#sql
#delete from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze`

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select file_name,snapshot_id,id,datesilver from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze`

# COMMAND ----------

#dbutils.fs.ls("wasbs://grupo5@aulafiaead.blob.core.windows.net/landing")