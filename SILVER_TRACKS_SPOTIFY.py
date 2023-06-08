# Databricks notebook source
#from pyspark.sql.functions import *
#from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lit, to_timestamp
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

account_name = "aulafiaead"
account_key = "QDKbVST0U3yAaEI4HN9DFwYTB3jGO6xb4Kk5r59UFYOzXrkrVLESZKmrKzPZ/eEsDLV8Fw5XxybA+ASt4EZ2zA=="
container_name = "grupo5"
directory_name = "landing"

spark = SparkSession.builder\
    .config("spark.sql.extensions", "org.apache.spark.sql.delta.sources.DeltaDataSource")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

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

df_bronze = spark.read.format("delta").load(bronzePath)
df_bronze = df_bronze.filter(f"datesilver is null ")
#df_bronze.display()

# COMMAND ----------

#itens das playlist
df_tracks = df_bronze.select("id","partition",explode("tracks.items").alias("musica"))
#detalhes da playlist
df_tracks = df_tracks.select("id","partition","musica.*")
#tracks da playlist
df_tracks = df_tracks.select(col("id").alias("id_playlist"),"partition","track.*")
#Albuns e tracks
df_tracks = (df_tracks
             .select("id_playlist",
                     "album.*",
                     col("artists").alias("artista_Track"),
                     "duration_ms",
                     "external_ids.isrc",
                     "external_urls.spotify",
                     col("href").alias("href_track"),
                     col("id").alias("id_track"),
                     col("name").alias("name_track"),
                     col("popularity").alias("popularity_track"),
                     "track_number",
                     col("type").alias("type_track"),
                     col("uri").alias("uri_track"),
                     "partition")
             .withColumnRenamed("href","href_album")
             .withColumnRenamed("id","id_album")
             .withColumnRenamed("name","name_album")
             .withColumnRenamed("artists","artistas_album")
             .withColumnRenamed("popularity","popularity_album")
             .withColumnRenamed("type","type_album")
             .withColumnRenamed("uri","uri_album")
            )


# COMMAND ----------

#dbutils.fs.ls(bronzePath)
#df_tracks.printSchema()

# COMMAND ----------

#df = spark.sql("select * from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/silver` ")
#df.printSchema()

# COMMAND ----------

try:
    if df_tracks.count() > 0:
        #df_tracks\
        #.write\
        #.format("delta")\
        #.partitionBy("partition")\
        #.mode("overwrite")\
        #.option("overwriteSchema", "true")\
        #.save(silverPath)
        #.option("mergeSchema", "true")\
        #le tabela silver no diretório
        df_silver = DeltaTable.forPath(spark, silverPath)

        #faz o merge apenas com insert de novos snapshot_ids
        df_silver\
            .alias("silver")\
            .merge(df_tracks.alias("tracks"),
                   "silver.id_playlist = tracks.id_playlist and silver.id_track = tracks.id_track and silver.id_album = tracks.id_album and silver.partition = tracks.partition"
                  )\
            .whenNotMatchedInsertAll()\
            .whenMatchedUpdateAll()\
            .execute()
        
    
        spark.sql(f"update delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze` set datesilver = current_timestamp() where datesilver is null")
    else:
        print("sem arquivos")
except Exception as e:
    print(f"deu ruim:{e}")


# COMMAND ----------

#%sql

#delete from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/silver`

# COMMAND ----------

#%sql
#select id_playlist ,count(1) as qtd from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/silver` group by id_playlist

# COMMAND ----------

#%sql
#select * from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/silver`
