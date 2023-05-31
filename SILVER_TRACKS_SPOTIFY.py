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

df_bronze = spark.table("spotify_bronze.playlists").filter(f"partition == '{particao}' and datesilver is null ")
df_bronze.display()

# COMMAND ----------

#itens das playlist
df_tracks = df_bronze.select("id",explode("tracks.items").alias("musica"))
#detalhes da playlist
df_tracks = df_tracks.select("id","musica.*")
#tracks da playlist
df_tracks = df_tracks.select(col("id").alias("id_playlist"),"track.*")
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
                     col("uri").alias("uri_track"))
             .withColumnRenamed("href","href_album")
             .withColumnRenamed("id","id_album")
             .withColumnRenamed("name","name_album")
             .withColumnRenamed("artists","artistas_album")
            )
#partição
df_tracks = df_tracks.withColumn("partition",lit(particao))


# COMMAND ----------

#dbutils.fs.ls(bronzePath)

# COMMAND ----------

try:
    if df_tracks.count() > 0:
        df_tracks\
        .write\
        .format("delta")\
        .partitionBy("partition")\
        .mode("append")\
        .option("mergeSchema", "true")\
        .option("overwriteSchema", "true")\
        .save(silverPath)
    
        spark.sql(f"update delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze` set datesilver = current_timestamp() where datesilver is null and partition == '{particao}'")
    else:
        print("sem arquivos")
except:
    print("deu ruim")


# COMMAND ----------

#%sql
#select * from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze`
