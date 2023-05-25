# Databricks notebook source
account_name = "aulafiaead"
account_key = "QDKbVST0U3yAaEI4HN9DFwYTB3jGO6xb4Kk5r59UFYOzXrkrVLESZKmrKzPZ/eEsDLV8Fw5XxybA+ASt4EZ2zA=="
container_name = "grupo5"
directory_name = "landing"

spark.conf.set('fs.azure.account.key.' + account_name + '.blob.core.windows.net', account_key)
rootPath = "wasbs://" + container_name + "@" + account_name + ".blob.core.windows.net/"
bronzePath = rootPath+"delta/bronze"
silverPath = rootPath+"delta/silver"
landzonePath = rootPath+"landing"
#display(dbutils.fs.ls(filePath))

# COMMAND ----------

# %sql
# create table if not exists spotify_bronze.playlists
# using delta
# location "wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze"

# COMMAND ----------

# %sql
# create table if not exists spotify_silver.tracks
# using delta
# location "wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/silver"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime

#Leitura do arquivo de Playlists do Spotify
df = (
    spark
        .read
        .option("multiline", "true")
        .json(landzonePath)
)


# COMMAND ----------

#cria arquivo delta na bronze
df = df.withColumn("partition",lit(datetime.now().strftime("%Y%m%d")))
df\
    .write\
    .format("delta")\
    .mode("append")\
    .option("overwriteSchema", "true")\
    .save(bronzePath)

# COMMAND ----------

#itens das playlist
df_tracks = df.select("id",explode("tracks.items").alias("musica"))
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
             .withColumnRenamed("popularity","popularity_album")
             .withColumnRenamed("type","type_album")
             .withColumnRenamed("uri","uri_album")
            )
#partição
df_tracks = df_tracks.withColumn("partition",lit(datetime.now().strftime("%Y%m%d")))


# COMMAND ----------

#display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

df_tracks\
    .write\
    .format("delta")\
    .partitionBy("partition")\
    .mode("append")\
    .option("overwriteSchema", "true")\
    .save(silverPath)

# COMMAND ----------

# %sql
# select * from delta.`wasbs://grupo5@aulafiaead.blob.core.windows.net/delta/bronze`

# COMMAND ----------

# %sql
# select * from spotify_silver.tracks

# COMMAND ----------

#display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

#%sql
#describe extended spotify_bronze.playlists
