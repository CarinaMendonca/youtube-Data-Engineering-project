# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://yotube@youtubedataset.dfs.core.windows.net/",
  mount_point = "/mnt/yotube",
  extra_configs = configs)


# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
  source = "abfss://youtube-cleansed@youtubedataset.dfs.core.windows.net/",
  mount_point = "/mnt/youtube-cleansed/",
  extra_configs = configs)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/youtube-cleansed"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/yotube"

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *


CAVideos = spark.read.format("csv").option("header","true").load("/mnt/yotube/youtubecsv/region=ca/CAvideos.csv")
CAVideos_Add = CAVideos.withColumn('region',lit('Canada'))
display(CAVideos_Add)

# COMMAND ----------

CAVideos_Add.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

CAVideos_cast = CAVideos_Add.withColumn("category_id",col("category_id").cast("INT")).withColumn("views",col("views").cast("INT")).withColumn("likes",col("likes").cast("INT")).withColumn("dislikes",col("dislikes").cast("INT"))

CAVideos_cast.printSchema()

# COMMAND ----------

DEVideos = spark.read.format('csv').option('header','true').load("/mnt/yotube/youtubecsv/region=de/DEvideos.csv")
DEVideos_Add = DEVideos.withColumn('region',lit("Germany"))
DEVideos_Add.show()
FRVideos = spark.read.format("csv").option("header","true").load("/mnt/yotube/youtubecsv/region=fr/FRvideos.csv")
FRVideos_Add = FRVideos.withColumn('region',lit('France'))
GBVideos = spark.read.format("csv").option("header","true").load("/mnt/yotube/youtubecsv/region=gb/GBvideos.csv")
GBVideos_Add = GBVideos.withColumn('region',lit('GB'))
RUVideos = spark.read.format("csv").option("header","true").load("/mnt/yotube/youtubecsv/region=ru/RUvideos.csv")
RUVideos_Add = RUVideos.withColumn('region',lit('Russia'))
USVideos = spark.read.format("csv").option("header","true").load("/mnt/yotube/youtubecsv/region=us/USvideos.csv")
USVideos_Add = USVideos.withColumn('region',lit('USA'))

# COMMAND ----------

DEVideos_cast = DEVideos_Add.withColumn("category_id",col("category_id").cast("INT")).withColumn("views",col("views").cast("INT")).withColumn("likes",col("likes").cast("INT")).withColumn("dislikes",col("dislikes").cast("INT"))
DEVideos_cast.printSchema()

# COMMAND ----------

FRVideos_cast = FRVideos_Add.withColumn("category_id",col("category_id").cast("INT")).withColumn("views",col("views").cast("INT")).withColumn("likes",col("likes").cast("INT")).withColumn("dislikes",col("dislikes").cast("INT"))
FRVideos_cast.printSchema()

# COMMAND ----------

GBVideos_cast = GBVideos_Add.withColumn("category_id",col("category_id").cast("INT")).withColumn("views",col("views").cast("INT")).withColumn("likes",col("likes").cast("INT")).withColumn("dislikes",col("dislikes").cast("INT"))
GBVideos_cast.printSchema()

# COMMAND ----------

RUVideos_cast = RUVideos_Add.withColumn("category_id",col("category_id").cast("INT")).withColumn("views",col("views").cast("INT")).withColumn("likes",col("likes").cast("INT")).withColumn("dislikes",col("dislikes").cast("INT"))
RUVideos_cast.printSchema()

# COMMAND ----------

USVideos_cast = USVideos_Add.withColumn("category_id",col("category_id").cast("INT")).withColumn("views",col("views").cast("INT")).withColumn("likes",col("likes").cast("INT")).withColumn("dislikes",col("dislikes").cast("INT"))
USVideos_cast.printSchema()

# COMMAND ----------

CAVideos1 = CAVideos_cast.select(col('video_id'),col('title'),col('channel_title'),col('category_id'),col('views'),col('likes'),col('dislikes'),col('region'))

CAVideos1.show()

# COMMAND ----------

DEVideos1 = DEVideos_cast.select(col('video_id'),col('title'),col('channel_title'),col('category_id'),col('views'),col('likes'),col('dislikes'),col('region'))

DEVideos1.show()

# COMMAND ----------

CAVideos1.write.format("parquet").mode("overwrite").save("/mnt/youtube-cleansed/youtubecsvtoparquet/CAVideos")

# COMMAND ----------

FRVideos1 = FRVideos_cast.select(col('video_id'),col('title'),col('channel_title'),col('category_id'),col('views'),col('likes'),col('dislikes'),col('region'))

FRVideos1.show()

# COMMAND ----------

GBVideos1 = GBVideos_cast.select(col('video_id'),col('title'),col('channel_title'),col('category_id'),col('views'),col('likes'),col('dislikes'),col('region'))

GBVideos1.show()

# COMMAND ----------

RUVideos1 = RUVideos_cast.select(col('video_id'),col('title'),col('channel_title'),col('category_id'),col('views'),col('likes'),col('dislikes'),col('region'))

RUVideos1.show()

# COMMAND ----------

USVideos1 = USVideos_cast.select(col('video_id'),col('title'),col('channel_title'),col('category_id'),col('views'),col('likes'),col('dislikes'),col('region'))

USVideos1.show()

# COMMAND ----------

DEVideos1.write.format("parquet").mode("overwrite").save("/mnt/youtube-cleansed/youtubecsvtoparquet/DAVideos")
FRVideos1.write.format("parquet").mode("overwrite").save("/mnt/youtube-cleansed/youtubecsvtoparquet/FRVideos")
GBVideos1.write.format("parquet").mode("overwrite").save("/mnt/youtube-cleansed/youtubecsvtoparquet/GBVideos")
RUVideos1.write.format("parquet").mode("overwrite").save("/mnt/youtube-cleansed/youtubecsvtoparquet/RUVideos")
USVideos1.write.format("parquet").mode("overwrite").save("/mnt/youtube-cleansed/youtubecsvtoparquet/USVideos")
