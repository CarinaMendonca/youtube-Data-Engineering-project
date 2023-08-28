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
# MAGIC ls '/mnt/youtube-cleansed'

# COMMAND ----------



input_parth = '/mnt/yotube/youtubejson/CA_category_id.json'

df = spark.read.option("multiline","true").json(input_parth)

from pyspark.sql.functions import explode, col, split

items = df.select(explode("items").alias ("items"))



display(items)

# COMMAND ----------


columns = items.select(
    col("items.etag").alias("etag"),
    col("items.snippet").alias ("snippet"),
    col("items.id").alias("id"),
    col("items.kind").alias("kind"),
  )

CA_category_id = columns.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),("snippet.title"))

display(CA_category_id)



# COMMAND ----------

CA_category_id.printSchema()

# COMMAND ----------

#converting the "id" into an interger
from pyspark.sql.types import *
CA_category_id.withColumn("id",col("id").cast("INT")).printSchema()

# COMMAND ----------

CA_category_id.write.mode('overwrite').parquet('mnt/youtube-cleansed/CA_category_id')

# COMMAND ----------


input_path2 = '/mnt/yotube/youtubejson/DE_category_id.json'

df2 = df = spark.read.option("multiline","true").json(input_path2)

items2 = df.select(explode("items").alias ("items2"))

columns2 = items2.select(
    col("items2.etag").alias("etag"),
    col("items2.snippet").alias ("snippet"),
    col("items2.id").alias("id"),
    col("items2.kind").alias("kind"),
  )

DE_category_id = columns2.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))

DE_category_id.withColumn("id",col("id").cast("INT"))


DE_category_id.write.parquet("/mnt/youtube-cleansed/DE_category_id")



# COMMAND ----------

input_path3 = '/mnt/yotube/youtubejson/FR_category_id.json'

df3 = spark.read.option("multiline","true").json(input_path3)

items3 = df3.select(explode("items").alias ("items3"))

columns3 = items3.select(
    col("items3.etag").alias("etag"),
    col("items3.snippet").alias ("snippet"),
    col("items3.id").alias("id"),
    col("items3.kind").alias("kind"),
  )

FR_category_id = columns3.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))

FR_category_id.withColumn("id",col("id").cast("INT"))

FR_category_id.write.parquet("/mnt/youtube-cleansed/FR_category_id")





# COMMAND ----------

input_path4 = '/mnt/yotube/youtubejson/GB_category_id.json'

df4 = spark.read.option("multiline","true").json(input_path4)

items4 = df4.select(explode("items").alias ("items4"))

columns4 = items4.select(
    col("items4.etag").alias("etag"),
    col("items4.snippet").alias ("snippet"),
    col("items4.id").alias("id"),
    col("items4.kind").alias("kind"),
  )

GB_category_id = columns4.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))

GB_category_id.withColumn("id",col("id").cast("INT"))

GB_category_id.write.parquet("/mnt/youtube-cleansed/GB_category_id")




# COMMAND ----------

input_path5 = '/mnt/yotube/youtubejson/IN_category_id.json'

df5 = spark.read.option("multiline","true").json(input_path5)

items5 = df5.select(explode("items").alias ("items5"))

columns5 = items5.select(
    col("items5.etag").alias("etag"),
    col("items5.snippet").alias ("snippet"),
    col("items5.id").alias("id"),
    col("items5.kind").alias("kind"),
  )

IN_category_id = columns5.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))

IN_category_id.withColumn("id",col("id").cast("INT"))

IN_category_id.write.parquet("/mnt/youtube-cleansed/IN_category_id")



# COMMAND ----------

input_path6 = '/mnt/yotube/youtubejson/JP_category_id.json'

df6 = spark.read.option("multiline","true").json(input_path6)

items6 = df6.select(explode("items").alias ("items6"))

columns6 = items6.select(
    col("items6.etag").alias("etag"),
    col("items6.snippet").alias ("snippet"),
    col("items6.id").alias("id"),
    col("items6.kind").alias("kind"),
  )

JP_category_id = columns6.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))
JP_category_id.withColumn("id",col("id").cast("INT"))

JP_category_id.write.parquet("/mnt/youtube-cleansed/JP_category_id")





# COMMAND ----------

input_path7 = '/mnt/yotube/youtubejson/KR_category_id.json'

df7 = spark.read.option("multiline","true").json(input_path7)

items7 = df7.select(explode("items").alias ("items7"))

columns7 = items7.select(
    col("items7.etag").alias("etag"),
    col("items7.snippet").alias ("snippet"),
    col("items7.id").alias("id"),
    col("items7.kind").alias("kind"),
  )

KR_category_id = columns7.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))
KR_category_id.withColumn("id",col("id").cast("INT"))

KR_category_id.write.parquet("/mnt/youtube-cleansed/KR_category_id")





# COMMAND ----------

input_path8 = '/mnt/yotube/youtubejson/MX_category_id.json'

df8 = spark.read.option("multiline","true").json(input_path8)

items8 = df8.select(explode("items").alias ("items8"))

columns8 = items8.select(
    col("items8.etag").alias("etag"),
    col("items8.snippet").alias ("snippet"),
    col("items8.id").alias("id"),
    col("items8.kind").alias("kind"),
  )

MX_category_id = columns8.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))
MX_category_id.withColumn("id",col("id").cast("INT"))

MX_category_id.write.parquet("/mnt/youtube-cleansed/MX_category_id")




# COMMAND ----------

input_path9 = '/mnt/yotube/youtubejson/US_category_id.json'

df9 = spark.read.option("multiline","true").json(input_path9)

items9 = df9.select(explode("items").alias ("items9"))

columns9 = items9.select(
    col("items9.etag").alias("etag"),
    col("items9.snippet").alias ("snippet"),
    col("items9.id").alias("id"),
    col("items9.kind").alias("kind"),
  )

US_category_id = columns.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))
US_category_id.withColumn("id",col("id").cast("INT"))

US_category_id.write.parquet("/mnt/youtube-cleansed/US_category_id")


# COMMAND ----------

input_path10 = '/mnt/yotube/youtubejson/RU_category_id.json'

df10 = spark.read.option("multiline","true").json(input_path10)

items10 = df10.select(explode("items").alias ("items10"))

columns10 = items10.select(
    col("items10.etag").alias("etag"),
    col("items10.snippet").alias ("snippet"),
    col("items10.id").alias("id"),
    col("items10.kind").alias("kind"),
  )

RU_category_id = columns10.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))
RU_category_id.withColumn("id",col("id").cast("INT"))

RU_category_id.write.parquet("/mnt/youtube-cleansed/RU_category_id")



