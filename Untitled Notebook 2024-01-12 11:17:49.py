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



# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
  source = "abfss://youtube-json@youtubedataset.dfs.core.windows.net/",
  mount_point = "/mnt/youtube-json/",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC  ls '/mnt/youtube-json'

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

CA_category = columns.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),("snippet.title"))

display(CA_category)



# COMMAND ----------



#converting the "id" into an interger
from pyspark.sql.types import *

ca_select = CA_category.select(col("etag"),col("id"),col( "channelId"),col("title"))
display(ca_select)
ca_rename = ca_select.withColumnRenamed("title","category")

ca_save = ca_rename.withColumn("id",col("id").cast("INT"))
ca_save.write.mode('overwrite').parquet('mnt/youtube-json/jsontoparquet/CAcategoryid')



# COMMAND ----------

input_path2 = '/mnt/yotube/youtubejson/DE_category_id.json'

df2 = spark.read.option("multiline","true").json(input_path2)

items2 = df2.select(explode("items").alias ("items2"))

columns2 = items2.select(
    col("items2.etag").alias("etag"),
    col("items2.snippet").alias ("snippet"),
    col("items2.id").alias("id"),
    col("items2.kind").alias("kind"),
  )

de_select = columns2.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))

de_parquet = de_select.select(col("etag"),col("id"),col( "channelId"),col("title"))

de_1 = de_parquet.withColumn("id",col("id").cast("INT"))
de_rename = de_1.withColumnRenamed("title","category")
display(de_rename)

de_rename.write.mode('overwrite').parquet('mnt/youtube-json/jsontoparquet/DEcategoryid')



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

display(FR_category_id )


# COMMAND ----------

fr_parquet = FR_category_id.select(col("etag"),col("id"),col( "channelId"),col("title"))
display(fr_parquet)

fr_cast = fr_parquet.withColumn("id",col("id").cast("INT"))
fr_rename = fr_cast.withColumnRenamed("title","category")

fr_rename.write.mode('overwrite').parquet('mnt/youtube-json/jsontoparquet/FRcategoryid')

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

display(GB_category_id)

# COMMAND ----------

gb_parquet = GB_category_id.select(col("etag"),col("id"),col( "channelId"),col("title"))


gb_cast = gb_parquet.withColumn("id",col("id").cast("INT"))
gb_rename = gb_cast.withColumnRenamed("title","category")
display(gb_rename)

gb_rename.write.mode('overwrite').parquet('mnt/youtube-json/jsontoparquet/GBcategoryid')

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
display(RU_category_id)

# COMMAND ----------

ru_select = RU_category_id.select(col("etag"),col("id"),col( "channelId"),col("title"))
display(ru_select)

ru_cast = ru_select.withColumn("id",col("id").cast("INT"))
ru_rename = ru_cast.withColumnRenamed("title","category")
display(ru_rename)

ru_rename.write.mode('overwrite').parquet('mnt/youtube-json/jsontoparquet/RUcategoryid')

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

US_category_id = columns9.select(col("etag"),col("id"),col("kind"),col("snippet.channelId"),col("snippet.title"))

display(US_category_id)

# COMMAND ----------

us_select =  US_category_id.select(col("etag"),col("id"),col( "channelId"),col("title"))


us_cast = us_select.withColumn("id",col("id").cast("INT"))
us_rename = us_cast.withColumnRenamed("title","category")
display(us_rename)

us_rename.write.mode('overwrite').parquet('mnt/youtube-json/jsontoparquet/RUcategoryid')
