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
# MAGIC ls '/mnt/youtube'

# COMMAND ----------

input_parth = '/mnt/youtube-cleansed/youtubejsontoparquet/CA_category_id'

df = spark.read.parquet(input_parth)

display(df)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import *

ca_category_id = df.withColumn("id",col("id").cast("INT"))

ca_category_id.printSchema()
ca_category_id.write.mode('overwrite').parquet('/mnt/youtube-cleansed/youtubejsontoparquet/CA_category_id')



# COMMAND ----------

df2 =spark.read.parquet("/mnt/youtube-cleansed/youtubejsontoparquet/DE_category_id")
display(df2)

# COMMAND ----------

df3 = spark.read.parquet("/mnt/youtube-cleansed/youtubejsontoparquet/gbcategoryid")

display(df2)

# COMMAND ----------

GB_category_id = df2.withColumn("id",col("id").cast("INT"))

GB_category_id.printSchema()
GB_category_id.write.mode('overwrite').parquet('/mnt/youtube-cleansed/youtubejsontoparquet/GB_category_id')



# COMMAND ----------

df3 = spark.read.parquet("/mnt/youtube-cleansed/youtubejsontoparquet/uscategoryid")

display(df3)

# COMMAND ----------

US_category_id = df3.withColumn("id",col("id").cast("INT"))

US_category_id.printSchema()
US_category_id.write.mode('overwrite').parquet('/mnt/youtube-cleansed/youtubejsontoparquet/US_category_id')



# COMMAND ----------

US_category_id.printSchema()
