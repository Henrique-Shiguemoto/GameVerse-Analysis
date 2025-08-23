# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Construction
# MAGIC
# MAGIC This notebook constructs the layer by fetching the data needed from json and csv files, this simulates a data fetch from a server.

# COMMAND ----------

from pyspark.sql.types import IntegerType, StructType, StructField, StringType, ArrayType
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ###### This here is just to set the metadata necessary for the AutoLoader to automatically update the SQL tables correctly. This isn't particularly necessary for this project, since I don't intend to update this projects by adding more and more files. I just ran it once.

# COMMAND ----------

# dbutils.fs.mkdirs("/Volumes/workspace/default/raw/_checkpoints/game_logs")
# dbutils.fs.mkdirs("/Volumes/workspace/default/raw/_checkpoints/player_profiles")
# dbutils.fs.mkdirs("/Volumes/workspace/default/raw/_checkpoints/purchases")

# COMMAND ----------

# MAGIC %md
# MAGIC ## First: Game logs/sessions data (as JSON Format, we'll transform into an SQL table)

# COMMAND ----------

game_logs_schema = StructType([
    StructField("_player_id", StringType(), True),
    StructField("_session_id", StringType(), True),
    StructField("_login", StringType(), True),
    StructField("_logout", StringType(), True),
    StructField("_action", ArrayType(StringType()), True),
    StructField("_platform", StringType(), True),
])

bronze_game_logs = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(game_logs_schema)
    .load("/Volumes/workspace/default/raw/game_logs/")
)

bronze_game_logs.writeStream \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/Volumes/workspace/default/raw/_checkpoints/game_logs") \
    .toTable("bronze_game_logs")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.bronze_game_logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second: Getting data from Player Profiles dataset (CSV File)

# COMMAND ----------

players_schema = StructType([
    StructField("plid", StringType(), True),
    StructField("country", StringType(), True),
    StructField("age", StringType(), True),
    StructField("acc_creat_date", StringType(), True),
    StructField("gender", StringType(), True),
])

bronze_players = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(players_schema)
    .load("/Volumes/workspace/default/raw/player_profiles/")
)

bronze_players.writeStream \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/Volumes/workspace/default/raw/_checkpoints/player_profiles") \
    .toTable("bronze_players")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.bronze_players

# COMMAND ----------

# MAGIC %md
# MAGIC ## Third: Getting data from Player Purchases dataset (another CSV File), pretty much the same thing as before

# COMMAND ----------

purchases_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("item", StringType(), True),
    StructField("item_price", StringType(), True),
    StructField("pmethod", StringType(), True),
    StructField("time", StringType(), True),
])

bronze_purchases = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(purchases_schema)
    .load("/Volumes/workspace/default/raw/purchases/")
)

bronze_purchases.writeStream \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "/Volumes/workspace/default/raw/_checkpoints/purchases") \
    .toTable("bronze_purchases")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.bronze_purchases