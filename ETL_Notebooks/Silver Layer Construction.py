# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Construction
# MAGIC
# MAGIC This layer takes the tables created by the bronze layer notebook and creates cleaned tabled for the silver layer

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, unix_timestamp, to_date, hour, explode, when, count, expr, to_number

# COMMAND ----------

# MAGIC %md
# MAGIC ## First: Clean the data from bronze_game_logs

# COMMAND ----------

silver_game_logs = (
    spark.read.table("bronze_game_logs")
    .withColumnRenamed("_player_id", "PlayerID")
    .withColumnRenamed("_session_id", "SessionID")
    .withColumnRenamed("_login", "LoginTime")
    .withColumnRenamed("_logout", "LogoutTime")
    .withColumnRenamed("_action", "Actions")
    .withColumnRenamed("_platform", "Platform")
    .withColumn("LoginTime", to_timestamp(col("LoginTime")))
    .withColumn("LogoutTime", to_timestamp(col("LogoutTime")))
    .withColumn("SessionTimeLength",
        (unix_timestamp(col("LogoutTime")) - unix_timestamp(col("LoginTime"))) / 60.0
    )
    .withColumn(
        "Platform",
        when(col("Platform") == "1", "Personal Computer")
        .when(col("Platform") == "2", "PlayStation 5")
        .when(col("Platform") == "3", "Xbox Series X")
        .when(col("Platform") == "4", "Mobile")
        .otherwise("Unknown")
    )
    .withColumn("Actions",
        expr("""
            transform(Actions, x -> 
                CASE x
                    WHEN '1' THEN 'PvP Battle'
                    WHEN '2' THEN 'Item Crafted'
                    WHEN '3' THEN 'Exploration'
                    WHEN '4' THEN 'Story'
                    ELSE 'Unknown'
                END
            )
        """)
    )
    .dropDuplicates(["SessionId"])
)

silver_game_logs.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_game_logs")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.silver_game_logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second: Clean the data from bronze_players

# COMMAND ----------

silver_players = (
    spark.read.table("bronze_players")
    .withColumnRenamed("plid", "PlayerID")
    .withColumnRenamed("country", "Country")
    .withColumnRenamed("age", "Age")
    .withColumnRenamed("acc_creat_date", "AccountCreationDate")
    .withColumnRenamed("gender", "Gender")
    .withColumn("Age", col("age").cast("int"))
    .withColumn("Age_Bin", 
        when(col("Age") < 15, "Under 15")
        .when((col("Age") >= 15) & (col("Age") < 25), "15-24")
        .when((col("Age") >= 25) & (col("Age") < 35), "25-34")
        .when((col("Age") >= 35) & (col("Age") < 45), "35-44")
        .when((col("Age") >= 45) & (col("Age") < 55), "45-54")
        .when((col("Age") >= 55) & (col("Age") < 65), "55-64")
        .when(col("Age") >= 65, "65+")
        .otherwise("Unknown")
    )
    .withColumn("Gender",
        when(col("Gender") == "1", "Male")
        .when(col("Gender") == "2", "Female")
        .when(col("Gender") == "3", "Unspecified")
        .otherwise("Unknown")
    )
    .withColumn("AccountCreationDate", to_date(col("AccountCreationDate")))
    .dropDuplicates(["PlayerID"])
)

silver_players.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_players")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.silver_players

# COMMAND ----------

# MAGIC %md
# MAGIC ## Third: Clean the data from bronze_purchases

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM bronze_purchases

# COMMAND ----------

silver_purchases = (
    spark.read.table("bronze_purchases")
    .withColumnRenamed("transaction_id", "TransactionID")
    .withColumnRenamed("player_id", "PlayerID")
    .withColumnRenamed("item", "Item")
    .withColumnRenamed("item_price", "ItemPrice")
    .withColumnRenamed("pmethod", "PaymentMethod")
    .withColumnRenamed("time", "Timestamp")
    .withColumn("Timestamp", to_timestamp(col("Timestamp")))
    .withColumn("ItemPrice", col("ItemPrice").cast("double"))
    .withColumn("Item",
        when(col("Item") == "1", "Sword of Dawn")
        .when(col("Item") == "2", "Shield of Ages")
        .when(col("Item") == "3", "Health Potion")
        .when(col("Item") == "4", "Mana Potion")
        .when(col("Item") == "5", "100 Gems")
        .when(col("Item") == "6", "Skin Pack")
        .when(col("Item") == "7", "Expansion Pass")
        .otherwise("Unknown")
    )
    .withColumn(
        "PaymentMethod",
        when(col("PaymentMethod") == "1", "Credit Card")
        .when(col("PaymentMethod") == "2", "PayPal")
        .when(col("PaymentMethod") == "3", "GiftCard")
        .otherwise("Unknown")
    )
    .dropDuplicates(["TransactionID"])
)

silver_purchases.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_purchases")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.silver_purchases