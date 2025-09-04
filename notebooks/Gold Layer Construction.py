# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Construction
# MAGIC
# MAGIC This layer takes the tables from the silver layer tables and creates new tables which might be useful for further data analysis, such as dashboard development and report creation.

# COMMAND ----------

from pyspark.sql.functions import to_date, countDistinct, sum, count, avg, median, mode, stddev, datediff, current_date, max, min, col, explode, when, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## First: Joining game logs with player profile info

# COMMAND ----------

silver_game_logs = spark.read.table("silver_game_logs")
silver_players = spark.read.table("silver_players")

gold_players_info = silver_game_logs \
    .join(silver_players, on = "PlayerID", how = "inner") \
    .withColumn("SessionDate", to_date("LoginTime"))

gold_players_info.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_players_info")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.gold_players_info

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second: Joining player purchases with player info

# COMMAND ----------

silver_purchases = spark.read.table("silver_purchases")
silver_players = spark.read.table("silver_players")

gold_players_purchases = silver_purchases \
    .join(silver_players, on = "PlayerID", how = "inner") \
    .withColumnRenamed("Timestamp", "PaymentTimestamp")  \
    .withColumn("SessionDate", to_date("PaymentTimestamp"))

gold_players_purchases.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_players_purchases")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.gold_players_purchases

# COMMAND ----------

# MAGIC %md
# MAGIC ## Third: Creating some tables with aggregated data beforehand

# COMMAND ----------

gold_daily_metrics_1 = spark.read.table("gold_players_info"). \
    groupby("SessionDate", "Country"). \
    agg(
        countDistinct("PlayerID").alias("UniquePlayers"),
        countDistinct("SessionID").alias("UniqueSessions"),
        sum("SessionTimeLength").alias("TotalDailySessionLength"),
        avg("Age").alias("AvgPlayerAge"),
        median("Age").alias("MedianPlayerAge"),
        mode("Age").alias("ModePlayerAge"),
        stddev("Age").alias("StandardDeviationPlayerAge"),
        countDistinct("Country").alias("UniqueCountries")
    )

gold_daily_metrics_2 = spark.read.table("gold_players_purchases"). \
    groupby("SessionDate", "Country"). \
    agg(
        countDistinct("TransactionID").alias("UniqueTransactions"),
        sum("ItemPrice").alias("TotalDailyRevenue"),
        avg("ItemPrice").alias("AverageDailyItemPrice")
    )
    
gold_daily_metrics = gold_daily_metrics_1.join(gold_daily_metrics_2, on = ["SessionDate", "Country"], how = "inner")

gold_daily_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_daily_metrics")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.gold_daily_metrics

# COMMAND ----------

# Since Actions is a list column, we need to explode it so we can count each individual value per group afterwards
actions_exploded = (
    spark.read.table("silver_game_logs")
    .join(spark.read.table("silver_players").select("PlayerID", "Country").distinct(), on = "PlayerID", how = "inner")
    .select("PlayerID", "Country", explode(col("Actions")).alias("Action"))
)

actions_summary = (
    actions_exploded
    .groupBy("PlayerID", "Country")
    .agg(
        sum(when(col("Action") == "PvP Battle", 1).otherwise(0)).alias("PvP_Battle_Count"),
        sum(when(col("Action") == "Dungeon Raid", 1).otherwise(0)).alias("Dungeon_Raid_Count"),
        sum(when(col("Action") == "Crafting", 1).otherwise(0)).alias("Crafting_Count"),
        sum(when(col("Action") == "Exploration", 1).otherwise(0)).alias("Exploration_Count"),
        sum(when(col("Action") == "Trading", 1).otherwise(0)).alias("Trading_Count"),
        sum(when(col("Action") == "Gathering", 1).otherwise(0)).alias("Gathering_Count"),
        sum(when(col("Action") == "Questing / Story Progression", 1).otherwise(0)).alias("Questing_Story_Progression_Count"),
        sum(when(col("Action") == "Social Interaction", 1).otherwise(0)).alias("Social_Interaction_Count")
    )
    .withColumn(
        "FavoriteAction",
        when(
            (col("PvP_Battle_Count") >= col("Dungeon_Raid_Count")) &
            (col("PvP_Battle_Count") >= col("Crafting_Count")) &
            (col("PvP_Battle_Count") >= col("Exploration_Count")) &
            (col("PvP_Battle_Count") >= col("Trading_Count")) &
            (col("PvP_Battle_Count") >= col("Gathering_Count")) &
            (col("PvP_Battle_Count") >= col("Questing_Story_Progression_Count")) &
            (col("PvP_Battle_Count") >= col("Social_Interaction_Count")), "PvP Battle"
        ).when(
            (col("Dungeon_Raid_Count") >= col("PvP_Battle_Count")) &
            (col("Dungeon_Raid_Count") >= col("Crafting_Count")) &
            (col("Dungeon_Raid_Count") >= col("Exploration_Count")) &
            (col("Dungeon_Raid_Count") >= col("Trading_Count")) &
            (col("Dungeon_Raid_Count") >= col("Gathering_Count")) &
            (col("Dungeon_Raid_Count") >= col("Questing_Story_Progression_Count")) &
            (col("Dungeon_Raid_Count") >= col("Social_Interaction_Count")), "Dungeon Raid"
        ).when(
            (col("Crafting_Count") >= col("PvP_Battle_Count")) &
            (col("Crafting_Count") >= col("Dungeon_Raid_Count")) &
            (col("Crafting_Count") >= col("Exploration_Count")) &
            (col("Crafting_Count") >= col("Trading_Count")) &
            (col("Crafting_Count") >= col("Gathering_Count")) &
            (col("Crafting_Count") >= col("Questing_Story_Progression_Count")) &
            (col("Crafting_Count") >= col("Social_Interaction_Count")), "Crafting"
        ).when(
            (col("Exploration_Count") >= col("PvP_Battle_Count")) &
            (col("Exploration_Count") >= col("Dungeon_Raid_Count")) &
            (col("Exploration_Count") >= col("Crafting_Count")) &
            (col("Exploration_Count") >= col("Trading_Count")) &
            (col("Exploration_Count") >= col("Gathering_Count")) &
            (col("Exploration_Count") >= col("Questing_Story_Progression_Count")) &
            (col("Exploration_Count") >= col("Social_Interaction_Count")), "Exploration"
        ).when(
            (col("Trading_Count") >= col("PvP_Battle_Count")) &
            (col("Trading_Count") >= col("Dungeon_Raid_Count")) &
            (col("Trading_Count") >= col("Crafting_Count")) &
            (col("Trading_Count") >= col("Exploration_Count")) &
            (col("Trading_Count") >= col("Gathering_Count")) &
            (col("Trading_Count") >= col("Questing_Story_Progression_Count")) &
            (col("Trading_Count") >= col("Social_Interaction_Count")), "Trading"
        ).when(
            (col("Gathering_Count") >= col("PvP_Battle_Count")) &
            (col("Gathering_Count") >= col("Dungeon_Raid_Count")) &
            (col("Gathering_Count") >= col("Crafting_Count")) &
            (col("Gathering_Count") >= col("Exploration_Count")) &
            (col("Gathering_Count") >= col("Trading_Count")) &
            (col("Gathering_Count") >= col("Questing_Story_Progression_Count")) &
            (col("Gathering_Count") >= col("Social_Interaction_Count")), "Gathering"
        ).when(
            (col("Questing_Story_Progression_Count") >= col("PvP_Battle_Count")) &
            (col("Questing_Story_Progression_Count") >= col("Dungeon_Raid_Count")) &
            (col("Questing_Story_Progression_Count") >= col("Crafting_Count")) &
            (col("Questing_Story_Progression_Count") >= col("Exploration_Count")) &
            (col("Questing_Story_Progression_Count") >= col("Trading_Count")) &
            (col("Questing_Story_Progression_Count") >= col("Gathering_Count")) &
            (col("Questing_Story_Progression_Count") >= col("Social_Interaction_Count")), "Questing / Story Progression"
        ).otherwise("Social Interaction")
    )
)

gold_player_summary_1 = spark.read.table("gold_players_info"). \
    groupby("PlayerID", "Country"). \
    agg(
        countDistinct("SessionID").alias("TotalSessions"),
        avg("SessionTimeLength").alias("AvgSessionLengthMinutes"),
        sum("SessionTimeLength").alias("TotalPlayTimeMinutes"),
        max("LoginTime").alias("LastLoginDate"),
        (countDistinct("SessionID") / countDistinct("SessionDate")).alias("AvgSessionsPerDay"),
        datediff(current_date(), min("AccountCreationDate")).alias("AccountAgeDays"),
        mode("Platform").alias("PrimaryPlatform"),
        countDistinct("Platform").alias("PlatformDiversity"),
        when(
            datediff(current_date(), max("LoginTime")) <= 15,
            lit(True)
        ).otherwise(lit(False)).alias("IsActive")
    ). \
    join(actions_summary, on = ["PlayerID", "Country"], how = "inner")

gold_player_summary_2 = spark.read.table("gold_players_purchases"). \
    groupby("PlayerID", "Country"). \
    agg(
        sum("ItemPrice").alias("TotalSpent"),
        countDistinct("TransactionID").alias("NumPurchases"),
        avg("ItemPrice").alias("AvgPurchaseValue"),
        min("PaymentTimestamp").alias("FirstPurchaseDate"),
        max("PaymentTimestamp").alias("LastPurchaseDate")
    )

gold_player_summary = gold_player_summary_1.join(gold_player_summary_2, on = ["PlayerID", "Country"], how = "inner")

gold_player_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_player_summary")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.default.gold_player_summary