# Databricks notebook source
# MAGIC %md
# MAGIC # Questions to Answer:
# MAGIC ## 1 - How players engage with the game?
# MAGIC ## 2 - Which features drive retention? 
# MAGIC ## 3 - How in-game purchases affect revenue?

# COMMAND ----------

# MAGIC %md
# MAGIC ### First I will write these queries in Databricks, but then I'll hand them over to Excel through an ODBC driver connection

# COMMAND ----------

# MAGIC %md
# MAGIC 1 - How players engage with the game?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT PlayerID) AS Players FROM workspace.default.silver_players

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT PlayerID) FROM workspace.default.gold_player_summary WHERE IsActive = TRUE AND AccountAgeDays > 15

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT PlayerID) FROM workspace.default.gold_player_summary WHERE AccountAgeDays <= 15

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH result_1 AS (
# MAGIC   SELECT COUNT(DISTINCT PlayerID) AS Players FROM workspace.default.silver_players
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   Country, 
# MAGIC   COUNT(DISTINCT PlayerID) AS Players,
# MAGIC   COUNT(DISTINCT PlayerID) / (SELECT Players FROM result_1) AS PercPlayers
# MAGIC FROM
# MAGIC   workspace.default.silver_players
# MAGIC GROUP BY Country 
# MAGIC ORDER BY Players DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   FavoriteAction, ROUND(SUM(TotalPlayTimeMinutes) / 60) AS HoursPlayed 
# MAGIC FROM workspace.default.gold_player_summary 
# MAGIC GROUP BY FavoriteAction 
# MAGIC ORDER BY HoursPlayed DESC
# MAGIC
# MAGIC -- Players usually engage in cooperative or group activities such as Dungeon Raids, Social Interactions and PvP Battles
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   PrimaryPlatform, ROUND(SUM(TotalPlayTimeMinutes) / 60) AS HoursPlayed 
# MAGIC FROM workspace.default.gold_player_summary 
# MAGIC GROUP BY PrimaryPlatform 
# MAGIC ORDER BY HoursPlayed DESC
# MAGIC
# MAGIC -- Players largely prefer playing on PC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sessions_weekday AS (
# MAGIC   SELECT *, weekday(SessionDate) AS SessionWeekday FROM workspace.default.gold_players_info
# MAGIC )
# MAGIC
# MAGIC SELECT SessionWeekDay, COUNT(*) AS sessions, ROUND(SUM(SessionTimeLength) / 60) as TotalSessionTimeInHours
# MAGIC FROM sessions_weekday
# MAGIC GROUP BY SessionWeekDay
# MAGIC ORDER BY SessionWeekDay
# MAGIC
# MAGIC -- Players usually play on fridays, saturdays and sundays
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT PlayerID), 
# MAGIC   AgeBin, 
# MAGIC   Gender 
# MAGIC FROM workspace.default.gold_players_info 
# MAGIC GROUP BY AgeBin, Gender 
# MAGIC ORDER BY AgeBin, Gender

# COMMAND ----------

# MAGIC %md
# MAGIC 2 - Which features drive retention?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH result_1 AS (
# MAGIC   SELECT COUNT(DISTINCT PlayerID) FROM workspace.default.gold_player_summary WHERE AccountAgeDays > 15
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT PlayerID) AS Players,
# MAGIC   ROUND(COUNT(DISTINCT PlayerID) / (SELECT * FROM result_1), 2) AS Percentage, 
# MAGIC   IsActive
# MAGIC FROM workspace.default.gold_player_summary
# MAGIC WHERE AccountAgeDays > 15
# MAGIC GROUP BY IsActive
# MAGIC
# MAGIC -- About 76% of players which played in the last 3 months are active players (played in the last 15 days and account is older than 15 days)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH active_players AS (
# MAGIC   SELECT DISTINCT * FROM workspace.default.gold_player_summary WHERE IsActive = TRUE AND AccountAgeDays > 15
# MAGIC ),
# MAGIC
# MAGIC summary_active_players AS (
# MAGIC   SELECT 
# MAGIC     AVG(PlatformDiversity) AS AvgPlatformDiversity,
# MAGIC     SUM(PvP_Battle_Count) AS sum_PvP_Battle_Count,
# MAGIC     SUM(Dungeon_Raid_Count) AS sum_Dungeon_Raid_Count,
# MAGIC     SUM(Crafting_Count) AS sum_Crafting_Count,
# MAGIC     SUM(Exploration_Count) AS sum_Exploration_Count,
# MAGIC     SUM(Trading_Count) AS sum_Trading_Count,
# MAGIC     SUM(Gathering_Count) AS sum_Gathering_Count,
# MAGIC     SUM(Questing_Story_Progression_Count) AS sum_Questing_Story_Progression_Count,
# MAGIC     SUM(Social_Interaction_Count) AS sum_Social_Interaction_Count
# MAGIC   FROM active_players
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   sum_PvP_Battle_Count + sum_Dungeon_Raid_Count + sum_Crafting_Count + sum_Exploration_Count + sum_Trading_Count + sum_Gathering_Count + sum_Questing_Story_Progression_Count + sum_Social_Interaction_Count AS TotalActions,
# MAGIC   sum_PvP_Battle_Count / TotalActions AS perc_PvP_Battle_Count,
# MAGIC   sum_Dungeon_Raid_Count / TotalActions AS perc_Dungeon_Raid_Count,
# MAGIC   sum_Crafting_Count / TotalActions AS perc_Crafting_Count,
# MAGIC   sum_Exploration_Count / TotalActions AS perc_Exploration_Count,
# MAGIC   sum_Trading_Count / TotalActions AS perc_Trading_Count,
# MAGIC   sum_Gathering_Count / TotalActions AS perc_Gathering_Count,
# MAGIC   sum_Questing_Story_Progression_Count / TotalActions AS perc_Questing_Story_Progression_Count,
# MAGIC   sum_Social_Interaction_Count / TotalActions AS perc_Social_Interaction_Count
# MAGIC FROM summary_active_players
# MAGIC
# MAGIC -- It seems that the multiplatform aspect of the game is well used by players. Also, the ability to to Raid Dungeons, PvP Battle and Interact Socially are key features which drive retention

# COMMAND ----------

# MAGIC %md
# MAGIC 3 - How in-game purchases affect revenue?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH purchases_with_months AS (
# MAGIC   SELECT 
# MAGIC     *, 
# MAGIC     date_format(date_trunc('month', PaymentTimestamp), 'yyyy-MM-01') AS month_year
# MAGIC   FROM workspace.default.gold_players_purchases
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   SUM(ItemPrice) AS Revenue,
# MAGIC   month_year AS Month
# MAGIC FROM purchases_with_months
# MAGIC GROUP BY month_year
# MAGIC ORDER BY month_year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   PlayerID, 
# MAGIC   
# MAGIC   -- Correlation matrix where these variables are the columns
# MAGIC   TotalSessions, 
# MAGIC   AvgSessionLengthMinutes, 
# MAGIC   TotalPlayTimeMinutes, 
# MAGIC
# MAGIC   -- And these variables are the rows
# MAGIC   TotalSpent, 
# MAGIC   AvgPurchaseValue 
# MAGIC FROM workspace.default.gold_player_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   SUM(ItemPrice) AS TotalRevenue, 
# MAGIC   Item
# MAGIC FROM workspace.default.gold_players_purchases
# MAGIC GROUP BY Item 
# MAGIC ORDER BY SUM(ItemPrice) DESC
# MAGIC
# MAGIC -- Seems like cosmetics are high value items in terms of revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   COUNT(DISTINCT TransactionID), 
# MAGIC   PaymentMethod
# MAGIC FROM workspace.default.gold_players_purchases
# MAGIC GROUP	BY PaymentMethod
# MAGIC
# MAGIC -- Credit Card is the dominant method of payment
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   SessionDate, 
# MAGIC   SUM(UniqueTransactions) AS UniqueTransactions, 
# MAGIC   SUM(TotalDailyRevenue) AS TotalDailyRevenue, 
# MAGIC   SUM(AverageDailyItemPrice) AS AverageDailyItemPrice
# MAGIC FROM workspace.default.gold_daily_metrics 
# MAGIC GROUP BY SessionDate
# MAGIC ORDER BY SessionDate