# Databricks notebook source
# MAGIC %md
# MAGIC # Read data from Kinaxis - PS ACL Cluster

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd

# COMMAND ----------

# Here we can change the scenarios to comparison
# Path to optimized scenario from Wahupa - iteration 1
# kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_service.xlsx")
# Path to calibration scenario from Wahupa - iteration 2
# kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_onhand.xlsx")
# Path to scenario with direct demand locked and historical Forecast Error
kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_onhand_direct_locked_Forecast_Error.xlsx")
# Path to scenario with direct demand locked and Demand Uncertainty
# kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_onhand_direct_locked_Demand_Uncertainty.xlsx")

# COMMAND ----------

kinaxis_df.head()

# COMMAND ----------

## Convert into Spark DataFrame
spark_df = spark.createDataFrame(kinaxis_df)

spark_df = spark_df.select([col(c).alias(
        c.replace( '(', '')
        .replace( ')', '')
        .replace( ',', '')
        .replace( ';', '')
        .replace( '{', '')
        .replace( '}', '')
        .replace( '\n', '')
        .replace( '\t', '')
        .replace( ' ', '_')
        .replace( '$','USD_')
    ) for c in spark_df.columns])

## Write Frame out as Table
spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("userdb_jaworska_ka.kinaxis_results")

# COMMAND ----------

converter_sku_to_bu_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/SU_converter.xlsx")

# COMMAND ----------

## Convert into Spark DataFrame
spark_df = spark.createDataFrame(converter_sku_to_bu_df)

spark_df = spark_df.select([col(c).alias(
        c.replace( '(', '')
        .replace( ')', '')
        .replace( ',', '')
        .replace( ';', '')
        .replace( '{', '')
        .replace( '}', '')
        .replace( '\n', '')
        .replace( '\t', '')
        .replace( ' ', '_')
        .replace( '$','USD_')
    ) for c in spark_df.columns])

## Write Frame out as Table
spark_df.write.mode("overwrite").saveAsTable("userdb_jaworska_ka.converter_sku_to_bu")

# COMMAND ----------

ibpi_cov_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/CoV_22_05_2023.xlsx")

# COMMAND ----------

## Convert into Spark DataFrame
spark_df = spark.createDataFrame(ibpi_cov_df)

spark_df = spark_df.select([col(c).alias(
        c.replace( '(', '')
        .replace( ')', '')
        .replace( ',', '')
        .replace( ';', '')
        .replace( '{', '')
        .replace( '}', '')
        .replace( '\n', '')
        .replace( '\t', '')
        .replace( ' ', '_')
        .replace( '$','USD_')
    ) for c in spark_df.columns])

## Write Frame out as Table
spark_df.write.mode("overwrite").saveAsTable("userdb_jaworska_ka.ibpi_cov")

# COMMAND ----------

ibpi_service_level_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/average_SL_22_05_2023.xlsx")

# COMMAND ----------

## Convert into Spark DataFrame
spark_df = spark.createDataFrame(ibpi_service_level_df)

spark_df = spark_df.select([col(c).alias(
        c.replace( '(', '')
        .replace( ')', '')
        .replace( ',', '')
        .replace( ';', '')
        .replace( '{', '')
        .replace( '}', '')
        .replace( '\n', '')
        .replace( '\t', '')
        .replace( ' ', '_')
        .replace( '$','USD_')
    ) for c in spark_df.columns])

## Write Frame out as Table
spark_df.write.mode("overwrite").saveAsTable("userdb_jaworska_ka.ibpi_service_level")

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine Kinaxis data with IBPi data - MO-EU TAC Cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(KEYFIGUREDATE)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.tb_final_ibpi_with_22_may_2023

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE userdb_jaworska_ka.ibpi_vs_kinaxis_comparison AS
# MAGIC SELECT
# MAGIC   k.*,
# MAGIC   i.*,
# MAGIC   cv.CV,
# MAGIC   sl.Service_Level,
# MAGIC   k.Safety_StockDays AS Wahupa_SS_Days,
# MAGIC   i.IOSAFETYSTOCKDAYSOFSUPPLY AS IBPI_Reco_SS_Days,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       i.IOSAFETYSTOCKDAYSOFSUPPLY = 0
# MAGIC       AND k.Safety_StockDays IS NOT NULL
# MAGIC       AND k.Safety_StockDays <> 0
# MAGIC     ) THEN ((k.Safety_StockDays - 0.01) / 0.01) * 100
# MAGIC     WHEN (
# MAGIC       i.IOSAFETYSTOCKDAYSOFSUPPLY = 0
# MAGIC       AND k.Safety_StockDays = 0
# MAGIC     ) THEN 0
# MAGIC     ELSE (
# MAGIC       (k.Safety_StockDays - i.IOSAFETYSTOCKDAYSOFSUPPLY) / i.IOSAFETYSTOCKDAYSOFSUPPLY
# MAGIC     ) * 100
# MAGIC   END AS SS_Reco_Days_diff,
# MAGIC   i.ZFINALSAFETYDAYSOFSUPPLY AS IBPI_Final_SS_Days,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       i.ZFINALSAFETYDAYSOFSUPPLY = 0
# MAGIC       AND k.Safety_StockDays IS NOT NULL
# MAGIC       AND k.Safety_StockDays <> 0
# MAGIC     ) THEN ((k.Safety_StockDays - 0.01) / 0.01) * 100
# MAGIC     WHEN (
# MAGIC       i.ZFINALSAFETYDAYSOFSUPPLY = 0
# MAGIC       AND k.Safety_StockDays = 0
# MAGIC     ) THEN 0
# MAGIC     ELSE (
# MAGIC       (k.Safety_StockDays - i.ZFINALSAFETYDAYSOFSUPPLY) / i.ZFINALSAFETYDAYSOFSUPPLY
# MAGIC     ) * 100
# MAGIC   END AS SS_Final_Days_diff,
# MAGIC   k.Safety_StockUnits AS Wahupa_SS_Units,
# MAGIC   k.Safety_StockUnits * c.converter_factor AS Wahupa_SS_SU,
# MAGIC   i.ZRECOMMENDEDSAFETYSTOCK / c.converter_factor AS IBPI_Reco_SS_Units,
# MAGIC   i.ZRECOMMENDEDSAFETYSTOCK AS IBPI_Reco_SS_SU,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       i.ZRECOMMENDEDSAFETYSTOCK / c.converter_factor = 0
# MAGIC       AND k.Safety_StockUnits IS NOT NULL
# MAGIC       AND k.Safety_StockUnits <> 0
# MAGIC     ) THEN ((k.Safety_StockUnits - 0.01) / 0.01) * 100
# MAGIC     WHEN (
# MAGIC       i.ZRECOMMENDEDSAFETYSTOCK / c.converter_factor = 0
# MAGIC       AND k.Safety_StockUnits = 0
# MAGIC     ) THEN 0
# MAGIC     ELSE (
# MAGIC       (
# MAGIC         k.Safety_StockUnits -(i.ZRECOMMENDEDSAFETYSTOCK / c.converter_factor)
# MAGIC       ) /(i.ZRECOMMENDEDSAFETYSTOCK / c.converter_factor)
# MAGIC     ) * 100
# MAGIC   END AS SS_Reco_Units_diff,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       i.ZRECOMMENDEDSAFETYSTOCK = 0
# MAGIC       AND k.Safety_StockUnits IS NOT NULL
# MAGIC       AND k.Safety_StockUnits * c.converter_factor <> 0
# MAGIC     ) THEN (
# MAGIC       (
# MAGIC         (k.Safety_StockUnits * c.converter_factor) - 0.01
# MAGIC       ) / 0.01
# MAGIC     ) * 100
# MAGIC     WHEN (
# MAGIC       i.ZRECOMMENDEDSAFETYSTOCK = 0
# MAGIC       AND k.Safety_StockUnits * c.converter_factor = 0
# MAGIC     ) THEN 0
# MAGIC     ELSE (
# MAGIC       (
# MAGIC         (
# MAGIC           (k.Safety_StockUnits * c.converter_factor) - i.ZRECOMMENDEDSAFETYSTOCK
# MAGIC         )
# MAGIC       ) / i.ZRECOMMENDEDSAFETYSTOCK
# MAGIC     ) * 100
# MAGIC   END AS SS_Reco_SU_diff,
# MAGIC   i.FINALIOSAFETYSTOCK / c.converter_factor AS IBPI_Final_SS_Units,
# MAGIC   i.FINALIOSAFETYSTOCK AS IBPI_Final_SS_SU,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       i.FINALIOSAFETYSTOCK / c.converter_factor = 0
# MAGIC       AND k.Safety_StockUnits IS NOT NULL
# MAGIC       AND k.Safety_StockUnits <> 0
# MAGIC     ) THEN ((k.Safety_StockUnits - 0.01) / 0.01) * 100
# MAGIC     WHEN (
# MAGIC       i.FINALIOSAFETYSTOCK / c.converter_factor = 0
# MAGIC       AND k.Safety_StockUnits = 0
# MAGIC     ) THEN 0
# MAGIC     ELSE (
# MAGIC       (
# MAGIC         k.Safety_StockUnits -(i.FINALIOSAFETYSTOCK / c.converter_factor)
# MAGIC       ) /(i.FINALIOSAFETYSTOCK / c.converter_factor)
# MAGIC     ) * 100
# MAGIC   END AS SS_Final_Units_diff,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       i.FINALIOSAFETYSTOCK = 0
# MAGIC       AND k.Safety_StockUnits IS NOT NULL
# MAGIC       AND k.Safety_StockUnits * c.converter_factor <> 0
# MAGIC     ) THEN (
# MAGIC       (
# MAGIC         (k.Safety_StockUnits * c.converter_factor) - 0.01
# MAGIC       ) / 0.01
# MAGIC     ) * 100
# MAGIC     WHEN (
# MAGIC       i.FINALIOSAFETYSTOCK = 0
# MAGIC       AND k.Safety_StockUnits * c.converter_factor = 0
# MAGIC     ) THEN 0
# MAGIC     ELSE (
# MAGIC       (
# MAGIC         (
# MAGIC           (k.Safety_StockUnits * c.converter_factor) - i.FINALIOSAFETYSTOCK
# MAGIC         )
# MAGIC       ) / i.FINALIOSAFETYSTOCK
# MAGIC     ) * 100
# MAGIC   END AS SS_Final_SU_diff,
# MAGIC   (
# MAGIC     i.SU_Situational_Safety_Brown_B + i.SU_Situational_Cycle_C
# MAGIC   ) / i.SU_Total_Plant_Stock_T AS Parameter_Cycle_to_Total_Ratio,
# MAGIC   (
# MAGIC     (
# MAGIC       (
# MAGIC         i.SU_Situational_Safety_Brown_B + i.SU_Situational_Cycle_C
# MAGIC       ) / i.SU_Total_Plant_Stock_T
# MAGIC     ) + 1
# MAGIC   ) * (k.Safety_StockUnits * c.converter_factor) AS Wahupa_SS_SU_including_Parameter_Cycle,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       i.FINALIOSAFETYSTOCK = 0
# MAGIC       AND k.Safety_StockUnits IS NOT NULL
# MAGIC       AND (
# MAGIC         (
# MAGIC           (
# MAGIC             i.SU_Situational_Safety_Brown_B + i.SU_Situational_Cycle_C
# MAGIC           ) / i.SU_Total_Plant_Stock_T
# MAGIC         ) + 1
# MAGIC       ) * (k.Safety_StockUnits * c.converter_factor) <> 0
# MAGIC     ) THEN (
# MAGIC       (
# MAGIC         (
# MAGIC           (
# MAGIC             (
# MAGIC               (
# MAGIC                 i.SU_Situational_Safety_Brown_B + i.SU_Situational_Cycle_C
# MAGIC               ) / i.SU_Total_Plant_Stock_T
# MAGIC             ) + 1
# MAGIC           ) * (k.Safety_StockUnits * c.converter_factor)
# MAGIC         ) - 0.01
# MAGIC       ) / 0.01
# MAGIC     ) * 100
# MAGIC     WHEN (
# MAGIC       i.FINALIOSAFETYSTOCK = 0
# MAGIC       AND (
# MAGIC         (
# MAGIC           (
# MAGIC             i.SU_Situational_Safety_Brown_B + i.SU_Situational_Cycle_C
# MAGIC           ) / i.SU_Total_Plant_Stock_T
# MAGIC         ) + 1
# MAGIC       ) * (k.Safety_StockUnits * c.converter_factor) = 0
# MAGIC     ) THEN 0
# MAGIC     ELSE (
# MAGIC       (
# MAGIC         (
# MAGIC           (
# MAGIC             (
# MAGIC               (
# MAGIC                 (
# MAGIC                   i.SU_Situational_Safety_Brown_B + i.SU_Situational_Cycle_C
# MAGIC                 ) / i.SU_Total_Plant_Stock_T
# MAGIC               ) + 1
# MAGIC             ) * (k.Safety_StockUnits * c.converter_factor)
# MAGIC           ) - i.FINALIOSAFETYSTOCK
# MAGIC         )
# MAGIC       ) / i.FINALIOSAFETYSTOCK
# MAGIC     ) * 100
# MAGIC   END AS SS_Final_SU_diff_including_Parameter_Cycle
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.kinaxis_results k
# MAGIC   LEFT JOIN userdb_jaworska_ka.tb_final_ibpi_with_22_may_2023 i ON concat(k.PartName, k.PartSite, '_', '22May2023') = concat(
# MAGIC     i.PRDID,
# MAGIC     i.LOCID,
# MAGIC     '_',
# MAGIC     DAY(i.KEYFIGUREDATE),
# MAGIC     date_format(i.KEYFIGUREDATE, 'MMMM'),
# MAGIC     YEAR(i.KEYFIGUREDATE)
# MAGIC   )
# MAGIC   LEFT JOIN userdb_jaworska_ka.converter_sku_to_bu c ON i.PRDID = c.Material
# MAGIC   LEFT JOIN userdb_jaworska_ka.ibpi_cov cv ON (
# MAGIC     k.PartName = cv.PRDID
# MAGIC     AND k.PartSite = cv.LOCID
# MAGIC   )
# MAGIC   LEFT JOIN userdb_jaworska_ka.ibpi_service_level sl ON (
# MAGIC     k.PartName = sl.PRDID
# MAGIC     AND k.PartSite = sl.LOCID
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All matlocs for Baby Care subsector
# MAGIC SELECT COUNT(*) FROM userdb_jaworska_ka.tb_final_ibpi_with_22_may_2023 i WHERE i.KEYFIGUREDATE = '2023-05-22' AND i.PGSUBSECTOR = 'BABYCARE' AND i.FINALIOSAFETYSTOCK IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Number of all examples provided by Wahupa Kinaxis
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Examples that are in Wahupa Kinaxis analysis but not in IBPI snapshot
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k
# MAGIC WHERE k.PRDID IS NULL AND k.LOCID IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE userdb_jaworska_ka.ibpi_vs_kinaxis_comparison;

# COMMAND ----------

# MAGIC %md
# MAGIC Filter out examples where we don't have IBPi information Final Safety Stock values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k
# MAGIC WHERE
# MAGIC   k.IBPI_Final_SS_SU IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.existing_in_both_datasets AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k
# MAGIC WHERE
# MAGIC   k.IBPI_Final_SS_SU IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.existing_in_both_datasets;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total number of Units, SU, Days and USD for both solutions
# MAGIC SELECT
# MAGIC   sum(Wahupa_SS_Units),
# MAGIC   sum(IBPI_Reco_SS_Units),
# MAGIC   sum(IBPI_Final_SS_Units),
# MAGIC   sum(Wahupa_SS_SU),
# MAGIC   sum(IBPI_Reco_SS_SU),
# MAGIC   sum(IBPI_Final_SS_SU),
# MAGIC   sum(Wahupa_SS_Days),
# MAGIC   sum(IBPI_Reco_SS_Days),
# MAGIC   sum(IBPI_Final_SS_Days),
# MAGIC   sum(Current_On_HandCost),
# MAGIC   sum(FINALIOSAFETYSTOCKVAL)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.existing_in_both_datasets

# COMMAND ----------

# MAGIC %md
# MAGIC # Histograms

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

# ibpi_vs_kinaxis_comparison_df = spark.read.table("hive_metastore.userdb_jaworska_ka.ibpi_vs_kinaxis_comparison")
ibpi_vs_kinaxis_comparison_df = spark.read.table("hive_metastore.userdb_jaworska_ka.existing_in_both_datasets")

# COMMAND ----------

ibpi_vs_kinaxis_comparison_pd_df = ibpi_vs_kinaxis_comparison_df.toPandas()

# COMMAND ----------

# Variables
lower_range = -500
upper_range = 500
number_of_bins = 20

# COMMAND ----------

def prepare_data(data, lower_range, upper_range, column_name):
  df = data[[column_name]]
  df_between_range = df[df[column_name].between(lower_range, upper_range)]
  return df_between_range

# COMMAND ----------

final_units_diff_df_between_range = prepare_data(ibpi_vs_kinaxis_comparison_pd_df, lower_range, upper_range, "SS_Final_SU_diff")
reco_units_diff_df_between_range = prepare_data(ibpi_vs_kinaxis_comparison_pd_df, lower_range, upper_range, "SS_Reco_SU_diff")
final_days_diff_df_between_range = prepare_data(ibpi_vs_kinaxis_comparison_pd_df, lower_range, upper_range, "SS_Final_Days_diff")
reco_days_diff_df_between_range = prepare_data(ibpi_vs_kinaxis_comparison_pd_df, lower_range, upper_range, "SS_Reco_Days_diff")
final_SU_diff_including_Parameter_Cycle_df_between_range = prepare_data(ibpi_vs_kinaxis_comparison_pd_df, lower_range, upper_range, "SS_Final_SU_diff_including_Parameter_Cycle")

# COMMAND ----------

def histogram(lower_range, upper_range, bins_number, pd_data, title):
  xmin = lower_range
  xmax = upper_range
  num_bins = bins_number
  fig = plt.figure(figsize = (8,8))
  ax = fig.gca()
  ax.set_xlim([xmin,xmax])
  n, bins, patches = plt.hist(pd_data, bins=num_bins)

  # Add numbers on the top of each bar
  for i in range(num_bins):
      if n[i] > 0:
          plt.text(bins[i], n[i] + 30, str(int(n[i])), ha='left')
  plt.title(title,fontsize=15)
  plt.show()

# COMMAND ----------

histogram(lower_range, upper_range, number_of_bins, final_units_diff_df_between_range, "SS Final SU difference")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.final AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k
# MAGIC WHERE
# MAGIC   k.IBPI_Final_SS_Units IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- how many examples from Wahupa site are within 20% error regarding to IBPI
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final i
# MAGIC WHERE
# MAGIC   i.SS_Final_SU_diff BETWEEN -20
# MAGIC   AND 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- how many examples from Wahupa site proposed smaller safety stock than IBPI
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final i
# MAGIC WHERE
# MAGIC   i.SS_Final_SU_diff < 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract only non-zero values from Wahupa
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.final_excl_zeros_from_Wahupa AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k
# MAGIC WHERE
# MAGIC   (
# MAGIC     k.IBPI_Reco_SS_Units IS NOT NULL
# MAGIC     AND k.Wahupa_SS_SU <> 0
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count how many examples are non-zero from Wahupa proposal
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final_excl_zeros_from_Wahupa;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count how many examples frome Wahupa are withing 20% error regading to IBPI but only for non-zero Wahupa proposals
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final_excl_zeros_from_Wahupa i
# MAGIC WHERE
# MAGIC   i.SS_Final_SU_diff BETWEEN -20
# MAGIC   AND 20;

# COMMAND ----------

# MAGIC %md
# MAGIC Histograms

# COMMAND ----------

histogram(lower_range, upper_range, number_of_bins, final_units_diff_df_between_range, "SS Final SU difference")

# COMMAND ----------

# TO DO: check if that makes sense with Niels!
histogram(lower_range, upper_range, number_of_bins, final_SU_diff_including_Parameter_Cycle_df_between_range, "SS Final SU difference including Parameter Cycle")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final i
# MAGIC WHERE
# MAGIC   i.SS_Final_SU_diff_including_Parameter_Cycle BETWEEN -20
# MAGIC   AND 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final_excl_zeros_from_Wahupa i
# MAGIC WHERE
# MAGIC   i.SS_Final_SU_diff_including_Parameter_Cycle BETWEEN -20
# MAGIC   AND 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final_excl_zeros_from_Wahupa i
# MAGIC WHERE
# MAGIC   i.SS_Final_SU_diff_including_Parameter_Cycle > 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final_excl_zeros_from_Wahupa i
# MAGIC WHERE
# MAGIC   i.SS_Final_SU_diff > 0;

# COMMAND ----------

histogram(lower_range, upper_range, number_of_bins, reco_units_diff_df_between_range, "SS Reco SU difference")

# COMMAND ----------

histogram(lower_range, upper_range, number_of_bins, final_days_diff_df_between_range, "SS Final Days difference")

# COMMAND ----------

histogram(lower_range, upper_range, number_of_bins, reco_days_diff_df_between_range, "SS Reco Days difference")

# COMMAND ----------

# Histogram dla CoV values
cv_lower_range = 0.1
cv_upper_range = 1.6
cv_number_of_bins = 10
cv_data = prepare_data(ibpi_vs_kinaxis_comparison_pd_df, cv_lower_range, cv_upper_range, "CV")
histogram(cv_lower_range, cv_upper_range, cv_number_of_bins, cv_data, "CoV Histogram")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Review

# COMMAND ----------

# MAGIC %md
# MAGIC Comparison between Wahupa and Reco IBPI

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.reco AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k
# MAGIC WHERE
# MAGIC   k.IBPI_Reco_SS_Units IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Non-zero values for Wahupa
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.reco k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Zero values for Wahupa and non-zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.reco k
# MAGIC WHERE
# MAGIC   k.Wahupa_SS_SU = 0
# MAGIC   AND k.IBPI_Reco_SS_SU <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Zero values for Wahupa and zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.reco k
# MAGIC WHERE
# MAGIC   k.Wahupa_SS_SU = 0
# MAGIC   AND k.IBPI_Reco_SS_SU = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Non-zero values for Wahupa and zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.reco k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits <> 0
# MAGIC   AND k.IBPI_Reco_SS_Units = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Non-zero values for Wahupa and non-zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.reco k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits <> 0
# MAGIC   AND k.IBPI_Reco_SS_Units <> 0

# COMMAND ----------

# MAGIC %md
# MAGIC Comparison between Wahupa and Final IBPI

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final
# MAGIC WHERE
# MAGIC   Wahupa_SS_SU <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   sum(Wahupa_SS_SU),
# MAGIC   sum(IBPI_Reco_SS_SU),
# MAGIC   sum(IBPI_Final_SS_SU)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final
# MAGIC WHERE
# MAGIC   Wahupa_SS_SU <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See how much the individual safety buckets are for examples from IBPI
# MAGIC SELECT
# MAGIC   sum(SU_Total_Plant_Stock_T) AS All_Total_Stock_SU,
# MAGIC   sum(SU_Situational_Safety_Brown_B) AS All_Situational_Safety_SU,
# MAGIC   sum(SU_Situational_Cycle_C) AS All_Situational_Cycle_SU,
# MAGIC   sum(SU_Situational_Safety_Brown_B) + sum(SU_Situational_Cycle_C) AS Situational_Safety_and_Situational_Cycle_SU,
# MAGIC   sum(SU_Parameter_Driven_Cycle_P) AS Parameter_Driven_Cycle_SU,
# MAGIC   sum(SU_Productive_Safety_S) AS All_Productive_Safety_SU,
# MAGIC   sum(SU_Red_R),
# MAGIC   sum(SU_Orange_O),
# MAGIC   sum(SU_Anticipation_A),
# MAGIC   sum(SU_Frozen_In_Transit_F),
# MAGIC   sum(SU_Frozen_QI_Q),
# MAGIC   sum(SU_Amber_M),
# MAGIC   sum(SU_SND_Excess_D),
# MAGIC   sum(SU_Situational_Safety_Brown_B),
# MAGIC   sum(SU_Launch_Excess_L),
# MAGIC   sum(SU_Productive_Safety_S),
# MAGIC   sum(SU_Excess_Y),
# MAGIC   sum(SU_Parameter_Driven_Cycle_P),
# MAGIC   sum(SU_Situational_Cycle_C),
# MAGIC   sum(SU_Unknown_U)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final
# MAGIC WHERE
# MAGIC   Wahupa_SS_SU <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Non-zero values for Wahupa
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Zero values for Wahupa and non-zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits = 0
# MAGIC   AND k.IBPI_Final_SS_Units <> 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Zero values for Wahupa and zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits = 0
# MAGIC   AND k.IBPI_Final_SS_Units = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Non-zero values for Wahupa and zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits <> 0
# MAGIC   AND k.IBPI_Final_SS_Units = 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Non-zero values for Wahupa and non-zero values for IBPi
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final k
# MAGIC WHERE
# MAGIC   k.Safety_StockUnits <> 0
# MAGIC   AND k.IBPI_Final_SS_Units <> 0

# COMMAND ----------

# MAGIC %md
# MAGIC # Safety Buckets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check in all buckets add up to Total Stock
# MAGIC SELECT
# MAGIC   sum(SU_Red_R) + sum(SU_Orange_O) + sum(SU_Anticipation_A) + sum(SU_Frozen_In_Transit_F) + sum(SU_Frozen_QI_Q) + sum(SU_Amber_M) + sum(SU_SND_Excess_D) + sum(SU_Situational_Safety_Brown_B) + sum(SU_Launch_Excess_L) + sum(SU_Productive_Safety_S) + sum(SU_Excess_Y) + sum(SU_Parameter_Driven_Cycle_P) + sum(SU_Situational_Cycle_C) + sum(SU_Unknown_U) AS sum_all_buckets,
# MAGIC   sum(SU_Total_Plant_Stock_T) AS total_stock
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   sum(SU_Total_Plant_Stock_T) AS All_Total_Stock_SU,
# MAGIC   sum(SU_Situational_Safety_Brown_B) AS All_Situational_Safety_SU,
# MAGIC   sum(SU_Situational_Cycle_C) AS All_Situational_Cycle_SU,
# MAGIC   sum(SU_Situational_Safety_Brown_B) + sum(SU_Situational_Cycle_C) AS Situational_Safety_and_Situational_Cycle_SU,
# MAGIC   sum(SU_Parameter_Driven_Cycle_P) AS Parameter_Driven_Cycle_SU,
# MAGIC   sum(SU_Productive_Safety_S) AS All_Productive_Safety_SU,
# MAGIC   sum(SU_Red_R),
# MAGIC   sum(SU_Orange_O),
# MAGIC   sum(SU_Anticipation_A),
# MAGIC   sum(SU_Frozen_In_Transit_F),
# MAGIC   sum(SU_Frozen_QI_Q),
# MAGIC   sum(SU_Amber_M),
# MAGIC   sum(SU_SND_Excess_D),
# MAGIC   sum(SU_Situational_Safety_Brown_B),
# MAGIC   sum(SU_Launch_Excess_L),
# MAGIC   sum(SU_Productive_Safety_S),
# MAGIC   sum(SU_Excess_Y),
# MAGIC   sum(SU_Parameter_Driven_Cycle_P),
# MAGIC   sum(SU_Situational_Cycle_C),
# MAGIC   sum(SU_Unknown_U)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   sum(Wahupa_SS_Units),
# MAGIC   sum(IBPI_Reco_SS_Units),
# MAGIC   sum(IBPI_Final_SS_Units)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final

# COMMAND ----------

# MAGIC %md
# MAGIC # Create comments for all examples

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.final AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.ibpi_vs_kinaxis_comparison k
# MAGIC WHERE
# MAGIC   k.IBPI_Final_SS_SU IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final k

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE userdb_jaworska_ka.ibpi_vs_kinaxis_comparison;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.prepare_to_comment AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN f.IBPI_Final_SS_SU <> 0 THEN (
# MAGIC       f.SU_Situational_Cycle_C + f.SU_Situational_Safety_Brown_B
# MAGIC     ) / f.IBPI_Final_SS_SU
# MAGIC     ELSE 0
# MAGIC   END AS Parameter_Cycle_to_Final_IBPI_Safety_Ratio,
# MAGIC   CASE
# MAGIC     WHEN f.Wahupa_SS_SU > f.IBPI_Final_SS_SU THEN 'YES'
# MAGIC     ELSE 'NO'
# MAGIC   END AS Wahupa_SS_SU_higher_than_IBPI_SS_SU,
# MAGIC   CASE
# MAGIC     WHEN f.CFR IS NOT NULL
# MAGIC     AND f.CFR > f.Service_Level THEN 'YES'
# MAGIC     WHEN f.CFR IS NOT NULL
# MAGIC     AND f.CFR <= f.Service_Level THEN 'NO'
# MAGIC     ELSE 'no info about CFR'
# MAGIC   END AS CFR_on_Target,
# MAGIC   CASE
# MAGIC     WHEN f.Daily_Demandweek_CoV >= f.CV THEN 'YES'
# MAGIC     ELSE 'NO'
# MAGIC   END AS Wahupa_CoV_higher_or_equal_to_IBPI_CoV,
# MAGIC   f.Daily_Demandweek_CoV AS Wahupa_CoV,
# MAGIC   f.ZFINALSAFETYDAYSOFSUPPLY AS IBPI_SS_Days,
# MAGIC   f.PGTRANSITIONTYPE AS Transition_Type,
# MAGIC   1 AS ID_FOR_FINAL
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final f

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.final_comparison AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU = 0 THEN 'Both solutions proposed the same ZERO results'
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days <= 3
# MAGIC     AND p.Transition_Type IS NOT NULL THEN 'Potentially low business risk caused by lack of TPSS'
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days <= 3
# MAGIC     AND p.Transition_Type IS NULL THEN 'Potentially medium business risk, maybe caused by Wahupa MIX'
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 3
# MAGIC     AND p.IBPI_Final_SS_Days <= 7
# MAGIC     AND p.Transition_Type IS NOT NULL THEN 'Potentially medium or high business risk caused by lack of TPSS'
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 3
# MAGIC     AND p.IBPI_Final_SS_Days <= 7
# MAGIC     AND p.Transition_Type IS NULL
# MAGIC     AND (
# MAGIC       p.Wahupa_CoV <= 0.5
# MAGIC       OR p.Wahupa_CoV IS NULL
# MAGIC     ) THEN 'Lack of safety causing medium business risk, help needed why CoV is low or not calculated'
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 3
# MAGIC     AND p.IBPI_Final_SS_Days <= 7
# MAGIC     AND p.Transition_Type IS NULL
# MAGIC     AND p.Wahupa_CoV > 0.5 THEN 'Lack od safety causing medium business risk, help needed to understand why no safety'
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 7
# MAGIC     AND (
# MAGIC       p.Wahupa_CoV <= 0.5
# MAGIC       OR p.Wahupa_CoV IS NULL
# MAGIC     ) THEN 'Lack of safety causing high business risk, help needed why CoV is low or not calculated'
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 7
# MAGIC     AND p.Wahupa_CoV > 0.5 THEN 'Lack of safety causing high business risk, help needed to understand why no safety'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.2
# MAGIC     AND p.SS_Final_SU_diff <= 20 THEN 'Wahupa recommendations in line with expectation'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.2
# MAGIC     AND p.SS_Final_SU_diff > 20
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'NO' THEN 'Wahupa results not consistent, to be verified'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.2
# MAGIC     AND p.SS_Final_SU_diff > 20
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES' THEN 'Wahupa SS is higher than IBPI, may be caused by differences in CoV calculations'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.2
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'YES' THEN 'Wahupa recommendations are too high'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.2
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'NO' THEN 'Wahupa recommendations in line with expectation'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.2
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'no info about CFR' THEN 'NO INFO ABOUT CFR'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4 THEN 'We do not follow safety stock policies, which can affect safety calculations in theory, Wahupa recommendations might be correct'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'NO' THEN 'Wahupa SS is lower than IBPI, may be caused by differences in CoV calculations'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4
# MAGIC     AND p.CFR_on_Target = 'YES' THEN 'IBPI safety potentially to high and Wahupa recommendations may meet business needs'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4
# MAGIC     AND p.CFR_on_Target = 'NO' THEN 'Results inconclusive as their might be a supply chain planning issue'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4
# MAGIC     AND p.CFR_on_Target = 'no info about CFR' THEN 'NO INFO ABOUT CFR'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'YES' THEN 'Potentially high risk for business, Wahupa recommendations potentially too low'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'NO' THEN 'High risk for business, Wahupa recommendations  too low'
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'no info about CFR' THEN 'NO INFO ABOUT CFR'
# MAGIC     ELSE 'Not assigned'
# MAGIC   END AS Comment,
# MAGIC   CASE
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU = 0 THEN 1
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days <= 3
# MAGIC     AND p.Transition_Type <> 'null' THEN 2
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days <= 3
# MAGIC     AND p.Transition_Type IS NULL THEN 3
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 3
# MAGIC     AND p.IBPI_Final_SS_Days <= 7
# MAGIC     AND p.Transition_Type <> 'null' THEN 4
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 3
# MAGIC     AND p.IBPI_Final_SS_Days <= 7
# MAGIC     AND p.Transition_Type IS NULL
# MAGIC     AND (
# MAGIC       p.Wahupa_CoV <= 0.5
# MAGIC       OR p.Wahupa_CoV IS NULL
# MAGIC     ) THEN 5
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 3
# MAGIC     AND p.IBPI_Final_SS_Days <= 7
# MAGIC     AND p.Transition_Type IS NULL
# MAGIC     AND p.Wahupa_CoV > 0.5 THEN 6
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 7
# MAGIC     AND (
# MAGIC       p.Wahupa_CoV <= 0.5
# MAGIC       OR p.Wahupa_CoV IS NULL
# MAGIC     ) THEN 7
# MAGIC     WHEN p.Wahupa_SS_SU = 0
# MAGIC     AND p.IBPI_Final_SS_SU <> 0
# MAGIC     AND p.IBPI_Final_SS_Days > 7
# MAGIC     AND p.Wahupa_CoV > 0.5 THEN 8
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.2
# MAGIC     AND p.SS_Final_SU_diff <= 20 THEN 9
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.2
# MAGIC     AND p.SS_Final_SU_diff > 20
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'NO' THEN 10
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.2
# MAGIC     AND p.SS_Final_SU_diff > 20
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES' THEN 11
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.2
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'YES' THEN 12
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.2
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'NO' THEN 13
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.2
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'no info about CFR' THEN 20
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4 THEN 14
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'NO' THEN 15
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4
# MAGIC     AND p.CFR_on_Target = 'YES' THEN 16
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4
# MAGIC     AND p.CFR_on_Target = 'NO' THEN 17
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio > 0.4
# MAGIC     AND p.CFR_on_Target = 'no info about CFR' THEN 20
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'YES' THEN 18
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'NO' THEN 19
# MAGIC     WHEN p.Wahupa_SS_SU <> 0
# MAGIC     AND p.Wahupa_SS_SU_higher_than_IBPI_SS_SU = 'NO'
# MAGIC     AND p.Wahupa_CoV_higher_or_equal_to_IBPI_CoV = 'YES'
# MAGIC     AND p.Parameter_Cycle_to_Final_IBPI_Safety_Ratio <= 0.4
# MAGIC     AND p.CFR_on_Target = 'no info about CFR' THEN 20
# MAGIC     ELSE 21
# MAGIC   END AS Comment_number
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.prepare_to_comment p

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM userdb_jaworska_ka.final_comparison;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create summary for the comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.number_of_total_examples AS
# MAGIC SELECT
# MAGIC   COUNT(*) AS Number_of_total_examples,
# MAGIC   1 AS ID_FOR_TOTAL
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final f;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.sum_of_IBPI_final_su AS
# MAGIC SELECT
# MAGIC   SUM(f.IBPI_Final_SS_SU) AS IBPI_Total_SU,
# MAGIC   1 AS ID_FOR_SUM
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final f;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.sum_of_IBPI_final_USD AS
# MAGIC SELECT
# MAGIC   SUM(f.FINALIOSAFETYSTOCKVAL) AS IBPI_Total_USD,
# MAGIC   1 AS ID_FOR_USD
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final f;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.prep_for_summary AS
# MAGIC SELECT
# MAGIC   c.Comment,
# MAGIC   COUNT(*) AS Number_of_Matlocs,
# MAGIC   SUM(c.IBPI_Final_SS_SU) AS IBPI_Final_SU_sum,
# MAGIC   SUM(c.FINALIOSAFETYSTOCKVAL) AS IBPI_Final_USD_sum,
# MAGIC   -- SUM(c.Wahupa_SS_SU) AS Wahupa_Final_SU_sum,
# MAGIC   1 AS ID_PREP
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.final_comparison c
# MAGIC GROUP BY
# MAGIC   c.Comment;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ps.Comment,
# MAGIC   ps.Number_of_Matlocs,
# MAGIC   ps.IBPI_Final_SU_sum,
# MAGIC   t.Number_of_total_examples,
# MAGIC   f.IBPI_Total_SU,
# MAGIC   u.IBPI_Total_USD,
# MAGIC   ps.Number_of_Matlocs / t.Number_of_total_examples * 100 AS Percentage_of_total_examples,
# MAGIC   ps.IBPI_Final_SU_sum / f.IBPI_Total_SU * 100 AS Percentage_of_total_IBPI_volume,
# MAGIC   ps.IBPI_Final_USD_sum / u.IBPI_Total_USD * 100 AS Percentage_of_total_IBPI_value
# MAGIC FROM
# MAGIC   userdb_jaworska_ka.prep_for_summary ps
# MAGIC   LEFT JOIN userdb_jaworska_ka.number_of_total_examples t ON t.ID_FOR_TOTAL = ps.ID_PREP
# MAGIC   LEFT JOIN userdb_jaworska_ka.sum_of_IBPI_final_su f ON f.ID_FOR_SUM = ps.ID_PREP
# MAGIC   LEFT JOIN userdb_jaworska_ka.sum_of_ibpi_final_usd u ON u.ID_FOR_USD = ps.ID_PREP
# MAGIC ORDER BY
# MAGIC   Percentage_of_total_IBPI_volume DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # Comparison for two scenarios from Wahupa Kinaxis

# COMMAND ----------

# MAGIC %md
# MAGIC Upload the data

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd

# COMMAND ----------

# Here we can change the scenarios to comparison
# Path to optimized scenario from Wahupa - iteration 1
# kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_service.xlsx")
# Path to calibration scenario from Wahupa - iteration 2
# kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_onhand.xlsx")
# Path to scenario with direct demand locked and historical Forecast Error
# kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_onhand_direct_locked_Forecast_Error.xlsx")
# Path to scenario with direct demand locked and Demand Uncertainty
kinaxis_df = pd.read_excel("/dbfs/FileStore/userdb_jaworska_ka/Updated_Detail_Results_MEIO_current_onhand_direct_locked_Demand_Uncertainty.xlsx")

# COMMAND ----------

## Convert into Spark DataFrame
spark_df = spark.createDataFrame(kinaxis_df)

spark_df = spark_df.select([col(c).alias(
        c.replace( '(', '')
        .replace( ')', '')
        .replace( ',', '')
        .replace( ';', '')
        .replace( '{', '')
        .replace( '}', '')
        .replace( '\n', '')
        .replace( '\t', '')
        .replace( ' ', '_')
        .replace( '$','USD_')
    ) for c in spark_df.columns])

## Write Frame out as Table
spark_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("userdb_jaworska_ka.kinaxis_results_second_version")

# COMMAND ----------

# MAGIC %md
# MAGIC Join the data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW userdb_jaworska_ka.two_scenario_comparison AS
# MAGIC SELECT 
# MAGIC one.PartName as product_id
# MAGIC , one.PartSite as location_id
# MAGIC , one.Daily_Demandweek_CoV as first_weekly_cov
# MAGIC , one.Daily_DemandLT_CoV as first_lt_cov
# MAGIC , one.TargetService_Level as first_target_service_level
# MAGIC , one.Avg_On_HandUnits as first_safety_stock_units
# MAGIC , two.Daily_Demandweek_CoV as second_weekly_cov
# MAGIC , two.Daily_DemandLT_CoV as second_lt_cov
# MAGIC , two.TargetService_Level as second_target_service_level
# MAGIC , two.Avg_On_HandUnits as second_safety_stock_units
# MAGIC , one.Daily_DemandDirect as first_direct_demand
# MAGIC , two.Daily_DemandDirect as second_direct_demand
# MAGIC FROM userdb_jaworska_ka.kinaxis_results one
# MAGIC LEFT JOIN userdb_jaworska_ka.kinaxis_results_second_version two
# MAGIC ON one.PartName = two.PartName AND one.PartSite = two.PartSite;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM userdb_jaworska_ka.two_scenario_comparison LIMIT 10;

# COMMAND ----------


