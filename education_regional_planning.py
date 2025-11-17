# ======================================================================
# USE CASE 1: Regional Education Planning Analytics
# Dataset: Orta ixtisas təhsili müəssisələrinin fəaliyyəti və tələbə hərəkətliliyi
# Author: Vurghun Hajiyev
# Version: 1.0
# ======================================================================

# -----------------------------------------------------------
# 1. Libraries
# -----------------------------------------------------------
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Education_Regional_Planning").getOrCreate()

# -----------------------------------------------------------
# 2. Raw Data – OpenData Portal CSV Load
# -----------------------------------------------------------

csv_url = "https://admin.opendata.az/dataset/656f17a6-c576-4167-bc93-ffe0d9d1801e/resource/95b57e10-3bd1-4eb3-baa6-0f93b08ee67f/download/orta-ixtisas-thsili-mussislri-tdris-ilinin-vvlin.csv"

df_raw = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("encoding", "UTF-8") \
    .load(csv_url)

print("Raw Data Schema:")
df_raw.printSchema()

print("Sample Rows:")
df_raw.show(10)

# -----------------------------------------------------------
# 3. Data Cleaning
# -----------------------------------------------------------

# Standard column name cleaning
clean_cols = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df_raw.columns]
df_clean = df_raw.toDF(*clean_cols)

# Replace empty strings with null
df_clean = df_clean.replace("", None)

# Convert numeric columns (auto-detect)
numeric_columns = ["telebe_sayi", "mezun_sayi", "qebul_sayi", "mobility_gelen", "mobility_geden", "muellim_sayi"]

for col_name in numeric_columns:
    if col_name in df_clean.columns:
        df_clean = df_clean.withColumn(col_name, df_clean[col_name].cast(IntegerType()))

# -----------------------------------------------------------
# 4. Add Derived Metrics
# -----------------------------------------------------------

df_enriched = df_clean \
    .withColumn("student_teacher_ratio", col("telebe_sayi") / col("muellim_sayi")) \
    .withColumn("net_mobility", col("mobility_gelen") - col("mobility_geden"))

# -----------------------------------------------------------
# 5. Region Normalization Table (Optional)
# -----------------------------------------------------------

region_map = {
    "Baki": "Bakı",
    "Gence": "Gəncə",
    "Sumqayit": "Sumqayıt"
    # Buraya əlavə mapping-lər əlavə ediləcək
}

mapping_expr = create_map([lit(x) for x in sum(region_map.items(), ())])

df_enriched = df_enriched.withColumn("region_clean", mapping_expr.getItem(col("region")))

# -----------------------------------------------------------
# 6. Save to Bronze → Si
