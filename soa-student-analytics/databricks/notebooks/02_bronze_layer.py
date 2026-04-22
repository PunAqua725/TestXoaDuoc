# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Bronze Layer (Raw Delta Table)
# MAGIC **Dự án:** Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên
# MAGIC
# MAGIC Bronze layer lưu **dữ liệu thô** từ nguồn vào Delta Lake.
# MAGIC Không biến đổi, chỉ thêm metadata (ingestion timestamp).

# COMMAND ----------

# MAGIC %md ## 1. Đọc lại từ DBFS và thêm metadata

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/workspace/default/sos_data/student_score_dataset.csv")

# Thêm cột metadata
df_bronze = df_raw \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source", lit("student_score_dataset.csv"))

print(f"✅ Bronze records: {df_bronze.count()}")
display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md ## 2. Lưu vào Delta Lake – Bronze

# COMMAND ----------

# Đường dẫn mới nằm trong Volume của bạn
bronze_path = "/Volumes/workspace/default/sos_data/delta/bronze_students"

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(bronze_path)

print(f"✅ Bronze layer saved to Volume: {bronze_path}")

# COMMAND ----------

# MAGIC %md ## 3. Xác minh Delta table

# COMMAND ----------

# Trỏ đúng vào đường dẫn trong Volume
bronze_path = "/Volumes/workspace/default/sos_data/delta/bronze_students"

bronze_check = spark.read.format("delta").load(bronze_path)

print(f"📦 Bronze row count: {bronze_check.count()}")
print(f"📋 Columns: {bronze_check.columns}")
display(bronze_check.limit(10))

# COMMAND ----------

# MAGIC %md ## 4. Xem Delta history

# COMMAND ----------

from delta.tables import DeltaTable

# Trỏ vào đúng folder trong Volume
bronze_volume_path = "/Volumes/workspace/default/sos_data/delta/bronze_students"

dt_bronze = DeltaTable.forPath(spark, bronze_volume_path)
display(dt_bronze.history())

# COMMAND ----------

print("✅ Bronze layer hoàn thành!")
print("➡️  Tiếp theo: chạy notebook 03_silver_layer.py")