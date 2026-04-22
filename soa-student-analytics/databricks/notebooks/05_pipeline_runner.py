# Databricks notebook source
# MAGIC %md
# MAGIC # 05 – Pipeline Runner
# MAGIC **Dự án:** Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên
# MAGIC
# MAGIC Notebook điều phối – chạy toàn bộ ETL pipeline theo thứ tự.
# MAGIC Dùng notebook này để tạo **Databricks Job** (Workflows).
# MAGIC
# MAGIC | Bước | Notebook | Mô tả |
# MAGIC |---|---|---|
# MAGIC | 1 | 01_data_ingestion | Đọc CSV từ DBFS, validate |
# MAGIC | 2 | 02_bronze_layer   | Lưu raw → Delta Bronze |
# MAGIC | 3 | 03_silver_layer   | Làm sạch → Delta Silver |
# MAGIC | 4 | 04_gold_analytics | Tính GPA, phân tích → Delta Gold |

# COMMAND ----------

import time
from datetime import datetime

def run_step(step_num, name, path):
    print(f"\n{'='*50}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] STEP {step_num}: {name}")
    print(f"{'='*50}")
    start = time.time()
    dbutils.notebook.run(path, timeout_seconds=600)
    elapsed = round(time.time() - start, 1)
    print(f"✅ Step {step_num} hoàn thành trong {elapsed}s")
    return elapsed

# COMMAND ----------

# THAY ĐỔI: Dán đường dẫn bạn vừa copy vào đây (bỏ phần tên notebook cuối cùng đi)
base_path = "/Users/vungocthuphuong725@gmail.com/soa-student-analytics"

print("🚀 BẮT ĐẦU ETL PIPELINE")
print(f"⏰ Thời gian bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 50)

pipeline_start = time.time()

# Cập nhật các đường dẫn sử dụng f-string
t1 = run_step(1, "Data Ingestion", f"{base_path}/01_ingestion/01_data_ingestion")
t2 = run_step(2, "Bronze Layer",    f"{base_path}/02_bronze/02_bronze_layer")
t3 = run_step(3, "Silver Layer",    f"{base_path}/03_silver/03_silver_layer")
t4 = run_step(4, "Gold Analytics",  f"{base_path}/04_gold/04_gold_analytics")

total = round(time.time() - pipeline_start, 1)

print("\n" + "=" * 50)
print("🎉 PIPELINE HOÀN THÀNH!")
print(f"⏱️ Tổng thời gian: {total}s")

# COMMAND ----------

# MAGIC %md ## Kết quả cuối – kiểm tra nhanh Gold tables

# COMMAND ----------

print("📊 Kết quả Gold layer sau pipeline:")
for path, name in [
    ("/delta/gold/student_gpa",        "student_gpa"),
    ("/delta/gold/score_distribution", "score_distribution"),
    ("/delta/gold/attendance_impact",  "attendance_impact"),
]:
    df = spark.read.format("delta").load(path)
    print(f"  ✅ {name}: {df.count()} rows")

# COMMAND ----------

# Quick summary
from pyspark.sql.functions import count, when, col, avg, round as spark_round

df_gold = spark.read.format("delta").load("/delta/gold/student_gpa")

total    = df_gold.count()
pass_ct  = df_gold.filter(col("pass_fail") == "Pass").count()
risk_ct  = df_gold.filter(col("at_risk") == True).count()
avg_g    = df_gold.agg(spark_round(avg("grade_10"),2).alias("avg")).collect()[0]["avg"]

print(f"\n📈 TỔNG KẾT HỌC LỰC ({total} sinh viên)")
print(f"  Đạt (Pass)      : {pass_ct} ({round(pass_ct/total*100,1)}%)")
print(f"  Không đạt (Fail): {total-pass_ct} ({round((total-pass_ct)/total*100,1)}%)")
print(f"  Nguy cơ học yếu : {risk_ct} ({round(risk_ct/total*100,1)}%)")
print(f"  Điểm TB toàn lớp: {avg_g} / 10")