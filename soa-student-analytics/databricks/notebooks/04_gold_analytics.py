# Databricks notebook source
# MAGIC %md
# MAGIC # 04 – Gold Layer (Analytics & GPA)
# MAGIC **Dự án:** Hệ thống SOA Phân tích và Quản lý Kết quả Học tập Sinh viên
# MAGIC
# MAGIC Gold layer tạo các bảng phân tích sẵn sàng cho API:
# MAGIC - **student_gpa**: điểm tổng kết, xếp loại, cờ cảnh báo học yếu
# MAGIC - **score_distribution**: phân phối điểm theo xếp loại
# MAGIC - **attendance_impact**: tương quan điểm danh vs điểm số

# COMMAND ----------

# MAGIC %md ## 1. Đọc Silver

# COMMAND ----------

from pyspark.sql.functions import (
    col, round as spark_round, when, avg, count, 
    min, max, stddev, corr, sum as spark_sum,
    current_timestamp
)

# SỬA DÒNG NÀY: Trỏ vào đúng folder Silver trong Volume của bạn
silver_volume_path = "/Volumes/workspace/default/sos_data/delta/silver_students_clean"

# Đọc dữ liệu từ Volume
df_silver = spark.read.format("delta").load(silver_volume_path)

print(f"📦 Silver input: {df_silver.count()} rows")

# COMMAND ----------

# MAGIC %md ## 2. Bảng Gold 1 – student_gpa (xếp loại từng sinh viên)

# COMMAND ----------

# 1. Thực hiện tính toán các cột (Giữ nguyên logic của bạn vì nó rất chuẩn rồi)
df_gpa = df_silver.withColumn(
    "letter_grade",
    when(col("grade_10") >= 8.5, "A - Xuất sắc")
   .when(col("grade_10") >= 7.0, "B - Giỏi")
   .when(col("grade_10") >= 5.5, "C - Khá")
   .when(col("grade_10") >= 4.0, "D - Trung bình")
   .otherwise("F - Yếu / Không đạt")
).withColumn(
    "pass_fail",
    when(col("grade_10") >= 4.0, "Pass").otherwise("Fail")
).withColumn(
    "at_risk",
    when(
        (col("grade_10") < 5.0) |
        (col("lectures_attended") < 3) |
        (col("labs_attended") < 2),
        True
    ).otherwise(False)
).withColumn(
    "gpa_trend",
    when(col("grade_10") > col("previous_gpa"), "↑ Cải thiện")
   .when(col("grade_10") < col("previous_gpa"), "↓ Giảm sút")
   .otherwise("→ Ổn định")
).select(
    "student_id","name","age","gender",
    "quiz_total","midterm_pct","final_pct","total_score","grade_10",
    "previous_gpa","letter_grade","pass_fail","at_risk","gpa_trend",
    "lectures_attended","labs_attended"
)

# 2. SỬA ĐƯỜNG DẪN LƯU TẠI ĐÂY
# Thay vì "/delta/gold/...", ta lưu vào Volume sos_data
gold_save_path = "/Volumes/workspace/default/sos_data/delta/student_gpa_gold"

df_gpa.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(gold_save_path)

# 3. In kết quả và hiển thị
print(f"✅ student_gpa saved to Volume: {df_gpa.count()} rows")
display(df_gpa.limit(15))

# COMMAND ----------

# Tóm tắt phân loại
print("📊 Phân bổ sinh viên theo xếp loại:")
display(
    df_gpa.groupBy("letter_grade")
          .agg(count("*").alias("so_sinh_vien"))
          .orderBy("letter_grade")
)

print("\n🚨 Sinh viên có nguy cơ học yếu (at_risk = True):")
at_risk_count = df_gpa.filter(col("at_risk") == True).count()
total = df_gpa.count()
print(f"   {at_risk_count} / {total} sinh viên ({round(at_risk_count/total*100,1)}%)")

# COMMAND ----------

# MAGIC %md ## 3. Bảng Gold 2 – score_distribution (thống kê tổng hợp)

# COMMAND ----------

# 1. Tính toán thống kê (Logic của bạn rất tốt, giữ nguyên nhé)
df_dist = df_gpa.agg(
    count("*").alias("total_students"),
    spark_round(avg("grade_10"), 2).alias("avg_grade"),
    spark_round(min("grade_10"), 2).alias("min_grade"),
    spark_round(max("grade_10"), 2).alias("max_grade"),
    spark_round(stddev("grade_10"), 2).alias("stddev_grade"),
    count(when(col("pass_fail") == "Pass", 1)).alias("pass_count"),
    count(when(col("pass_fail") == "Fail", 1)).alias("fail_count"),
    count(when(col("at_risk") == True, 1)).alias("at_risk_count"),
)

# 2. SỬA ĐƯỜNG DẪN LƯU TẠI ĐÂY
# Lưu vào Volume sos_data thay vì /delta/gold/
dist_save_path = "/Volumes/workspace/default/sos_data/delta/score_distribution_gold"

df_dist.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(dist_save_path)

# 3. Thông báo và hiển thị
print(f"✅ score_distribution saved to Volume: {dist_save_path}")
display(df_dist)

# COMMAND ----------

# MAGIC %md ## 4. Bảng Gold 3 – attendance_impact (tương quan điểm danh vs điểm)

# COMMAND ----------

# 1. Tính toán thống kê (Logic của bạn rất tốt, giữ nguyên nhé)
df_dist = df_gpa.agg(
    count("*").alias("total_students"),
    spark_round(avg("grade_10"), 2).alias("avg_grade"),
    spark_round(min("grade_10"), 2).alias("min_grade"),
    spark_round(max("grade_10"), 2).alias("max_grade"),
    spark_round(stddev("grade_10"), 2).alias("stddev_grade"),
    count(when(col("pass_fail") == "Pass", 1)).alias("pass_count"),
    count(when(col("pass_fail") == "Fail", 1)).alias("fail_count"),
    count(when(col("at_risk") == True, 1)).alias("at_risk_count"),
)

# 2. SỬA ĐƯỜNG DẪN LƯU TẠI ĐÂY
# Lưu vào Volume sos_data thay vì /delta/gold/
dist_save_path = "/Volumes/workspace/default/sos_data/delta/score_distribution_gold"

df_dist.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(dist_save_path)

# 3. Thông báo và hiển thị
print(f"✅ score_distribution saved to Volume: {dist_save_path}")
display(df_dist)
display(df_att.orderBy("avg_grade", ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md ## 5. Xác minh toàn bộ Gold tables

# COMMAND ----------

# Danh sách các bảng bạn đã lưu vào Volume sos_data
# Lưu ý: Tên folder phải khớp chính xác với tên bạn đã dùng ở lệnh .save() lúc nãy
check_list = [
    ("/Volumes/workspace/default/sos_data/delta/student_gpa_gold",      "student_gpa"),
    ("/Volumes/workspace/default/sos_data/delta/score_distribution_gold", "score_distribution"),
    # Thêm các bảng khác nếu bạn đã lưu, ví dụ:
    # ("/Volumes/workspace/default/sos_data/delta/attendance_impact_gold", "attendance_impact"),
]

for path, name in check_list:
    try:
        df_check = spark.read.format("delta").load(path)
        print(f"✅ {name}: {df_check.count()} rows, {len(df_check.columns)} cols")
    except Exception as e:
        print(f"❌ Không tìm thấy {name} tại đường dẫn: {path}")

# COMMAND ----------

from delta.tables import DeltaTable

# SỬA DÒNG NÀY: Trỏ vào đúng folder Gold trong Volume của bạn
gold_volume_path = "/Volumes/workspace/default/sos_data/delta/student_gpa_gold"

# Truy cập bảng Delta bằng đường dẫn Volume
dt_gold = DeltaTable.forPath(spark, gold_volume_path)

# Hiển thị lịch sử (History)
display(dt_gold.history())

# COMMAND ----------

print("🎉 GOLD LAYER HOÀN THÀNH!")
print("=" * 50)
print("Các bảng đã tạo:")
print("  /delta/gold/student_gpa        → xếp loại từng SV")
print("  /delta/gold/score_distribution → thống kê tổng hợp")
print("  /delta/gold/attendance_impact  → tương quan điểm danh")