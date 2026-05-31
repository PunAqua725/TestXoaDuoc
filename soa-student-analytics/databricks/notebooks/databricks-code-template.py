# Databricks notebook source
import pandas as pd
import random

# 1. Tự động sinh Tên tiếng Việt ngẫu nhiên để không lo bị ít người
ho_mau = ["Nguyễn", "Trần", "Lê", "Phạm", "Vũ", "Hoàng", "Phan", "Đặng", "Bùi", "Đỗ"]
dem_mau = ["Văn", "Thị", "Ngọc", "Minh", "Hoàng", "Thu", "Đức", "Hải", "Tuấn", "Thảo"]
ten_mau = ["An", "Bình", "Chinh", "Đức", "Phương", "Vy", "Hùng", "Trang", "Linh", "Nam", "Anh", "Khoa"]

def sinh_ten_ngau_nhien():
    return f"{random.choice(ho_mau)} {random.choice(dem_mau)} {random.choice(ten_mau)}"

# 2. Định nghĩa danh sách môn học mẫu đúng logic của bạn
danh_sach_mon = [
    {"ma_mon": "INT1001", "ten_mon": "Lập trình Python", "so_tiet_lt": 30, "so_tiet_th": 30},
    {"ma_mon": "INT1002", "ten_mon": "Cơ sở dữ liệu", "so_tiet_lt": 45, "so_tiet_th": 0},
    {"ma_mon": "INT1003", "ten_mon": "Phát triển ứng dụng", "so_tiet_lt": 30, "so_tiet_th": 45},
    {"ma_mon": "GDTC101", "ten_mon": "Giáo dục thể chất 1*", "so_tiet_lt": 0, "so_tiet_th": 30}, 
    {"ma_mon": "GDQP102", "ten_mon": "Giáo dục quốc phòng*", "so_tiet_lt": 45, "so_tiet_th": 0}
]

# 3. ĐIỀU CHỈNH SỐ LƯỢNG SINH VIÊN Ở ĐÂY
# Bạn muốn bao nhiêu sinh viên thì sửa con số này nhé (Ví dụ: 500 hoặc 1000)
so_luong_sinh_vien = 1000 

data = []
for i in range(1, so_luong_sinh_vien + 1):
    student_id = f"SV{1000 + i}"       # Sinh ra mã SV1001, SV1002...
    student_name = sinh_ten_ngau_nhien() # Mỗi mã SV sẽ có một tên ngẫu nhiên riêng biệt
    
    # Mỗi sinh viên học ngẫu nhiên từ 2 đến 4 môn
    cac_mon_hoc = random.sample(danh_sach_mon, random.randint(2, 4))
    for mon in cac_mon_hoc:
        data.append([student_id, student_name, mon["ma_mon"], mon["ten_mon"], mon["so_tiet_lt"], mon["so_tiet_th"]])

# 4. Tạo DataFrame và lưu thành bảng Bronze Delta
df = pd.DataFrame(data, columns=["student_id", "student_name", "ma_mon", "ten_mon", "so_tiet_lt", "so_tiet_th"])
spark_df = spark.createDataFrame(df)

# Tạo database nếu chưa có
spark.sql("CREATE DATABASE IF NOT EXISTS smartgpa_db")

# Ghi dữ liệu vào bảng tầng Bronze
spark_df.write.format("delta").mode("overwrite").saveAsTable("smartgpa_db.bronze_diem_sinh_vien")

print(f"🎉 Đã tạo thành công bảng gốc với {so_luong_sinh_vien} sinh viên thật (Tổng cộng {len(df)} dòng điểm)!")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Thêm chữ OR REPLACE vào đây
# MAGIC CREATE OR REPLACE TABLE smartgpa_db.danh_muc_mon_hoc AS
# MAGIC SELECT 
# MAGIC     ma_mon, 
# MAGIC     ten_mon,
# MAGIC     so_tiet_lt,
# MAGIC     so_tiet_th,
# MAGIC     (so_tiet_lt / 15) AS so_chi_lt,
# MAGIC     (so_tiet_th / 30) AS so_chi_th,
# MAGIC     ((so_tiet_lt / 15) + (so_tiet_th / 30)) AS tong_so_chi,
# MAGIC     CASE 
# MAGIC         WHEN so_tiet_lt > 0 AND so_tiet_th = 0 THEN 'ly_thuyet'
# MAGIC         WHEN so_tiet_lt = 0 AND so_tiet_th > 0 THEN 'thuc_hanh'
# MAGIC         ELSE 'tich_hop'
# MAGIC     END AS loai_hoc_phan,
# MAGIC     CASE 
# MAGIC         WHEN ten_mon LIKE '%*%' THEN true 
# MAGIC         ELSE false 
# MAGIC     END AS loai_tru_gpa
# MAGIC FROM smartgpa_db.bronze_diem_sinh_vien
# MAGIC GROUP BY ma_mon, ten_mon, so_tiet_lt, so_tiet_th;

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Đọc dữ liệu tầng Bronze và bảng Danh mục môn học
df_bronze = spark.table("smartgpa_db.bronze_diem_sinh_vien")
df_danhmuc = spark.table("smartgpa_db.danh_muc_mon_hoc").select("ma_mon", "loai_hoc_phan")

# 2. Tính số tín chỉ, làm sạch
df_silver_pre = df_bronze.withColumn("so_chi_lt", F.col("so_tiet_lt") / 15) \
                         .withColumn("so_chi_th", F.col("so_tiet_th") / 30) \
                         .withColumn("tong_so_chi", F.col("so_chi_lt") + F.col("so_chi_th"))

# 3. Tính cột điểm trung bình thực hành ngẫu nhiên (từ 2.0 -> 10.0) để test điểm liệt
df_silver_pre = df_silver_pre.withColumn("diem_trung_binh_thuc_hanh", F.round(F.rand() * 8 + 2, 1))

# 4. JOIN để lấy cột loai_hoc_phan sang Silver
df_silver_joined = df_silver_pre.join(df_danhmuc, on="ma_mon", how="left")

# 5. Xử lý điểm tích luỹ hiện tại ngẫu nhiên (từ 3.0 -> 10.0)
df_silver = df_silver_joined.withColumn(
    "diem_tich_luy_hien_tai", F.round(F.rand() * 7 + 3, 1)
).withColumn(
    "status_canh_bao",
    F.when(F.col("diem_tich_luy_hien_tai") < 4.0, "Nguy co rot mon").otherwise("An toan")
)

# 6. Ghi dữ liệu xuống bảng Silver - Đã chuẩn hóa thành sinh_vien (chữ h)
df_silver.write.format("delta").mode("overwrite").saveAsTable("smartgpa_db.silver_diem_sinh_vien")
print("--- Đã nạp dữ liệu tầng Silver CHUẨN thành công! ---")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE smartgpa_db.gold_diem_sinh_vien AS
# MAGIC SELECT 
# MAGIC     student_id,
# MAGIC     student_name,
# MAGIC     ma_mon,
# MAGIC     ten_mon,
# MAGIC     loai_hoc_phan,
# MAGIC     tong_so_chi,
# MAGIC     diem_tich_luy_hien_tai,
# MAGIC     status_canh_bao,
# MAGIC     diem_trung_binh_thuc_hanh,
# MAGIC     CASE 
# MAGIC         -- Điểm liệt thực hành ở môn tích hợp
# MAGIC         WHEN loai_hoc_phan = 'tich_hop' AND diem_trung_binh_thuc_hanh < 3.0 THEN 'F'
# MAGIC         -- Điểm liệt thực hành ở môn thực hành
# MAGIC         WHEN loai_hoc_phan = 'thuc_hanh' AND diem_tich_luy_hien_tai < 3.0 THEN 'F'
# MAGIC         -- Nếu không liệt thì quy đổi thông thường
# MAGIC         WHEN diem_tich_luy_hien_tai >= 9.0 THEN 'A+'
# MAGIC         WHEN diem_tich_luy_hien_tai >= 8.5 THEN 'A'
# MAGIC         WHEN diem_tich_luy_hien_tai >= 8.0 THEN 'B+'
# MAGIC         WHEN diem_tich_luy_hien_tai >= 7.0 THEN 'B'
# MAGIC         WHEN diem_tich_luy_hien_tai >= 6.0 THEN 'C+'
# MAGIC         WHEN diem_tich_luy_hien_tai >= 5.5 THEN 'C'
# MAGIC         WHEN diem_tich_luy_hien_tai >= 5.0 THEN 'D+'
# MAGIC         WHEN diem_tich_luy_hien_tai >= 4.0 THEN 'D'
# MAGIC         ELSE 'F'
# MAGIC     END AS diem_chu_hien_tai,
# MAGIC     CASE 
# MAGIC         WHEN (loai_hoc_phan IN ('thuc_hanh', 'tich_hop') AND diem_trung_binh_thuc_hanh < 3.0) 
# MAGIC         THEN 'CANH BAO: LIET THUC HANH (ROT MON)'
# MAGIC         ELSE status_canh_bao
# MAGIC     END AS status_canh_bao_final
# MAGIC FROM smartgpa_db.silver_diem_sinh_vien; -- Khớp hoàn toàn với tên bảng ở Bước 2

# COMMAND ----------

import os
from databricks import sql # Import toàn bộ module

def lay_diem_sinh_vien_tu_cloud(student_id: str, ma_mon: str):
    connection = sql.connect(  
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )
    
    cursor = connection.cursor()
    query = f"""
        SELECT diem_tich_luy_hien_tai, loai_hoc_phan, tong_so_chi, 
               diem_trung_binh_thuc_hanh, diem_chu_hien_tai, status_canh_bao_final
        FROM smartgpa_db.gold_diem_sinh_vien 
        WHERE student_id = '{student_id}' AND ma_mon = '{ma_mon}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result

# COMMAND ----------

# Chuyển DataFrame sang Pandas và lưu thành file CSV ngay tại thư mục hiện tại của Notebook
df = spark.table("smartgpa_db.gold_diem_sinh_vien").toPandas()
df.to_csv("diem_sinh_vien.csv", index=False)