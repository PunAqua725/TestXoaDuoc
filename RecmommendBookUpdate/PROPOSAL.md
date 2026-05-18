# 📋 PROJECT PROPOSAL
# Hệ thống Gợi ý Sách thông minh (Version 3.0) — PageSpark

> **Môn học:** Phát triển Ứng dụng  
> **Năm học:** 2025–2026  
> **Ngày cập nhật:** 06/05/2026

---

## 1. Tổng quan dự án

### 1.1 Tên dự án
**PageSpark — AI-Powered Book Recommendation System with User Management**

### 1.2 Mô tả ngắn
PageSpark là một nền tảng gợi ý sách toàn diện. Không chỉ dừng lại ở việc tìm kiếm bằng AI, hệ thống hiện đã tích hợp quản lý người dùng, lưu trữ lịch sử tìm kiếm và bảng điều khiển dành cho Admin để theo dõi hoạt động toàn hệ thống.

### 1.3 Mục tiêu nâng cao (V3.1)
- **Quản lý người dùng**: Đăng ký, Đăng nhập bảo mật (Mã hóa SHA-256), phân quyền User/Admin, Hỗ trợ Reset & Đổi mật khẩu.
- **Lưu trữ dữ liệu**: Sử dụng SQLite và SQLAlchemy để lưu thông tin người dùng và lịch sử tìm kiếm.
- **Báo cáo Admin**: Dashboard thống kê trực quan với Chart.js, quản lý trạng thái tài khoản, và tính năng Xuất dữ liệu (Export CSV).
- **Tối ưu hiệu năng**: Tích hợp cơ chế Caching (Pickle) cho ma trận TF-IDF để giảm 90% thời gian khởi động server.
- **Trải nghiệm người dùng**: Giao diện Premium Glassmorphism, Responsive hoàn toàn, hỗ trợ tìm kiếm linh hoạt bằng bộ lọc.

---

## 2. Kiến trúc hệ thống mở rộng

### 2.1 Sơ đồ luồng dữ liệu (Data Flow)
1. **Auth Flow**: User -> Register/Login -> JWT/Session (LocalStorage) -> Access App.
2. **Search Flow**: User Input -> AI Model (TF-IDF) -> Database (Log Search) -> Results.
3. **Admin Flow**: Admin -> Stats API -> Database (Users/History) -> Management UI.

### 2.2 Công nghệ bổ sung
- **ORM**: SQLAlchemy (xử lý SQLite).
- **Auth & Security**: Đăng nhập phân quyền với mã hóa mật khẩu SHA-256 (Built-in hashlib).
- **Storage**: `pagespark.db` (SQLite file-based) và `.pkl` (Pickle Cache cho ML Model).
- **Frontend Analytics**: Chart.js cho biểu đồ thống kê.

---

## 3. Thuật toán & Machine Learning

Hệ thống tiếp tục duy trì sức mạnh của **TF-IDF** và **Cosine Similarity** để đảm bảo kết quả gợi ý đạt độ chính xác cao nhất dựa trên ngữ nghĩa của mô tả, thay vì chỉ so khớp từ khóa đơn thuần.

---

## 4. Các tính năng cốt lõi (V3.1)

| Nhóm tính năng | Chi tiết |
|---|---|
| **Người dùng & Bảo mật** | Đăng ký/Đăng nhập, Mã hóa SHA-256, Reset/Đổi mật khẩu, Lưu lịch sử cá nhân. |
| **Gợi ý AI & Hiệu năng** | Tìm bằng NLP, Lọc linh hoạt (không cần mô tả), Caching mô hình TF-IDF. |
| **Thư viện cá nhân (V3.2)** | Đánh dấu yêu thích sách (Heart), Thống kê cá nhân trực quan về hoạt động. |
| **Quản trị (Admin)** | Biểu đồ thống kê (Chart.js), Xuất báo cáo (CSV Export), Quản lý User/History. |
| **Giao diện** | Hiệu ứng Blur Glass, Animation mượt mà, Dark mode sang trọng. |

---

## 5. Kế hoạch phát triển tiếp theo

- [x] **Bảo mật nâng cao**: Hash mật khẩu (SHA-256).
- [x] **Thư viện cá nhân & Yêu thích (V3.2)**: Lưu sách tâm đắc và thống kê cá nhân.
- [ ] **Đa ngôn ngữ**: Hỗ trợ thêm tiếng Việt cho phần AI (sử dụng các mô hình Transformer).
- [ ] **Recommendation Engine**: Kết hợp Collaborative Filtering (gợi ý dựa trên người dùng tương tự).

---

> **PageSpark** — *Kiến thức là sức mạnh, và chúng tôi giúp bạn tìm thấy nó.* 📖✨
